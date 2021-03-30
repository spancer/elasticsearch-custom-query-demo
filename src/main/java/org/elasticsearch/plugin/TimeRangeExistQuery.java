package org.elasticsearch.plugin;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/**
 * @author spancer.ray
 * @date 2021/3/26 11:35
 */
public class TimeRangeExistQuery extends Query {

  private Map<String, Float> fieldsBoosts;
  private String docField;
  private String docValue;
  private Integer minMatch;
  private Long timeInterval = 3 * 60 * 1000L; // default to 3 mins.

  public TimeRangeExistQuery(
      Map<String, Float> fieldsBoosts, String docField, String docValue, Integer minMatch, Long timeInterval) {
    this.fieldsBoosts = fieldsBoosts;
    this.docField = docField;
    this.docValue = docValue;
    this.minMatch = minMatch;
    this.timeInterval = timeInterval;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        DocIdSetIterator allDocs = DocIdSetIterator.all(context.reader().maxDoc());
        int targetDocId = NO_MORE_DOCS;
        TermQuery idQuery = new TermQuery(new Term(docField, docValue));
        TopDocs hits = searcher.search(idQuery, 1);
        if (hits.totalHits.value == 0)
          throw new IOException("Target Doc doesn't exist, make sure field ["+docField+"] indexed with type [keyword]");
        targetDocId = hits.scoreDocs[0].doc;
        int id = targetDocId;
        TwoPhaseIterator twoPhase =
            new TwoPhaseIterator(allDocs) {
              @Override
              public boolean matches() throws IOException {
                int currentId = allDocs.docID();
                //Target Doc may not exist in current reader context search, here we use context.docBase to
                //get original doc id of current doc in current reader context, so as to compare with target doc id
                //loaded from top reader context.
                if (context.docBase + currentId == id) return false;
                Document current = context.reader().document(currentId);
                Document target = searcher.getTopReaderContext().reader().document(id);
                int counter = minMatch;
                for (String field : fieldsBoosts.keySet()) {
                  String[] targetValues = target.getValues(field); // k1
                  breaker:
                  for (String val : current.getValues(field)) {
                    for (String targetVal : targetValues) {
                      long iterval = Long.parseLong(val) - Long.parseLong(targetVal);
                      if (Math.abs(iterval) <= timeInterval) {
                        counter--;
                        break breaker;
                      }
                    }
                  }
                  if (counter == 0) return true;
                }
                return false;
              }

              @Override
              public float matchCost() {
                return 1f;
              }
            };
        return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        // TODO: Change this to true when we can assume that scripts are pure functions
        // ie. the return value is always the same given the same conditions and may not
        // depend on the current timestamp, other documents, etc.
        return false;
      }
    };
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("TimeRangeExistQuery(");
    buffer.append(fieldsBoosts);
    buffer.append(minMatch);
    buffer.append(timeInterval);
    buffer.append(")");
    return buffer.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) return false;
    TimeRangeExistQuery other = (TimeRangeExistQuery) obj;
    return Objects.equals(fieldsBoosts, other.fieldsBoosts)
        && Objects.equals(minMatch, other.minMatch)
        && Objects.equals(timeInterval, other.timeInterval);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + fieldsBoosts.hashCode();
    h = 31 * h + minMatch.hashCode();
    h = 31 * h + timeInterval.hashCode();
    return h;
  }
}
