package org.elasticsearch.plugin;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
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
import org.apache.lucene.search.Weight;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author spancer.ray
 * @date 2021/3/26 11:35
 */
public class TimeRangeExistQuery extends Query {
  private static final Logger LOG = LoggerFactory.getLogger(TimeRangeExistQuery.class);

  private Map<String, Float> fieldsBoosts;
  private Integer minMatch;
  private Long timeInterval = 3 * 60 * 1000L; // default to 3 mins.
  TermQuery targetDocQuery; // query based on id

  public TimeRangeExistQuery(
      Map<String, Float> fieldsBoosts, String docId, Integer minMatch, Long timeInterval) {
    this.fieldsBoosts = fieldsBoosts;
    this.minMatch = minMatch;
    this.timeInterval = timeInterval;
    targetDocQuery = new TermQuery(new Term(IdFieldMapper.NAME, docId));
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new TimeRangeExistWeight(searcher);
  }

  public class TimeRangeExistWeight extends ConstantScoreWeight {

    Weight targetDocWeight = null;
    Weight trailingDocsWeight = null;

    public TimeRangeExistWeight(IndexSearcher searcher) throws IOException {
      super(TimeRangeExistQuery.this, 1.0f);
      targetDocWeight = targetDocQuery.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    }

    public boolean isCacheable(LeafReaderContext leaf) {
      return false;
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
      DocIdSetIterator targetDoc = DocIdSetIterator.empty();
      Scorer targetDocScorer = targetDocWeight.scorer(context);
      if (targetDocScorer != null) {
        targetDoc = targetDocScorer.iterator();
      }
      int targetDocId = targetDoc.nextDoc();
      return new ConstantScoreScorer(
          this,
          score(),
          ScoreMode.COMPLETE,
          new TwoPhaseIteratorExt(
              context, approximation, targetDocId, fieldsBoosts.keySet(), minMatch, timeInterval));
    }
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
