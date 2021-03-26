package org.elasticsearch.plugin;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author spancer.ray
 * @date 2021/3/26 11:35
 */
public class TimeRangeExistQuery extends Query {
  private static final Logger LOG = LoggerFactory.getLogger(TimeRangeExistQuery.class);

  private Map<String, Float> fieldsBoosts;
  private String docId;
  private Integer minMatch;
  private Long timeInterval = 3 * 60 * 1000L; // default to 3 mins.

  public TimeRangeExistQuery(
      Map<String, Float> fieldsBoosts, String docId, Integer minMatch, Long timeInterval) {
    this.fieldsBoosts = fieldsBoosts;
    this.docId = docId;
    this.minMatch = minMatch;
    this.timeInterval = timeInterval;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return new ConstantScoreWeight(this, boost) {

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
        int approximationDoc = approximation.docID();
        int approximationNextDoc =  approximation.nextDoc();
        LOG.info(
            "approximationDoc ids: [{},{}]",
            approximationDoc, approximationNextDoc);
        /**
         * Object idObject = docId; if (idObject instanceof BytesRef) { idObject = ((BytesRef)
         * idObject).utf8ToString(); } BytesRef bytesRefs= Uid.encodeId(idObject.toString()); //doc
         * id TermsEnum termsEnum = context.reader().terms(IdFieldMapper.NAME).iterator(); TermState
         * state = termsEnum.termState(); termsEnum.seekExact(bytesRefs, state); long doc =
         * termsEnum.ord();
         */
        TwoPhaseIterator twoPhase =
            new TwoPhaseIterator(approximation) {

              @Override
              public boolean matches() throws IOException {
                int currentId = approximation.docID();
                Document current = context.reader().document(currentId);
                LOG.info(
                    "twoPhase current DOC and param-fields: [{},{}]",
                    currentId,
                    fieldsBoosts.keySet());
                for (String field : fieldsBoosts.keySet()) {
                  LOG.info(
                      "twoPhase fields: [{},{}]",
                      current.getFields().stream()
                          .map(item -> item.name())
                          .collect(Collectors.joining(",")),
                      current.getField(field));
                  if (null != current.getField(field)) {
                    String[] value = current.getValues(field);
                    LOG.info("twoPhase value: [{}]", value);
                    if (value.length > 0) // here we got the time serials.
                    {
                      if (value[0].contains(",")) {
                        String[] timeValuesInString = value[0].split(",");
                        LOG.info("twoPhase time values: [{}]", value[0]);
                      }
                    }
                  }
                }
                return true;
              }

              @Override
              public float matchCost() {
                return 1000f;
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
    buffer.append(docId);
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
        && Objects.equals(docId, other.docId)
        && Objects.equals(minMatch, other.minMatch)
        && Objects.equals(timeInterval, other.timeInterval);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + fieldsBoosts.hashCode();
    h = 31 * h + docId.hashCode();
    h = 31 * h + minMatch.hashCode();
    h = 31 * h + timeInterval.hashCode();
    return h;
  }
}