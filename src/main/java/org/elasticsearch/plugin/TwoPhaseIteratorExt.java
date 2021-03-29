package org.elasticsearch.plugin;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author spancer.ray
 * @date 2021/3/29 17:30
 */
public class TwoPhaseIteratorExt extends TwoPhaseIterator {
  private static final Logger LOG = LoggerFactory.getLogger(TwoPhaseIteratorExt.class);
  private int targetDoc;
  private LeafReaderContext context;
  private Set<String> fields;
  private int minMatch;
  private long timeInterval;

  public TwoPhaseIteratorExt(LeafReaderContext context, DocIdSetIterator approximation, int targetDoc,
      Set<String> fields, int minMatch, long timeInterval) {
    super(approximation);
    this.targetDoc = targetDoc;
    this.context = context;
    this.fields = fields;
    this.minMatch = minMatch;
    this.timeInterval = timeInterval;
  }

  @Override
  public boolean matches() throws IOException {
    int currentId = approximation.nextDoc();
    if(currentId == targetDoc)
        return false;
    else
    {
      Document current = context.reader().document(currentId);
      Document target = context.reader().document(targetDoc);
      for (String field : fields) {
        int counter = this.minMatch;
        String[] targetValues = target.getValues(field);
        for (String val : current.getValues(field)) {
          LOG.info("docID: {}, field:{}, value: {}", currentId, field, val);
          for (String valTarget : targetValues) {
            if (Math.abs(Long.parseLong(val) - Long.parseLong(valTarget)) <= timeInterval) {
              counter--;
              break;
            }
          }
        }
        if (counter == 0) return true;
      }
      return false;
    }

  }

  @Override
  public float matchCost() {
    return 0;
  }
}
