package org.apache.solr.plugin;

import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.util.BytesRef;

/**
 * Created by dwang on 3/2/17.
 *
 * The class is used for payload field. It ignores coord/queryNorm/tf/idf
 * parameter for scoring and the final score will be the payload.
 */
public class PayloadSimilarity extends ClassicSimilarity {
  @Override
  public float scorePayload(int doc, int start, int end, BytesRef payload) {
    if (payload == null) {
      return 1.0F;
    }
    return PayloadHelper.decodeFloat(payload.bytes, payload.offset);
  }

  @Override
  public float coord(int overlap, int maxOverlap) {
    return 1.0f;
  }

  @Override
  public float queryNorm(float sumOfSquaredWeights) {
    return 1.0f;
  }

  @Override
  public float tf(float freq) {
    return 1.0f;
  }

  @Override
  public float idf(long docFreq, long numDocs) {
    return 1.0f;
  }
}
