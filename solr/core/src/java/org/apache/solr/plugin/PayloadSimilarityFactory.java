package org.apache.solr.plugin;

import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.index.FieldInvertState;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;

public class PayloadSimilarityFactory extends SimilarityFactory {
  @Override
  public Similarity getSimilarity() {
    return PayloadSimilarity.INSTANCE;
  }
}

/**
 * Created by dwang on 07/19/19.
 *
 * The class is used for payload field. It ignores coord/queryNorm/tf/idf
 * parameter for scoring and the final score will be the payload.
 */
class PayloadSimilarity extends Similarity {
  final static Similarity INSTANCE = new PayloadSimilarity();

  static class DirectScorer extends SimScorer {
    float boost;
    DirectScorer(float boost) {
      this.boost = boost;
    }

    @Override
    public float score(float freq, long norm) {
      return freq * this.boost;
    }
  }

  @Override
  public SimScorer scorer(float boost,
    CollectionStatistics collectionStats, TermStatistics... termStats) {
      return new DirectScorer(boost);
  }

  @Override
  public long computeNorm(FieldInvertState state) {
    return 1;
  }
}
