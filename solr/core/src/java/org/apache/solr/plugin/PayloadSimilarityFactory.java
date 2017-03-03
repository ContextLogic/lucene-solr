package org.apache.solr.plugin;

import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.SimilarityFactory;

/**
 * Created by dwang on 3/2/17.
 *
 * This class is used as a factory class plugin to Solr config.
 */
public class PayloadSimilarityFactory extends SimilarityFactory {

  @Override
  public void init(SolrParams params) {
    super.init(params);
  }

  @Override
  public Similarity getSimilarity() {
    return new PayloadSimilarity();
  }
}
