package org.elasticsearch.search.aggregations.bucket.tophits;

import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;

/**
 */
public interface TopHits extends Aggregation {

    SearchHits getHits();

}
