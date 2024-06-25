/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.HasAggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;

/**
 * A single bucket aggregation
 */
public interface SingleBucketAggregation extends Aggregation, HasAggregations, BucketCounter {

    /**
     * @return  The number of documents in this bucket
     */
    long getDocCount();

    /**
     * @return  The sub-aggregations of this bucket
     */
    @Override
    InternalAggregations getAggregations();

    @Override
    default int countBuckets() {
        int count = 1;
        InternalAggregations subAggregations = getAggregations();
        if (subAggregations != null) {
            for (Aggregation aggregation : subAggregations) {
                if (aggregation instanceof MultiBucketsAggregation multiBucketsAggregation) {
                    for (MultiBucketsAggregation.Bucket subBucket : multiBucketsAggregation.getBuckets()) {
                        count += subBucket.countBuckets();
                    }
                } else if (aggregation instanceof SingleBucketAggregation singleBucketAggregation) {
                    count += singleBucketAggregation.countBuckets();
                }
            }
        }
        return count;
    }
}
