/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;

/**
 * Rough measure of how many buckets this {@link Aggregator} will collect
 * used to pick data structures used during collection. Just "none", "one",
 * and "many".
 * <p>
 * Unlike {@link AggregationBuilder.BucketCardinality} this is influenced
 * by the number of buckets that the parent aggregation collect.
 */
public enum TotalBucketCardinality {
    /**
     * {@link Aggregator}s with this cardinality won't collect any buckets.
     * This could be because they are {@link MetricsAggregator}s which don't
     * support buckets at all. Or they could be {@link BucketsAggregator}
     * that are configured in such a way that they collect any buckets. 
     */
    NONE {
        @Override
        public TotalBucketCardinality forKnownBucketAggregator(int bucketCount) {
            return NONE;
        }
    },

    /**
     * {@link Aggregator}s with this cardinality will collect only a single
     * bucket. This will only be true for top level {@linkplain Aggregator}s
     * and for descendants of aggregation
     */
    ONE {
        @Override
        public TotalBucketCardinality forKnownBucketAggregator(int bucketCount) {
            switch (bucketCount) {
                case 0:
                    return NONE;
                case 1:
                    return ONE;
                default:
                    return MANY;
            }
        }
    },
    /**
     * {@link Aggregator}s with this cardinality will collect many buckets.
     * Most {@link BucketsAggregator}s will have this cardinality.
     */
    MANY {
        @Override
        public TotalBucketCardinality forKnownBucketAggregator(int bucketCount) {
            return MANY;
        }
    };

    /**
     * Get the rough measure of the number of buckets a fixed-bucket
     * {@link Aggregator} will collect.
     *
     * @param bucketCount the number of buckets that this {@link Aggregator}
     *   will collect per owning ordinal
     */
    public abstract TotalBucketCardinality forKnownBucketAggregator(int bucketCount);
}