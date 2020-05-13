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
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregator;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;

/**
 * Upper bound of how many {@code owningBucketOrds} that an {@link Aggregator}
 * will have to collect into. Just "none", "one", and "many".
 */
public enum CardinalityUpperBound {
    /**
     * {@link Aggregator}s with this cardinality won't collect any data at
     * all. For the most part this happens when an aggregation is inside of a
     * {@link BucketsAggregator} that is pointing to an unmapped field. 
     */
    NONE {
        @Override
        public CardinalityUpperBound multiply(int bucketCount) {
            return NONE;
        }
    },

    /**
     * {@link Aggregator}s with this cardinality will collect be collected
     * once or zero times. This will only be true for top level {@linkplain Aggregator}s
     * and for sub-aggregator's who's ancestors are all single-bucket
     * aggregations like {@link FilterAggregator} or a {@link RangeAggregator}
     * configured to collect only a single range.
     */
    ONE {
        @Override
        public CardinalityUpperBound multiply(int bucketCount) {
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
     * {@link Aggregator}s with this cardinality may be collected many times.
     * Most sub-aggregators of {@link BucketsAggregator}s will have
     * this cardinality.
     */
    MANY {
        @Override
        public CardinalityUpperBound multiply(int bucketCount) {
            if (bucketCount == 0) {
                return NONE;
            }
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
    public abstract CardinalityUpperBound multiply(int bucketCount);
}
