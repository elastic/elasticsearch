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


package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

public abstract class TermsAggregator extends BucketsAggregator {

    public static class BucketCountThresholds {
        public long minDocCount;
        public long shardMinDocCount;
        public int requiredSize;
        public int shardSize;
        
        public BucketCountThresholds(long minDocCount, long shardMinDocCount, int requiredSize, int shardSize) {
            this.minDocCount = minDocCount;
            this.shardMinDocCount = shardMinDocCount;
            this.requiredSize = requiredSize;
            this.shardSize = shardSize;
        }
        public BucketCountThresholds() {
            this.minDocCount = 1;
            this.shardMinDocCount = 0;
            this.requiredSize = 10;
            this.shardSize = -1;
        }

        public BucketCountThresholds(BucketCountThresholds bucketCountThresholds) {
            this.minDocCount = bucketCountThresholds.minDocCount;
            this.shardMinDocCount = bucketCountThresholds.shardMinDocCount;
            this.requiredSize = bucketCountThresholds.requiredSize;
            this.shardSize = bucketCountThresholds.shardSize;
        }
    }

    protected final BucketCountThresholds bucketCountThresholds;

    public TermsAggregator(String name, BucketAggregationMode bucketAggregationMode, AggregatorFactories factories, long estimatedBucketsCount, AggregationContext context, Aggregator parent, BucketCountThresholds bucketCountThresholds) {
        super(name, bucketAggregationMode, factories, estimatedBucketsCount, context, parent);
        this.bucketCountThresholds = bucketCountThresholds;
    }
}
