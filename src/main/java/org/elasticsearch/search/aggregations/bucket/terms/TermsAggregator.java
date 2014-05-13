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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

public abstract class TermsAggregator extends BucketsAggregator {

    public static class BucketCountThresholds {
        private Explicit<Long> minDocCount;
        private Explicit<Long> shardMinDocCount;
        private Explicit<Integer> requiredSize;
        private Explicit<Integer> shardSize;
        
        public BucketCountThresholds(long minDocCount, long shardMinDocCount, int requiredSize, int shardSize) {
            this.minDocCount = new Explicit<>(minDocCount, false);
            this.shardMinDocCount =  new Explicit<>(shardMinDocCount, false);
            this.requiredSize = new Explicit<>(requiredSize, false);
            this.shardSize = new Explicit<>(shardSize, false);
        }
        public BucketCountThresholds() {
            this(-1, -1, -1, -1);
        }

        public BucketCountThresholds(BucketCountThresholds bucketCountThresholds) {
            this(bucketCountThresholds.minDocCount.value(), bucketCountThresholds.shardMinDocCount.value(), bucketCountThresholds.requiredSize.value(), bucketCountThresholds.shardSize.value());
        }

        public void ensureValidity() {
            // shard_size cannot be smaller than size as we need to at least fetch <size> entries from every shards in order to return <size>
            if (shardSize.value() < requiredSize.value()) {
                shardSize = requiredSize;
            }

            // shard_min_doc_count should not be larger than min_doc_count because this can cause buckets to be removed that would match the min_doc_count criteria
            if (shardMinDocCount.value() > minDocCount.value()) {
                shardMinDocCount = minDocCount;
            }

            if (requiredSize.value() < 0 || minDocCount.value() < 0) {
                throw new ElasticsearchException("parameters [requiredSize] and [minDocCount] must be >=0 in terms aggregation.");
            }
        }

        public Explicit<Long> getShardMinDocCount() {
            return shardMinDocCount;
        }

        public void setShardMinDocCount(long shardMinDocCount) {
            this.shardMinDocCount = new Explicit<>(shardMinDocCount, true);
        }

        public Explicit<Long> getMinDocCount() {
            return minDocCount;
        }

        public void setMinDocCount(long minDocCount) {
            this.minDocCount = new Explicit<>(minDocCount, true);
        }

        public Explicit<Integer> getRequiredSize() {
            return requiredSize;
        }

        public void setRequiredSize(int requiredSize) {
            this.requiredSize = new Explicit<>(requiredSize, true);
        }

        public Explicit<Integer> getShardSize() {
            return shardSize;
        }

        public void setShardSize(int shardSize) {
            this.shardSize = new Explicit<>(shardSize, true);
        }
    }

    protected final BucketCountThresholds bucketCountThresholds;

    public TermsAggregator(String name, BucketAggregationMode bucketAggregationMode, AggregatorFactories factories, long estimatedBucketsCount, AggregationContext context, Aggregator parent, BucketCountThresholds bucketCountThresholds) {
        super(name, bucketAggregationMode, factories, estimatedBucketsCount, context, parent);
        this.bucketCountThresholds = bucketCountThresholds;
    }
}
