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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.InternalOrder.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

            if (shardSize.value() == 0) {
                setShardSize(Integer.MAX_VALUE);
            }

            if (requiredSize.value() == 0) {
                setRequiredSize(Integer.MAX_VALUE);
            }
            // shard_size cannot be smaller than size as we need to at least fetch <size> entries from every shards in order to return <size>
            if (shardSize.value() < requiredSize.value()) {
                setShardSize(requiredSize.value());
            }

            // shard_min_doc_count should not be larger than min_doc_count because this can cause buckets to be removed that would match the min_doc_count criteria
            if (shardMinDocCount.value() > minDocCount.value()) {
                setShardMinDocCount(minDocCount.value());
            }

            if (requiredSize.value() < 0 || minDocCount.value() < 0) {
                throw new ElasticsearchException("parameters [requiredSize] and [minDocCount] must be >=0 in terms aggregation.");
            }
        }

        public long getShardMinDocCount() {
            return shardMinDocCount.value();
        }

        public void setShardMinDocCount(long shardMinDocCount) {
            this.shardMinDocCount = new Explicit<>(shardMinDocCount, true);
        }

        public long getMinDocCount() {
            return minDocCount.value();
        }

        public void setMinDocCount(long minDocCount) {
            this.minDocCount = new Explicit<>(minDocCount, true);
        }

        public int getRequiredSize() {
            return requiredSize.value();
        }

        public void setRequiredSize(int requiredSize) {
            this.requiredSize = new Explicit<>(requiredSize, true);
        }

        public int getShardSize() {
            return shardSize.value();
        }

        public void setShardSize(int shardSize) {
            this.shardSize = new Explicit<>(shardSize, true);
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            if (requiredSize.explicit()) {
                builder.field(AbstractTermsParametersParser.REQUIRED_SIZE_FIELD_NAME.getPreferredName(), requiredSize.value());
            }
            if (shardSize.explicit()) {
                builder.field(AbstractTermsParametersParser.SHARD_SIZE_FIELD_NAME.getPreferredName(), shardSize.value());
            }
            if (minDocCount.explicit()) {
                builder.field(AbstractTermsParametersParser.MIN_DOC_COUNT_FIELD_NAME.getPreferredName(), minDocCount.value());
            }
            if (shardMinDocCount.explicit()) {
                builder.field(AbstractTermsParametersParser.SHARD_MIN_DOC_COUNT_FIELD_NAME.getPreferredName(), shardMinDocCount.value());
            }
        }
    }

    protected final BucketCountThresholds bucketCountThresholds;
    protected final Terms.Order order;
    protected final Set<Aggregator> aggsUsedForSorting = new HashSet<>();
    protected final SubAggCollectionMode collectMode;

    public TermsAggregator(String name, AggregatorFactories factories, AggregationContext context, Aggregator parent, BucketCountThresholds bucketCountThresholds, Terms.Order order, SubAggCollectionMode collectMode, Map<String, Object> metaData) throws IOException {
        super(name, factories, context, parent, metaData);
        this.bucketCountThresholds = bucketCountThresholds;
        this.order = InternalOrder.validate(order, this);
        this.collectMode = collectMode;
        // Don't defer any child agg if we are dependent on it for pruning results
        if (order instanceof Aggregation){
            AggregationPath path = ((Aggregation) order).path();
            aggsUsedForSorting.add(path.resolveTopmostAggregator(this));
        } else if (order instanceof CompoundOrder) {
            CompoundOrder compoundOrder = (CompoundOrder) order;
            for (Terms.Order orderElement : compoundOrder.orderElements()) {
                if (orderElement instanceof Aggregation) {
                    AggregationPath path = ((Aggregation) orderElement).path();
                    aggsUsedForSorting.add(path.resolveTopmostAggregator(this));
                }
            }
        }
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return collectMode == SubAggCollectionMode.BREADTH_FIRST
                && aggregator.needsScores() == false
                && !aggsUsedForSorting.contains(aggregator);
    }

}
