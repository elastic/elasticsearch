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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.InternalOrder.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class TermsAggregator extends BucketsAggregator {

    public static class BucketCountThresholds implements Writeable, ToXContent {
        private long minDocCount;
        private long shardMinDocCount;
        private int requiredSize;
        private int shardSize;

        public BucketCountThresholds(long minDocCount, long shardMinDocCount, int requiredSize, int shardSize) {
            this.minDocCount = minDocCount;
            this.shardMinDocCount = shardMinDocCount;
            this.requiredSize = requiredSize;
            this.shardSize = shardSize;
        }

        /**
         * Read from a stream.
         */
        public BucketCountThresholds(StreamInput in) throws IOException {
            requiredSize = in.readInt();
            shardSize = in.readInt();
            minDocCount = in.readLong();
            shardMinDocCount = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(requiredSize);
            out.writeInt(shardSize);
            out.writeLong(minDocCount);
            out.writeLong(shardMinDocCount);
        }

        public BucketCountThresholds(BucketCountThresholds bucketCountThresholds) {
            this(bucketCountThresholds.minDocCount, bucketCountThresholds.shardMinDocCount, bucketCountThresholds.requiredSize,
                    bucketCountThresholds.shardSize);
        }

        public void ensureValidity() {

            // shard_size cannot be smaller than size as we need to at least fetch <size> entries from every shards in order to return <size>
            if (shardSize < requiredSize) {
                setShardSize(requiredSize);
            }

            // shard_min_doc_count should not be larger than min_doc_count because this can cause buckets to be removed that would match the min_doc_count criteria
            if (shardMinDocCount > minDocCount) {
                setShardMinDocCount(minDocCount);
            }

            if (requiredSize <= 0 || shardSize <= 0) {
                throw new ElasticsearchException("parameters [required_size] and [shard_size] must be >0 in terms aggregation.");
            }

            if (minDocCount < 0 || shardMinDocCount < 0) {
                throw new ElasticsearchException("parameter [min_doc_count] and [shardMinDocCount] must be >=0 in terms aggregation.");
            }
        }

        public long getShardMinDocCount() {
            return shardMinDocCount;
        }

        public void setShardMinDocCount(long shardMinDocCount) {
            this.shardMinDocCount = shardMinDocCount;
        }

        public long getMinDocCount() {
            return minDocCount;
        }

        public void setMinDocCount(long minDocCount) {
            this.minDocCount = minDocCount;
        }

        public int getRequiredSize() {
            return requiredSize;
        }

        public void setRequiredSize(int requiredSize) {
            this.requiredSize = requiredSize;
        }

        public int getShardSize() {
            return shardSize;
        }

        public void setShardSize(int shardSize) {
            this.shardSize = shardSize;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(TermsAggregationBuilder.REQUIRED_SIZE_FIELD_NAME.getPreferredName(), requiredSize);
            builder.field(TermsAggregationBuilder.SHARD_SIZE_FIELD_NAME.getPreferredName(), shardSize);
            builder.field(TermsAggregationBuilder.MIN_DOC_COUNT_FIELD_NAME.getPreferredName(), minDocCount);
            builder.field(TermsAggregationBuilder.SHARD_MIN_DOC_COUNT_FIELD_NAME.getPreferredName(), shardMinDocCount);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(requiredSize, shardSize, minDocCount, shardMinDocCount);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            BucketCountThresholds other = (BucketCountThresholds) obj;
            return Objects.equals(requiredSize, other.requiredSize)
                    && Objects.equals(shardSize, other.shardSize)
                    && Objects.equals(minDocCount, other.minDocCount)
                    && Objects.equals(shardMinDocCount, other.shardMinDocCount);
        }
    }

    protected final DocValueFormat format;
    protected final BucketCountThresholds bucketCountThresholds;
    protected final Terms.Order order;
    protected final Set<Aggregator> aggsUsedForSorting = new HashSet<>();
    protected final SubAggCollectionMode collectMode;

    public TermsAggregator(String name, AggregatorFactories factories, AggregationContext context, Aggregator parent,
            BucketCountThresholds bucketCountThresholds, Terms.Order order, DocValueFormat format, SubAggCollectionMode collectMode,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.bucketCountThresholds = bucketCountThresholds;
        this.order = InternalOrder.validate(order, this);
        this.format = format;
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
                && !aggsUsedForSorting.contains(aggregator);
    }

}
