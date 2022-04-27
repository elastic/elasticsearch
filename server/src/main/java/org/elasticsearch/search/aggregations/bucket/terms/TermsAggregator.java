/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder.Aggregation;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class TermsAggregator extends DeferableBucketAggregator {

    public static class BucketCountThresholds implements Writeable, ToXContentFragment {
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
            this(
                bucketCountThresholds.minDocCount,
                bucketCountThresholds.shardMinDocCount,
                bucketCountThresholds.requiredSize,
                bucketCountThresholds.shardSize
            );
        }

        public void ensureValidity() {

            // shard_size cannot be smaller than size as we need to at least fetch <size> entries from every shards in order to return
            // <size>
            if (shardSize < requiredSize) {
                setShardSize(requiredSize);
            }

            // shard_min_doc_count should not be larger than min_doc_count because this can cause buckets to be removed that would match
            // the min_doc_count criteria
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

        /**
         * The minimum number of documents a bucket must have in order to
         * be returned from a shard.
         * <p>
         * Important: The default for this is 0, but we should only return
         * 0 document buckets if {@link #getMinDocCount()} is *also* 0.
         */
        public long getShardMinDocCount() {
            return shardMinDocCount;
        }

        public void setShardMinDocCount(long shardMinDocCount) {
            this.shardMinDocCount = shardMinDocCount;
        }

        /**
         * The minimum numbers of documents a bucket must have in order to
         * survive the final reduction.
         */
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
            if (shardSize != -1) {
                builder.field(TermsAggregationBuilder.SHARD_SIZE_FIELD_NAME.getPreferredName(), shardSize);
            }
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
    protected final BucketOrder order;
    protected final Comparator<InternalTerms.Bucket<?>> partiallyBuiltBucketComparator;
    protected final Set<Aggregator> aggsUsedForSorting;
    protected final SubAggCollectionMode collectMode;

    public TermsAggregator(
        String name,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        BucketCountThresholds bucketCountThresholds,
        BucketOrder order,
        DocValueFormat format,
        SubAggCollectionMode collectMode,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.bucketCountThresholds = bucketCountThresholds;
        this.order = order;
        partiallyBuiltBucketComparator = order == null ? null : order.partiallyBuiltBucketComparator(b -> b.bucketOrd, this);
        this.format = format;
        if ((subAggsNeedScore() && descendsFromNestedAggregator(parent)) || context.isInSortOrderExecutionRequired()) {
            /**
             * Force the execution to depth_first because we need to access the score of
             * nested documents in a sub-aggregation and we are not able to generate this score
             * while replaying deferred documents.
             *
             * We also force depth_first for time-series aggs executions since they need to be visited in a particular order (index
             * sort order) which might be changed by the breadth_first execution.
             */
            this.collectMode = SubAggCollectionMode.DEPTH_FIRST;
        } else {
            this.collectMode = collectMode;
        }
        aggsUsedForSorting = aggsUsedForSorting(this, order);
    }

    /**
     * Walks through bucket order and extracts all aggregations used for sorting
     */
    public static Set<Aggregator> aggsUsedForSorting(Aggregator root, BucketOrder order) {
        Set<Aggregator> aggsUsedForSorting = new HashSet<>();
        // Don't defer any child agg if we are dependent on it for pruning results
        if (order instanceof Aggregation) {
            AggregationPath path = ((Aggregation) order).path();
            aggsUsedForSorting.add(path.resolveTopmostAggregator(root));
        } else if (order instanceof CompoundOrder compoundOrder) {
            for (BucketOrder orderElement : compoundOrder.orderElements()) {
                if (orderElement instanceof Aggregation) {
                    AggregationPath path = ((Aggregation) orderElement).path();
                    aggsUsedForSorting.add(path.resolveTopmostAggregator(root));
                }
            }
        }
        return aggsUsedForSorting;
    }

    public static boolean descendsFromNestedAggregator(Aggregator parent) {
        while (parent != null) {
            if (parent.getClass() == NestedAggregator.class) {
                return true;
            }
            parent = parent.parent();
        }
        return false;
    }

    private boolean subAggsNeedScore() {
        for (Aggregator subAgg : subAggregators) {
            if (subAgg.scoreMode().needsScores()) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return collectMode == SubAggCollectionMode.BREADTH_FIRST && aggsUsedForSorting.contains(aggregator) == false;
    }
}
