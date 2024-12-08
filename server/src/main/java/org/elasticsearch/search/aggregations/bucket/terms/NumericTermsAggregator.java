/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.util.ObjectArrayPriorityQueue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude.LongFilter;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds.BucketOrdsEnum;
import org.elasticsearch.search.aggregations.bucket.terms.SignificanceLookup.BackgroundFrequencyForLong;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.search.aggregations.InternalOrder.isKeyOrder;

public final class NumericTermsAggregator extends TermsAggregator {
    private final ResultStrategy<?, ?> resultStrategy;
    private final ValuesSource.Numeric valuesSource;
    private final LongKeyedBucketOrds bucketOrds;
    private final LongFilter longFilter;
    private final boolean excludeDeletedDocs;

    public NumericTermsAggregator(
        String name,
        AggregatorFactories factories,
        Function<NumericTermsAggregator, ResultStrategy<?, ?>> resultStrategy,
        ValuesSource.Numeric valuesSource,
        DocValueFormat format,
        BucketOrder order,
        BucketCountThresholds bucketCountThresholds,
        AggregationContext context,
        Aggregator parent,
        SubAggCollectionMode subAggCollectMode,
        IncludeExclude.LongFilter longFilter,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata,
        boolean excludeDeletedDocs
    ) throws IOException {
        super(name, factories, context, parent, bucketCountThresholds, order, format, subAggCollectMode, metadata);
        this.resultStrategy = resultStrategy.apply(this); // ResultStrategy needs a reference to the Aggregator to do its job.
        this.valuesSource = valuesSource;
        this.longFilter = longFilter;
        bucketOrds = LongKeyedBucketOrds.build(bigArrays(), cardinality);
        this.excludeDeletedDocs = excludeDeletedDocs;
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        final SortedNumericDocValues values = resultStrategy.getValues(aggCtx.getLeafReaderContext());
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        return resultStrategy.wrapCollector(singleton != null ? getLeafCollector(singleton, sub) : getLeafCollector(values, sub));
    }

    private LeafBucketCollector getLeafCollector(SortedNumericDocValues values, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < values.docValueCount(); ++i) {
                        long val = values.nextValue();
                        if (previous != val || i == 0) {
                            collectValue(val, doc, owningBucketOrd, sub);
                            previous = val;
                        }
                    }
                }
            }
        };
    }

    private LeafBucketCollector getLeafCollector(NumericDocValues values, LeafBucketCollector sub) {
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    collectValue(values.longValue(), doc, owningBucketOrd, sub);
                }
            }
        };
    }

    private void collectValue(long val, int doc, long owningBucketOrd, LeafBucketCollector sub) throws IOException {
        if (longFilter == null || longFilter.accept(val)) {
            long bucketOrdinal = bucketOrds.add(owningBucketOrd, val);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = -1 - bucketOrdinal;
                collectExistingBucket(sub, doc, bucketOrdinal);
            } else {
                collectBucket(sub, doc, bucketOrdinal);
            }
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        return resultStrategy.buildAggregations(owningBucketOrds);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return resultStrategy.buildEmptyResult();
    }

    @Override
    public void doClose() {
        Releasables.close(super::doClose, bucketOrds, resultStrategy);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("result_strategy", resultStrategy.describe());
        add.accept("total_buckets", bucketOrds.size());
    }

    /**
     * Strategy for building results.
     */
    abstract class ResultStrategy<R extends InternalAggregation, B extends InternalMultiBucketAggregation.InternalBucket>
        implements
            Releasable {
        private InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
            try (
                LongArray otherDocCounts = bigArrays().newLongArray(owningBucketOrds.size(), true);
                ObjectArray<B[]> topBucketsPerOrd = buildTopBucketsPerOrd(owningBucketOrds.size())
            ) {
                try (IntArray bucketsToCollect = bigArrays().newIntArray(owningBucketOrds.size())) {
                    long ordsToCollect = 0;
                    for (long ordIdx = 0; ordIdx < owningBucketOrds.size(); ordIdx++) {
                        final long owningBucketOrd = owningBucketOrds.get(ordIdx);
                        collectZeroDocEntriesIfNeeded(owningBucketOrd, excludeDeletedDocs);
                        int size = (int) Math.min(bucketOrds.bucketsInOrd(owningBucketOrd), bucketCountThresholds.getShardSize());
                        bucketsToCollect.set(ordIdx, size);
                        ordsToCollect += size;
                    }
                    try (LongArray ordsArray = bigArrays().newLongArray(ordsToCollect)) {
                        long ordsCollected = 0;
                        for (long ordIdx = 0; ordIdx < topBucketsPerOrd.size(); ordIdx++) {
                            final long owningBucketOrd = owningBucketOrds.get(ordIdx);
                            try (ObjectArrayPriorityQueue<BucketAndOrd<B>> ordered = buildPriorityQueue(bucketsToCollect.get(ordIdx))) {
                                BucketAndOrd<B> spare = null;
                                BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrd);
                                BucketUpdater<B> bucketUpdater = bucketUpdater(owningBucketOrd);
                                while (ordsEnum.next()) {
                                    long docCount = bucketDocCount(ordsEnum.ord());
                                    otherDocCounts.increment(ordIdx, docCount);
                                    if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                                        continue;
                                    }
                                    if (spare == null) {
                                        checkRealMemoryCBForInternalBucket();
                                        spare = new BucketAndOrd<>(buildEmptyBucket());
                                    }
                                    bucketUpdater.updateBucket(spare.bucket, ordsEnum, docCount);
                                    spare.ord = ordsEnum.ord();
                                    spare = ordered.insertWithOverflow(spare);
                                }

                                // Get the top buckets
                                final int orderedSize = (int) ordered.size();
                                final B[] bucketsForOrd = buildBuckets(orderedSize);
                                for (int b = orderedSize - 1; b >= 0; --b) {
                                    BucketAndOrd<B> bucketAndOrd = ordered.pop();
                                    bucketsForOrd[b] = bucketAndOrd.bucket;
                                    ordsArray.set(ordsCollected + b, bucketAndOrd.ord);
                                    otherDocCounts.increment(ordIdx, -bucketAndOrd.bucket.getDocCount());
                                }
                                topBucketsPerOrd.set(ordIdx, bucketsForOrd);
                                ordsCollected += orderedSize;

                            }
                        }
                        assert ordsCollected == ordsArray.size();
                        buildSubAggs(topBucketsPerOrd, ordsArray);
                    }
                }
                return NumericTermsAggregator.this.buildAggregations(
                    Math.toIntExact(owningBucketOrds.size()),
                    ordIdx -> buildResult(owningBucketOrds.get(ordIdx), otherDocCounts.get(ordIdx), topBucketsPerOrd.get(ordIdx))
                );
            }
        }

        /**
         * Short description of the collection mechanism added to the profile
         * output to help with debugging.
         */
        abstract String describe();

        /**
         * Resolve the doc values to collect results of this type.
         */
        abstract SortedNumericDocValues getValues(LeafReaderContext ctx) throws IOException;

        /**
         * Wrap the "standard" numeric terms collector to collect any more
         * information that this result type may need.
         */
        abstract LeafBucketCollector wrapCollector(LeafBucketCollector primary);

        /**
         * Build an array to hold the "top" buckets for each ordinal.
         */
        abstract ObjectArray<B[]> buildTopBucketsPerOrd(long size);

        /**
         * Build an array of buckets for a particular ordinal. These arrays
         * are asigned to the value returned by {@link #buildTopBucketsPerOrd}.
         */
        abstract B[] buildBuckets(int size);

        /**
         * Build an empty bucket. Those buckets will then be {@link #bucketUpdater(long)}  updated}
         * for each collected bucket.
         */
        abstract B buildEmptyBucket();

        /**
         * Update fields in {@code spare} to reflect information collected for
         * this bucket ordinal.
         */
        abstract BucketUpdater<B> bucketUpdater(long owningBucketOrd);

        /**
         * Build a {@link ObjectArrayPriorityQueue} to sort the buckets. After we've
         * collected all of the buckets we'll collect all entries in the queue.
         */
        abstract ObjectArrayPriorityQueue<BucketAndOrd<B>> buildPriorityQueue(int size);

        /**
         * Build the sub-aggregations into the buckets. This will usually
         * delegate to {@link #buildSubAggsForAllBuckets(ObjectArray, LongArray, BiConsumer)}.
         */
        abstract void buildSubAggs(ObjectArray<B[]> topBucketsPerOrd, LongArray ordsArray) throws IOException;

        /**
         * Collect extra entries for "zero" hit documents if they were requested
         * and required.
         */
        abstract void collectZeroDocEntriesIfNeeded(long owningBucketOrd, boolean excludeDeletedDocs) throws IOException;

        /**
         * Turn the buckets into an aggregation result.
         */
        abstract R buildResult(long owningBucketOrd, long otherDocCounts, B[] topBuckets);

        /**
         * Build an "empty" result. Only called if there isn't any data on this
         * shard.
         */
        abstract R buildEmptyResult();
    }

    interface BucketUpdater<B extends InternalMultiBucketAggregation.InternalBucket> {
        void updateBucket(B spare, BucketOrdsEnum ordsEnum, long docCount) throws IOException;
    }

    abstract class StandardTermsResultStrategy<R extends InternalMappedTerms<R, B>, B extends InternalTerms.Bucket<B>> extends
        ResultStrategy<R, B> {
        protected final boolean showTermDocCountError;
        private final Comparator<BucketAndOrd<B>> comparator;

        StandardTermsResultStrategy(boolean showTermDocCountError, Aggregator aggregator) {
            this.showTermDocCountError = showTermDocCountError;
            this.comparator = order.partiallyBuiltBucketComparator(aggregator);
        }

        @Override
        final LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return primary;
        }

        @Override
        final ObjectArrayPriorityQueue<BucketAndOrd<B>> buildPriorityQueue(int size) {
            return new BucketPriorityQueue<>(size, bigArrays(), comparator);
        }

        @Override
        final void buildSubAggs(ObjectArray<B[]> topBucketsPerOrd, LongArray ordsArray) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, ordsArray, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        final void collectZeroDocEntriesIfNeeded(long owningBucketOrd, boolean excludeDeletedDocs) throws IOException {
            if (bucketCountThresholds.getMinDocCount() != 0) {
                return;
            }
            if (InternalOrder.isCountDesc(order) && bucketOrds.bucketsInOrd(owningBucketOrd) >= bucketCountThresholds.getRequiredSize()) {
                return;
            }
            // we need to fill-in the blanks
            for (LeafReaderContext ctx : searcher().getTopReaderContext().leaves()) {
                SortedNumericDocValues values = getValues(ctx);
                for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                    if (excludeDeletedDocs && ctx.reader().getLiveDocs() != null && ctx.reader().getLiveDocs().get(docId) == false) {
                        continue;
                    }
                    if (values.advanceExact(docId)) {
                        int valueCount = values.docValueCount();
                        for (int v = 0; v < valueCount; ++v) {
                            long value = values.nextValue();
                            if (longFilter == null || longFilter.accept(value)) {
                                bucketOrds.add(owningBucketOrd, value);
                            }
                        }
                    }
                }
            }
        }

        @Override
        public final void close() {}
    }

    class LongTermsResults extends StandardTermsResultStrategy<LongTerms, LongTerms.Bucket> {
        LongTermsResults(boolean showTermDocCountError, Aggregator aggregator) {
            super(showTermDocCountError, aggregator);
        }

        @Override
        String describe() {
            return "long_terms";
        }

        @Override
        SortedNumericDocValues getValues(LeafReaderContext ctx) throws IOException {
            return valuesSource.longValues(ctx);
        }

        @Override
        ObjectArray<LongTerms.Bucket[]> buildTopBucketsPerOrd(long size) {
            return bigArrays().newObjectArray(size);
        }

        @Override
        LongTerms.Bucket[] buildBuckets(int size) {
            return new LongTerms.Bucket[size];
        }

        @Override
        LongTerms.Bucket buildEmptyBucket() {
            return new LongTerms.Bucket(0, 0, null, showTermDocCountError, 0, format);
        }

        @Override
        BucketUpdater<LongTerms.Bucket> bucketUpdater(long owningBucketOrd) {
            return (LongTerms.Bucket spare, BucketOrdsEnum ordsEnum, long docCount) -> {
                spare.term = ordsEnum.value();
                spare.docCount = docCount;
            };
        }

        @Override
        LongTerms buildResult(long owningBucketOrd, long otherDocCount, LongTerms.Bucket[] topBuckets) {
            final BucketOrder reduceOrder;
            if (isKeyOrder(order) == false) {
                reduceOrder = InternalOrder.key(true);
                Arrays.sort(topBuckets, reduceOrder.comparator());
            } else {
                reduceOrder = order;
            }
            return new LongTerms(
                name,
                reduceOrder,
                order,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                otherDocCount,
                Arrays.asList(topBuckets),
                null
            );
        }

        @Override
        LongTerms buildEmptyResult() {
            return new LongTerms(
                name,
                order,
                order,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                0,
                emptyList(),
                0L
            );
        }
    }

    class DoubleTermsResults extends StandardTermsResultStrategy<DoubleTerms, DoubleTerms.Bucket> {

        DoubleTermsResults(boolean showTermDocCountError, Aggregator aggregator) {
            super(showTermDocCountError, aggregator);
        }

        @Override
        String describe() {
            return "double_terms";
        }

        @Override
        SortedNumericDocValues getValues(LeafReaderContext ctx) throws IOException {
            return FieldData.toSortableLongBits(valuesSource.doubleValues(ctx));
        }

        @Override
        ObjectArray<DoubleTerms.Bucket[]> buildTopBucketsPerOrd(long size) {
            return bigArrays().newObjectArray(size);
        }

        @Override
        DoubleTerms.Bucket[] buildBuckets(int size) {
            return new DoubleTerms.Bucket[size];
        }

        @Override
        DoubleTerms.Bucket buildEmptyBucket() {
            return new DoubleTerms.Bucket(0, 0, null, showTermDocCountError, 0, format);
        }

        @Override
        BucketUpdater<DoubleTerms.Bucket> bucketUpdater(long owningBucketOrd) {
            return (DoubleTerms.Bucket spare, BucketOrdsEnum ordsEnum, long docCount) -> {
                spare.term = NumericUtils.sortableLongToDouble(ordsEnum.value());
                spare.docCount = docCount;
            };
        }

        @Override
        DoubleTerms buildResult(long owningBucketOrd, long otherDocCount, DoubleTerms.Bucket[] topBuckets) {
            final BucketOrder reduceOrder;
            if (isKeyOrder(order) == false) {
                reduceOrder = InternalOrder.key(true);
                Arrays.sort(topBuckets, reduceOrder.comparator());
            } else {
                reduceOrder = order;
            }
            return new DoubleTerms(
                name,
                reduceOrder,
                order,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                otherDocCount,
                Arrays.asList(topBuckets),
                null
            );
        }

        @Override
        DoubleTerms buildEmptyResult() {
            return new DoubleTerms(
                name,
                order,
                order,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                0,
                emptyList(),
                0L
            );
        }
    }

    class SignificantLongTermsResults extends ResultStrategy<SignificantLongTerms, SignificantLongTerms.Bucket> {
        private final BackgroundFrequencyForLong backgroundFrequencies;
        private final long supersetSize;
        private final SignificanceHeuristic significanceHeuristic;
        private LongArray subsetSizes;

        SignificantLongTermsResults(
            SignificanceLookup significanceLookup,
            SignificanceHeuristic significanceHeuristic,
            CardinalityUpperBound cardinality
        ) {
            backgroundFrequencies = significanceLookup.longLookup(bigArrays(), cardinality);
            supersetSize = significanceLookup.supersetSize();
            this.significanceHeuristic = significanceHeuristic;
            boolean success = false;
            try {
                subsetSizes = bigArrays().newLongArray(1, true);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        @Override
        SortedNumericDocValues getValues(LeafReaderContext ctx) throws IOException {
            return valuesSource.longValues(ctx);
        }

        @Override
        String describe() {
            return "significant_terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return new LeafBucketCollectorBase(primary, null) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    super.collect(doc, owningBucketOrd);
                    subsetSizes = bigArrays().grow(subsetSizes, owningBucketOrd + 1);
                    subsetSizes.increment(owningBucketOrd, 1);
                }
            };
        }

        @Override
        ObjectArray<SignificantLongTerms.Bucket[]> buildTopBucketsPerOrd(long size) {
            return bigArrays().newObjectArray(size);
        }

        @Override
        SignificantLongTerms.Bucket[] buildBuckets(int size) {
            return new SignificantLongTerms.Bucket[size];
        }

        @Override
        SignificantLongTerms.Bucket buildEmptyBucket() {
            return new SignificantLongTerms.Bucket(0, 0, 0, null, format, 0);
        }

        @Override
        BucketUpdater<SignificantLongTerms.Bucket> bucketUpdater(long owningBucketOrd) {
            long subsetSize = subsetSizes.get(owningBucketOrd);
            return (spare, ordsEnum, docCount) -> {
                spare.term = ordsEnum.value();
                spare.subsetDf = docCount;
                spare.supersetDf = backgroundFrequencies.freq(spare.term);
                // During shard-local down-selection we use subset/superset stats that are for this shard only
                // Back at the central reducer these properties will be updated with global stats
                spare.updateScore(significanceHeuristic, subsetSize, supersetSize);
            };
        }

        @Override
        ObjectArrayPriorityQueue<BucketAndOrd<SignificantLongTerms.Bucket>> buildPriorityQueue(int size) {
            return new BucketSignificancePriorityQueue<>(size, bigArrays());
        }

        @Override
        void buildSubAggs(ObjectArray<SignificantLongTerms.Bucket[]> topBucketsPerOrd, LongArray ordsArray) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, ordsArray, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        void collectZeroDocEntriesIfNeeded(long owningBucketOrd, boolean excludeDeletedDocs) throws IOException {}

        @Override
        SignificantLongTerms buildResult(long owningBucketOrd, long otherDocCoun, SignificantLongTerms.Bucket[] topBuckets) {
            return new SignificantLongTerms(
                name,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                subsetSizes.get(owningBucketOrd),
                supersetSize,
                significanceHeuristic,
                Arrays.asList(topBuckets)
            );
        }

        @Override
        SignificantLongTerms buildEmptyResult() {
            return new SignificantLongTerms(
                name,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                0,
                supersetSize,
                significanceHeuristic,
                emptyList()
            );
        }

        @Override
        public void close() {
            Releasables.close(backgroundFrequencies, subsetSizes);
        }
    }

}
