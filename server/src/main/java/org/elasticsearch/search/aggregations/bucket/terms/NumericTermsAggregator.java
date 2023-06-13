/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.search.aggregations.InternalOrder.isKeyOrder;

public class NumericTermsAggregator extends TermsAggregator {
    private final ResultStrategy<?, ?> resultStrategy;
    private final ValuesSource.Numeric valuesSource;
    private final LongKeyedBucketOrds bucketOrds;
    private final LongFilter longFilter;

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
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, bucketCountThresholds, order, format, subAggCollectMode, metadata);
        this.resultStrategy = resultStrategy.apply(this); // ResultStrategy needs a reference to the Aggregator to do its job.
        this.valuesSource = valuesSource;
        this.longFilter = longFilter;
        bucketOrds = LongKeyedBucketOrds.build(bigArrays(), cardinality);
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
        SortedNumericDocValues values = resultStrategy.getValues(aggCtx.getLeafReaderContext());
        return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long val = values.nextValue();
                        if (previous != val || i == 0) {
                            if ((longFilter == null) || (longFilter.accept(val))) {
                                long bucketOrdinal = bucketOrds.add(owningBucketOrd, val);
                                if (bucketOrdinal < 0) { // already seen
                                    bucketOrdinal = -1 - bucketOrdinal;
                                    collectExistingBucket(sub, doc, bucketOrdinal);
                                } else {
                                    collectBucket(sub, doc, bucketOrdinal);
                                }
                            }

                            previous = val;
                        }
                    }
                }
            }
        });
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return resultStrategy.buildAggregations(owningBucketOrds);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return resultStrategy.buildEmptyResult();
    }

    @Override
    public void merge(Map<Long, List<AggregationAndBucket>> toMerge, BigArrays bigArrays) {
        resultStrategy.merge(toMerge, bigArrays);
    }

    public long getDocCount(long owningBucketOrd, long key) {
        long ordinal = bucketOrds.find(owningBucketOrd, key);
        return docCounts.get(ordinal);
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
        private InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            B[][] topBucketsPerOrd = buildTopBucketsPerOrd(owningBucketOrds.length);
            long[] otherDocCounts = new long[owningBucketOrds.length];
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                collectZeroDocEntriesIfNeeded(owningBucketOrds[ordIdx]);
                long bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);

                int size = (int) Math.min(bucketsInOrd, bucketCountThresholds.getShardSize());
                PriorityQueue<B> ordered = buildPriorityQueue(size);
                B spare = null;
                BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
                Supplier<B> emptyBucketBuilder = emptyBucketBuilder(owningBucketOrds[ordIdx]);
                while (ordsEnum.next()) {
                    long docCount = bucketDocCount(ordsEnum.ord());
                    otherDocCounts[ordIdx] += docCount;
                    if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                        continue;
                    }
                    if (spare == null) {
                        spare = emptyBucketBuilder.get();
                    }
                    updateBucket(spare, ordsEnum, docCount);
                    spare = ordered.insertWithOverflow(spare);
                }

                // Get the top buckets
                B[] bucketsForOrd = buildBuckets(ordered.size());
                topBucketsPerOrd[ordIdx] = bucketsForOrd;
                for (int b = ordered.size() - 1; b >= 0; --b) {
                    topBucketsPerOrd[ordIdx][b] = ordered.pop();
                    otherDocCounts[ordIdx] -= topBucketsPerOrd[ordIdx][b].getDocCount();
                }
            }

            buildSubAggs(topBucketsPerOrd);

            InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                result[ordIdx] = buildResult(owningBucketOrds[ordIdx], otherDocCounts[ordIdx], topBucketsPerOrd[ordIdx]);
            }
            return result;
        }

        /**
         *
         * @param toMerge - the aggregations to merge.  The top level list is each sub-aggregation. The Map keys are the
         *                owning bucket ordinals of the final merged data structure, for a given key.  The lists withing the map are pairs
         *                of aggregators with the ordinal for that key in that tree.
         * @param bigArrays - Used to allocate new memory for holding the merge result
         */
        // TODO: Should this tame a ReduceContext instead of just a BigArrays reference?
        public void merge(Map<Long, List<AggregationAndBucket>> toMerge, BigArrays bigArrays) {
            // It is an article of faith that all the aggregations have the same list of subAggregators in the same order
            List<Map<Long, List<AggregationAndBucket>>> nextLayer = new ArrayList<>(subAggregators.length);
            for (int i = 0; i < subAggregators.length; i++) {
                nextLayer.add(new HashMap<>());
            }
            LongKeyedBucketOrds mergedOrdinals = new LongKeyedBucketOrds.FromMany(bigArrays);

            // We know the upper limit of the size of this array is the number of owning buckets times the shard size, but in
            // practice it may be much lower, so probably better to let it grow naturally
            LongArray mergedDocCounts = bigArrays.newLongArray(1);

            // The upper limit for the size of this array is just the number of owning buckets (aka toMerge.size())
            LongArray mergedOtherBucketCounts = bigArrays.newLongArray(1);

            for (Map.Entry<Long, List<AggregationAndBucket>> mergeRow : toMerge.entrySet()) {
                List<Map<Long, List<AggregationAndBucket>>> nextLayerBuckets = mergeBucket(mergeRow.getValue(), mergeRow.getKey(),
                    (Long key, Long count) -> {
                        long ord = mergedOrdinals.add(mergeRow.getKey(), key);
                        long rawOrd = ord; // This is what we'll return, so the caller can know if it's a new bucket or not
                        if (ord < 0) { // already seen
                            ord = -1 - ord;
                            // we don't need to update the doc count because we already have it
                        } else {
                            grow(ord + 1);
                            mergedDocCounts.fill(ord - 1, ord, 0);
                        }
                        mergedDocCounts.set(ord, count);
                        return rawOrd;
                    }
                );
                assert nextLayer.size() == nextLayerBuckets.size();
                for (int i = 0; i < subAggregators.length; i++) {
                    nextLayer.get(i).putAll(nextLayerBuckets.get(i));
                }
            }
            // Trigger the next layer merge.
            for (int i = 0; i < subAggregators.length; i++) {
                subAggregators[i].merge(nextLayer.get(i), bigArrays);
            }
        }

        record KeyAndCount(long key, long docCount) { }

        public List<Map<Long, List<AggregationAndBucket>>> mergeBucket(List<AggregationAndBucket> others, long thisBucket,
        BiFunction<Long, Long, Long> addBucketCount) {
            final PriorityQueue<IteratorAndAggregator> pq = new PriorityQueue<>(others.size()) {
                @Override
                public boolean lessThan(IteratorAndAggregator a, IteratorAndAggregator b) {
                    return a.current() < b.current();
                }
            };

            // It is an article of faith that all the aggregations have the same list of subAggregators in the same order
            List<Map<Long, List<AggregationAndBucket>>> nextLayer = new ArrayList<>(subAggregators.length);
            for (int i = 0; i < subAggregators.length; i++) {
                nextLayer.add(new HashMap<>());
            }

            // Contract says that this instance should be included in the others list
            for (AggregationAndBucket other : others) {
                pq.add(
                    new IteratorAndAggregator(
                        ((NumericTermsAggregator) other.aggregator()).bucketOrds.keyOrderedIterator(other.bucketOrdinal()),
                        other.aggregator()
                    )
                );
            }

            // TODO NOCOMMIT - For the prototype, I'm hard coding doc count descending sort here.  In practice, this needs to deal with the
            // user specified sort, which may require looking into the subaggregations
            PriorityQueue<KeyAndCount> ordered = new PriorityQueue<KeyAndCount>(bucketCountThresholds.getShardSize()) {
                @Override
                protected boolean lessThan(KeyAndCount a, KeyAndCount b) {
                    return a.docCount < b.docCount;
                }
            };

            // Is it possible for the queue to be empty? Maybe this should check for >1, since we always add ourselves?
            long otherBucketDocCount = 0;
            if (pq.size() > 0) {
                // Buckets matching the current key
                List<AggregationAndBucket> currentBuckets = new ArrayList<>();
                long key = pq.top().current();
                do {
                    final IteratorAndAggregator top = pq.top();

                    if (top.current() != key) {
                        // the key changes, reduce what we already buffered and reset the buffer for current buckets

                        long docCount = 0;
                        for (AggregationAndBucket bucket : currentBuckets) {
                            // TODO NOCOMMIT: Casting here is terrible
                            docCount += ((NumericTermsAggregator) bucket.aggregator()).getDocCount(bucket.bucketOrdinal(), key);
                        }
                        ordered.insertWithOverflow(new KeyAndCount(key, docCount));
                        otherBucketDocCount += docCount;

                        // Reset the collection
                        currentBuckets.clear();
                        key = top.current();
                    }

                    currentBuckets.add(new AggregationAndBucket(top.current(), top.getAggregator()));

                    if (top.hasNext()) {
                        top.next();
                        assert top.current() > key : "shards must return data sorted by key";
                        pq.updateTop();
                    } else {
                        pq.pop();
                    }
                } while (pq.size() > 0);

                // Collect outputs
                for (KeyAndCount keep : ordered) {
                    long mergedBucketOrd = addBucketCount.apply(keep.key, keep.docCount);
                    // We should always be creating a new bucket here
                    assert mergedBucketOrd > 0;
                    // set up the next layer of reduction
                    for (int i = 0; i < subAggregators().length; i++) {
                        List<AggregationAndBucket> aggsAndOrds = new ArrayList<>();
                        for (AggregationAndBucket agg : others) {
                            // TODO: We shouldn't need to cast here
                            long ord = ((NumericTermsAggregator) agg.aggregator()).bucketOrds.find(agg.bucketOrdinal(), keep.key);
                            if (ord >= 0) {
                                aggsAndOrds.add(new AggregationAndBucket(ord, agg.aggregator()));
                            }
                        }
                        nextLayer.get(i).put(mergedBucketOrd, aggsAndOrds);
                    }
                }
            }
            return nextLayer;
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
         * Build an empty array to hold the "top" buckets for each ordinal.
         */
        abstract B[][] buildTopBucketsPerOrd(int size);

        /**
         * Build an array of buckets for a particular ordinal. These arrays
         * are asigned to the value returned by {@link #buildTopBucketsPerOrd}.
         */
        abstract B[] buildBuckets(int size);

        /**
         * Build a {@linkplain Supplier} that can be used to build "empty"
         * buckets. Those buckets will then be {@link #updateBucket updated}
         * for each collected bucket.
         */
        abstract Supplier<B> emptyBucketBuilder(long owningBucketOrd);

        /**
         * Update fields in {@code spare} to reflect information collected for
         * this bucket ordinal.
         */
        abstract void updateBucket(B spare, BucketOrdsEnum ordsEnum, long docCount) throws IOException;

        /**
         * Build a {@link PriorityQueue} to sort the buckets. After we've
         * collected all of the buckets we'll collect all entries in the queue.
         */
        abstract PriorityQueue<B> buildPriorityQueue(int size);

        /**
         * Build the sub-aggregations into the buckets. This will usually
         * delegate to {@link #buildSubAggsForAllBuckets}.
         */
        abstract void buildSubAggs(B[][] topBucketsPerOrd) throws IOException;

        /**
         * Collect extra entries for "zero" hit documents if they were requested
         * and required.
         */
        abstract void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException;

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

    abstract class StandardTermsResultStrategy<R extends InternalMappedTerms<R, B>, B extends InternalTerms.Bucket<B>> extends
        ResultStrategy<R, B> {
        protected final boolean showTermDocCountError;

        StandardTermsResultStrategy(boolean showTermDocCountError) {
            this.showTermDocCountError = showTermDocCountError;
        }

        @Override
        final LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return primary;
        }

        @Override
        final PriorityQueue<B> buildPriorityQueue(int size) {
            return new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
        }

        @Override
        final void buildSubAggs(B[][] topBucketsPerOrd) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        Supplier<B> emptyBucketBuilder(long owningBucketOrd) {
            return this::buildEmptyBucket;
        }

        abstract B buildEmptyBucket();

        @Override
        final void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException {
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
        LongTermsResults(boolean showTermDocCountError) {
            super(showTermDocCountError);
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
        LongTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new LongTerms.Bucket[size][];
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
        void updateBucket(LongTerms.Bucket spare, BucketOrdsEnum ordsEnum, long docCount) {
            spare.term = ordsEnum.value();
            spare.docCount = docCount;
            spare.bucketOrd = ordsEnum.ord();
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
                List.of(topBuckets),
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

        DoubleTermsResults(boolean showTermDocCountError) {
            super(showTermDocCountError);
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
        DoubleTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new DoubleTerms.Bucket[size][];
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
        void updateBucket(DoubleTerms.Bucket spare, BucketOrdsEnum ordsEnum, long docCount) {
            spare.term = NumericUtils.sortableLongToDouble(ordsEnum.value());
            spare.docCount = docCount;
            spare.bucketOrd = ordsEnum.ord();
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
                List.of(topBuckets),
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
        SignificantLongTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new SignificantLongTerms.Bucket[size][];
        }

        @Override
        SignificantLongTerms.Bucket[] buildBuckets(int size) {
            return new SignificantLongTerms.Bucket[size];
        }

        @Override
        Supplier<SignificantLongTerms.Bucket> emptyBucketBuilder(long owningBucketOrd) {
            long subsetSize = subsetSizes.get(owningBucketOrd);
            return () -> new SignificantLongTerms.Bucket(0, subsetSize, 0, supersetSize, 0, null, format, 0);
        }

        @Override
        void updateBucket(SignificantLongTerms.Bucket spare, BucketOrdsEnum ordsEnum, long docCount) throws IOException {
            spare.term = ordsEnum.value();
            spare.subsetDf = docCount;
            spare.supersetDf = backgroundFrequencies.freq(spare.term);
            spare.bucketOrd = ordsEnum.ord();
            // During shard-local down-selection we use subset/superset stats that are for this shard only
            // Back at the central reducer these properties will be updated with global stats
            spare.updateScore(significanceHeuristic);
        }

        @Override
        PriorityQueue<SignificantLongTerms.Bucket> buildPriorityQueue(int size) {
            return new BucketSignificancePriorityQueue<>(size);
        }

        @Override
        void buildSubAggs(SignificantLongTerms.Bucket[][] topBucketsPerOrd) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException {}

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
                List.of(topBuckets)
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
