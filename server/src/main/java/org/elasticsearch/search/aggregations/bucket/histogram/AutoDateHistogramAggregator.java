/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BestBucketsDeferringCollector;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder.RoundingInfo;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;

/**
 * An aggregator for date values that attempts to return a specific number of
 * buckets, reconfiguring how it rounds dates to buckets on the fly as new
 * data arrives.
 * <p>
 * This class is abstract because there is a simple implementation for when the
 * aggregator only collects from a single bucket and a more complex
 * implementation when it doesn't. This ain't great from a test coverage
 * standpoint but the simpler implementation is between 7% and 15% faster
 * when you can use it. This is an important aggregation and we need that
 * performance.
 */
abstract class AutoDateHistogramAggregator extends DeferableBucketAggregator {
    static AutoDateHistogramAggregator build(
        String name,
        AggregatorFactories factories,
        int targetBuckets,
        RoundingInfo[] roundingInfos,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return cardinality == CardinalityUpperBound.ONE
            ? new FromSingle(name, factories, targetBuckets, roundingInfos, valuesSourceConfig, context, parent, metadata)
            : new FromMany(name, factories, targetBuckets, roundingInfos, valuesSourceConfig, context, parent, metadata);
    }

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat formatter;
    private final Function<Rounding, Rounding.Prepared> roundingPreparer;
    /**
     * A reference to the collector so we can
     * {@link BestBucketsDeferringCollector#rewriteBuckets}.
     */
    private BestBucketsDeferringCollector deferringCollector;

    protected final RoundingInfo[] roundingInfos;
    protected final int targetBuckets;

    private AutoDateHistogramAggregator(
        String name,
        AggregatorFactories factories,
        int targetBuckets,
        RoundingInfo[] roundingInfos,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {

        super(name, factories, context, parent, metadata);
        this.targetBuckets = targetBuckets;
        // TODO: Remove null usage here, by using a different aggregator for create
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
        this.formatter = valuesSourceConfig.format();
        this.roundingInfos = roundingInfos;
        this.roundingPreparer = valuesSourceConfig.roundingPreparer();
    }

    @Override
    public final ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    protected final boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public final DeferringBucketCollector buildDeferringCollector() {
        deferringCollector = new BestBucketsDeferringCollector(topLevelQuery(), searcher(), descendsFromGlobalAggregator(parent()));
        return deferringCollector;
    }

    protected abstract LeafBucketCollector getLeafCollector(SortedNumericDocValues values, LeafBucketCollector sub) throws IOException;

    @Override
    public final LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        return getLeafCollector(valuesSource.longValues(ctx), sub);
    }

    protected final InternalAggregation[] buildAggregations(
        LongKeyedBucketOrds bucketOrds,
        LongToIntFunction roundingIndexFor,
        long[] owningBucketOrds
    ) throws IOException {
        return buildAggregationsForVariableBuckets(
            owningBucketOrds,
            bucketOrds,
            (bucketValue, docCount, subAggregationResults) -> new InternalAutoDateHistogram.Bucket(
                bucketValue,
                docCount,
                formatter,
                subAggregationResults
            ),
            (owningBucketOrd, buckets) -> {
                // the contract of the histogram aggregation is that shards must return
                // buckets ordered by key in ascending order
                CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

                // value source will be null for unmapped fields
                InternalAutoDateHistogram.BucketInfo emptyBucketInfo = new InternalAutoDateHistogram.BucketInfo(
                    roundingInfos,
                    roundingIndexFor.applyAsInt(owningBucketOrd),
                    buildEmptySubAggregations()
                );

                return new InternalAutoDateHistogram(name, buckets, targetBuckets, emptyBucketInfo, formatter, metadata(), 1);
            }
        );
    }

    @Override
    public final InternalAggregation buildEmptyAggregation() {
        InternalAutoDateHistogram.BucketInfo emptyBucketInfo = new InternalAutoDateHistogram.BucketInfo(
            roundingInfos,
            0,
            buildEmptySubAggregations()
        );
        return new InternalAutoDateHistogram(name, Collections.emptyList(), targetBuckets, emptyBucketInfo, formatter, metadata(), 1);
    }

    protected final Rounding.Prepared prepareRounding(int index) {
        return roundingPreparer.apply(roundingInfos[index].rounding);
    }

    protected final void merge(long[] mergeMap, long newNumBuckets) {
        LongUnaryOperator howToRewrite = b -> mergeMap[(int) b];
        rewriteBuckets(newNumBuckets, howToRewrite);
        if (deferringCollector != null) {
            deferringCollector.rewriteBuckets(howToRewrite);
        }
    }

    /**
     * Initially it uses the most fine grained rounding configuration possible
     * but as more data arrives it rebuckets the data until it "fits" in the
     * aggregation rounding. Similar to {@link FromMany} this checks both the
     * bucket count and range of the aggregation, but unlike
     * {@linkplain FromMany} it keeps an accurate count of the buckets and it
     * doesn't delay rebucketing.
     * <p>
     * Rebucketing is roughly {@code O(number_of_hits_collected_so_far)} but we
     * rebucket roughly {@code O(log number_of_hits_collected_so_far)} because
     * the "shape" of the roundings is <strong>roughly</strong>
     * logarithmically increasing.
     */
    private static class FromSingle extends AutoDateHistogramAggregator {
        private int roundingIdx;
        private Rounding.Prepared preparedRounding;
        /**
         * Map from value to bucket ordinals.
         * <p>
         * It is important that this is the exact subtype of
         * {@link LongKeyedBucketOrds} so that the JVM can make a monomorphic
         * call to {@link LongKeyedBucketOrds#add(long, long)} in the tight
         * inner loop of {@link LeafBucketCollector#collect(int, long)}. You'd
         * think that it wouldn't matter, but its seriously 7%-15% performance
         * difference for the aggregation. Yikes.
         */
        private LongKeyedBucketOrds.FromSingle bucketOrds;
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;

        FromSingle(
            String name,
            AggregatorFactories factories,
            int targetBuckets,
            RoundingInfo[] roundingInfos,
            ValuesSourceConfig valuesSourceConfig,
            AggregationContext context,
            Aggregator parent,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, factories, targetBuckets, roundingInfos, valuesSourceConfig, context, parent, metadata);

            preparedRounding = prepareRounding(0);
            bucketOrds = new LongKeyedBucketOrds.FromSingle(bigArrays());
        }

        @Override
        protected LeafBucketCollector getLeafCollector(SortedNumericDocValues values, LeafBucketCollector sub) throws IOException {
            return new LeafBucketCollectorBase(sub, values) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    assert owningBucketOrd == 0;
                    if (false == values.advanceExact(doc)) {
                        return;
                    }
                    int valuesCount = values.docValueCount();

                    long previousRounded = Long.MIN_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        long rounded = preparedRounding.round(value);
                        assert rounded >= previousRounded;
                        if (rounded == previousRounded) {
                            continue;
                        }
                        collectValue(doc, rounded);
                        previousRounded = rounded;
                    }
                }

                private void collectValue(int doc, long rounded) throws IOException {
                    long bucketOrd = bucketOrds.add(0, rounded);
                    if (bucketOrd < 0) { // already seen
                        bucketOrd = -1 - bucketOrd;
                        collectExistingBucket(sub, doc, bucketOrd);
                        return;
                    }
                    collectBucket(sub, doc, bucketOrd);
                    increaseRoundingIfNeeded(rounded);
                }

                private void increaseRoundingIfNeeded(long rounded) {
                    if (roundingIdx >= roundingInfos.length - 1) {
                        return;
                    }
                    min = Math.min(min, rounded);
                    max = Math.max(max, rounded);
                    if (bucketOrds.size() <= targetBuckets * roundingInfos[roundingIdx].getMaximumInnerInterval()
                        && max - min <= targetBuckets * roundingInfos[roundingIdx].getMaximumRoughEstimateDurationMillis()) {
                        return;
                    }
                    do {
                        LongKeyedBucketOrds oldOrds = bucketOrds;
                        boolean success = false;
                        try {
                            preparedRounding = prepareRounding(++roundingIdx);
                            long[] mergeMap = new long[Math.toIntExact(oldOrds.size())];
                            bucketOrds = new LongKeyedBucketOrds.FromSingle(bigArrays());
                            success = true; // now it is safe to close oldOrds after we finish
                            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = oldOrds.ordsEnum(0);
                            while (ordsEnum.next()) {
                                long oldKey = ordsEnum.value();
                                long newKey = preparedRounding.round(oldKey);
                                long newBucketOrd = bucketOrds.add(0, newKey);
                                mergeMap[(int) ordsEnum.ord()] = newBucketOrd >= 0 ? newBucketOrd : -1 - newBucketOrd;
                            }
                            merge(mergeMap, bucketOrds.size());
                        } finally {
                            if (success) {
                                oldOrds.close();
                            }
                        }
                    } while (roundingIdx < roundingInfos.length - 1
                        && (bucketOrds.size() > targetBuckets * roundingInfos[roundingIdx].getMaximumInnerInterval()
                            || max - min > targetBuckets * roundingInfos[roundingIdx].getMaximumRoughEstimateDurationMillis()));
                }
            };
        }

        @Override
        public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            return buildAggregations(bucketOrds, l -> roundingIdx, owningBucketOrds);
        }

        @Override
        public void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("surviving_buckets", bucketOrds.size());
        }

        @Override
        protected void doClose() {
            Releasables.close(bucketOrds);
        }
    }

    /**
     * Initially it uses the most fine grained rounding configuration possible but
     * as more data arrives it uses two heuristics to shift to coarser and coarser
     * rounding. The first heuristic is the number of buckets, specifically,
     * when there are more buckets than can "fit" in the current rounding it shifts
     * to the next rounding. Instead of redoing the rounding, it estimates the
     * number of buckets that will "survive" at the new rounding and uses
     * <strong>that</strong> as the initial value for the bucket count that it
     * increments in order to trigger another promotion to another coarser
     * rounding. This works fairly well at containing the number of buckets, but
     * the estimate of the number of buckets will be wrong if the buckets are
     * quite a spread out compared to the rounding.
     * <p>
     * The second heuristic it uses to trigger promotion to a coarser rounding is
     * the distance between the min and max bucket. When that distance is greater
     * than what the current rounding supports it promotes. This heuristic
     * isn't good at limiting the number of buckets but is great when the buckets
     * are spread out compared to the rounding. So it should complement the first
     * heuristic.
     * <p>
     * When promoting a rounding we keep the old buckets around because it is
     * expensive to call {@link BestBucketsDeferringCollector#rewriteBuckets}.
     * In particular it is {@code O(number_of_hits_collected_so_far)}. So if we
     * called it frequently we'd end up in {@code O(n^2)} territory. Bad news for
     * aggregations! Instead, we keep a "budget" of buckets that we're ok
     * "wasting". When we promote the rounding and our estimate of the number of
     * "dead" buckets that have data but have yet to be merged into the buckets
     * that are valid for the current rounding exceeds the budget then we rebucket
     * the entire aggregation and double the budget.
     * <p>
     * Once we're done collecting and we know exactly which buckets we'll be
     * returning we <strong>finally</strong> perform a "real", "perfect bucketing",
     * rounding all of the keys for {@code owningBucketOrd} that we're going to
     * collect and picking the rounding based on a real, accurate count and the
     * min and max.
     */
    private static class FromMany extends AutoDateHistogramAggregator {
        /**
         * An array of prepared roundings in the same order as
         * {@link #roundingInfos}. The 0th entry is prepared initially,
         * and other entries are null until first needed.
         */
        private final Rounding.Prepared[] preparedRoundings;
        /**
         * Map from value to bucket ordinals.
         * <p>
         * It is important that this is the exact subtype of
         * {@link LongKeyedBucketOrds} so that the JVM can make a monomorphic
         * call to {@link LongKeyedBucketOrds#add(long, long)} in the tight
         * inner loop of {@link LeafBucketCollector#collect(int, long)}.
         */
        private LongKeyedBucketOrds.FromMany bucketOrds;
        /**
         * The index of the rounding that each {@code owningBucketOrd} is
         * currently using.
         * <p>
         * During collection we use overestimates for how much buckets are save
         * by bumping to the next rounding index. So we end up bumping less
         * aggressively than a "perfect" algorithm. That is fine because we
         * correct the error when we merge the buckets together all the way
         * up in {@link InternalAutoDateHistogram#reduceBucket}. In particular,
         * on final reduce we bump the rounding until it we appropriately
         * cover the date range across all of the results returned by all of
         * the {@link AutoDateHistogramAggregator}s.
         */
        private ByteArray roundingIndices;
        /**
         * The minimum key per {@code owningBucketOrd}.
         */
        private LongArray mins;
        /**
         * The max key per {@code owningBucketOrd}.
         */
        private LongArray maxes;

        /**
         * An underestimate of the number of buckets that are "live" in the
         * current rounding for each {@code owningBucketOrdinal}.
         */
        private IntArray liveBucketCountUnderestimate;
        /**
         * An over estimate of the number of wasted buckets. When this gets
         * too high we {@link #rebucket} which sets it to 0.
         */
        private long wastedBucketsOverestimate = 0;
        /**
         * The next {@link #wastedBucketsOverestimate} that will trigger a
         * {@link #rebucket() rebucketing}.
         */
        private long nextRebucketAt = 1000; // TODO this could almost certainly start higher when asMultiBucketAggregator is gone
        /**
         * The number of times the aggregator had to {@link #rebucket()} the
         * results. We keep this just to report to the profiler.
         */
        private int rebucketCount = 0;

        FromMany(
            String name,
            AggregatorFactories factories,
            int targetBuckets,
            RoundingInfo[] roundingInfos,
            ValuesSourceConfig valuesSourceConfig,
            AggregationContext context,
            Aggregator parent,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, factories, targetBuckets, roundingInfos, valuesSourceConfig, context, parent, metadata);
            assert roundingInfos.length < 127 : "Rounding must fit in a signed byte";
            roundingIndices = bigArrays().newByteArray(1, true);
            mins = bigArrays().newLongArray(1, false);
            mins.set(0, Long.MAX_VALUE);
            maxes = bigArrays().newLongArray(1, false);
            maxes.set(0, Long.MIN_VALUE);
            preparedRoundings = new Rounding.Prepared[roundingInfos.length];
            // Prepare the first rounding because we know we'll need it.
            preparedRoundings[0] = prepareRounding(0);
            bucketOrds = new LongKeyedBucketOrds.FromMany(bigArrays());
            liveBucketCountUnderestimate = bigArrays().newIntArray(1, true);
        }

        @Override
        protected LeafBucketCollector getLeafCollector(SortedNumericDocValues values, LeafBucketCollector sub) throws IOException {
            return new LeafBucketCollectorBase(sub, values) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (false == values.advanceExact(doc)) {
                        return;
                    }
                    int valuesCount = values.docValueCount();

                    long previousRounded = Long.MIN_VALUE;
                    int roundingIdx = roundingIndexFor(owningBucketOrd);
                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        long rounded = preparedRoundings[roundingIdx].round(value);
                        assert rounded >= previousRounded;
                        if (rounded == previousRounded) {
                            continue;
                        }
                        roundingIdx = collectValue(owningBucketOrd, roundingIdx, doc, rounded);
                        previousRounded = rounded;
                    }
                }

                private int collectValue(long owningBucketOrd, int roundingIdx, int doc, long rounded) throws IOException {
                    long bucketOrd = bucketOrds.add(owningBucketOrd, rounded);
                    if (bucketOrd < 0) { // already seen
                        bucketOrd = -1 - bucketOrd;
                        collectExistingBucket(sub, doc, bucketOrd);
                        return roundingIdx;
                    }
                    collectBucket(sub, doc, bucketOrd);
                    liveBucketCountUnderestimate = bigArrays().grow(liveBucketCountUnderestimate, owningBucketOrd + 1);
                    int estimatedBucketCount = liveBucketCountUnderestimate.increment(owningBucketOrd, 1);
                    return increaseRoundingIfNeeded(owningBucketOrd, estimatedBucketCount, rounded, roundingIdx);
                }

                /**
                 * Increase the rounding of {@code owningBucketOrd} using
                 * estimated, bucket counts, {@link #rebucket() rebucketing} the all
                 * buckets if the estimated number of wasted buckets is too high.
                 */
                private int increaseRoundingIfNeeded(long owningBucketOrd, int oldEstimatedBucketCount, long newKey, int oldRounding) {
                    if (oldRounding >= roundingInfos.length - 1) {
                        return oldRounding;
                    }
                    if (mins.size() < owningBucketOrd + 1) {
                        long oldSize = mins.size();
                        mins = bigArrays().grow(mins, owningBucketOrd + 1);
                        mins.fill(oldSize, mins.size(), Long.MAX_VALUE);
                    }
                    if (maxes.size() < owningBucketOrd + 1) {
                        long oldSize = maxes.size();
                        maxes = bigArrays().grow(maxes, owningBucketOrd + 1);
                        maxes.fill(oldSize, maxes.size(), Long.MIN_VALUE);
                    }

                    long min = Math.min(mins.get(owningBucketOrd), newKey);
                    mins.set(owningBucketOrd, min);
                    long max = Math.max(maxes.get(owningBucketOrd), newKey);
                    maxes.set(owningBucketOrd, max);
                    if (oldEstimatedBucketCount <= targetBuckets * roundingInfos[oldRounding].getMaximumInnerInterval()
                        && max - min <= targetBuckets * roundingInfos[oldRounding].getMaximumRoughEstimateDurationMillis()) {
                        return oldRounding;
                    }
                    long oldRoughDuration = roundingInfos[oldRounding].roughEstimateDurationMillis;
                    int newRounding = oldRounding;
                    int newEstimatedBucketCount;
                    do {
                        newRounding++;
                        double ratio = (double) oldRoughDuration / (double) roundingInfos[newRounding].getRoughEstimateDurationMillis();
                        newEstimatedBucketCount = (int) Math.ceil(oldEstimatedBucketCount * ratio);
                    } while (newRounding < roundingInfos.length - 1
                        && (newEstimatedBucketCount > targetBuckets * roundingInfos[newRounding].getMaximumInnerInterval()
                            || max - min > targetBuckets * roundingInfos[newRounding].getMaximumRoughEstimateDurationMillis()));
                    setRounding(owningBucketOrd, newRounding);
                    mins.set(owningBucketOrd, preparedRoundings[newRounding].round(mins.get(owningBucketOrd)));
                    maxes.set(owningBucketOrd, preparedRoundings[newRounding].round(maxes.get(owningBucketOrd)));
                    wastedBucketsOverestimate += oldEstimatedBucketCount - newEstimatedBucketCount;
                    if (wastedBucketsOverestimate > nextRebucketAt) {
                        rebucket();
                        // Bump the threshold for the next rebucketing
                        wastedBucketsOverestimate = 0;
                        nextRebucketAt *= 2;
                    } else {
                        liveBucketCountUnderestimate.set(owningBucketOrd, newEstimatedBucketCount);
                    }
                    return newRounding;
                }
            };
        }

        private void rebucket() {
            rebucketCount++;
            LongKeyedBucketOrds oldOrds = bucketOrds;
            boolean success = false;
            try {
                long[] mergeMap = new long[Math.toIntExact(oldOrds.size())];
                bucketOrds = new LongKeyedBucketOrds.FromMany(bigArrays());
                success = true;
                for (long owningBucketOrd = 0; owningBucketOrd <= oldOrds.maxOwningBucketOrd(); owningBucketOrd++) {
                    LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = oldOrds.ordsEnum(owningBucketOrd);
                    Rounding.Prepared preparedRounding = preparedRoundings[roundingIndexFor(owningBucketOrd)];
                    while (ordsEnum.next()) {
                        long oldKey = ordsEnum.value();
                        long newKey = preparedRounding.round(oldKey);
                        long newBucketOrd = bucketOrds.add(owningBucketOrd, newKey);
                        mergeMap[(int) ordsEnum.ord()] = newBucketOrd >= 0 ? newBucketOrd : -1 - newBucketOrd;
                    }
                    liveBucketCountUnderestimate = bigArrays().grow(liveBucketCountUnderestimate, owningBucketOrd + 1);
                    liveBucketCountUnderestimate.set(owningBucketOrd, Math.toIntExact(bucketOrds.bucketsInOrd(owningBucketOrd)));
                }
                merge(mergeMap, bucketOrds.size());
            } finally {
                if (success) {
                    oldOrds.close();
                }
            }
        }

        @Override
        public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            /*
             * Rebucket before building the aggregation to build as small as result
             * as possible.
             *
             * TODO it'd be faster if we could apply the merging on the fly as we
             * replay the hits and build the buckets. How much faster is not clear,
             * but it does have the advantage of only touching the buckets that we
             * want to collect.
             */
            rebucket();
            return buildAggregations(bucketOrds, this::roundingIndexFor, owningBucketOrds);
        }

        @Override
        public void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("surviving_buckets", bucketOrds.size());
            add.accept("wasted_buckets_overestimate", wastedBucketsOverestimate);
            add.accept("next_rebucket_at", nextRebucketAt);
            add.accept("rebucket_count", rebucketCount);
        }

        private void setRounding(long owningBucketOrd, int newRounding) {
            roundingIndices = bigArrays().grow(roundingIndices, owningBucketOrd + 1);
            roundingIndices.set(owningBucketOrd, (byte) newRounding);
            if (preparedRoundings[newRounding] == null) {
                preparedRoundings[newRounding] = prepareRounding(newRounding);
            }
        }

        private int roundingIndexFor(long owningBucketOrd) {
            return owningBucketOrd < roundingIndices.size() ? roundingIndices.get(owningBucketOrd) : 0;
        }

        @Override
        public void doClose() {
            Releasables.close(bucketOrds, roundingIndices, mins, maxes, liveBucketCountUnderestimate);
        }
    }

}
