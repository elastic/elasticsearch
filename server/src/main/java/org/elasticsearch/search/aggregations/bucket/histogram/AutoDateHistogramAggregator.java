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
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.MergingBucketsDeferringCollector;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder.RoundingInfo;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * An aggregator for date values that attempts to return a specific number of
 * buckets, reconfiguring how it rounds dates to buckets on the fly as new
 * data arrives.
 * <p>
 * Initially it uses the most fine grained rounding configuration possible but
 * as more data arrives it uses two heuristics to shift to coarser and coarser
 * rounding. The first heuristic is the number of buckets, specifically, it
 * when there are more buckets than can "fit" in the current rounding it shifts
 * to the next rounding. Instead of redoing the rounding, it estimates the
 * number of buckets that will "survive" at the new rounding and uses
 * <strong>that</strong> as the initial value for the bucket count that it
 * increments in order to trigger another promotion to another coarser
 * rounding. This works fairly well at containing the number of buckets, but
 * it the estimate of the number of buckets will be wrong if the buckets are
 * quite a spread out compared to the rounding.
 * <p>
 * The second heuristic it uses to trigger promotion to a coarser rounding is
 * the distance between the min and max bucket. When that distance is greater
 * than what the current rounding supports it promotes. This is heuristic
 * isn't good at limiting the number of buckets but is great when the buckets
 * are spread out compared to the rounding. So it should complement the first
 * heuristic.
 * <p>
 * When promoting a rounding we keep the old buckets around because it is
 * expensive to call {@link MergingBucketsDeferringCollector#mergeBuckets}.
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
class AutoDateHistogramAggregator extends DeferableBucketAggregator {
    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat formatter;
    private final RoundingInfo[] roundingInfos;
    private final Function<Rounding, Rounding.Prepared> roundingPreparer;
    private final int targetBuckets;
    private final boolean collectsFromSingleBucket;
    /**
     * An array of prepared roundings in the same order as
     * {@link #roundingInfos}. The 0th entry is prepared initially,
     * and other entries are null until first needed.
     */
    private final Rounding.Prepared[] preparedRoundings;
    /**
     * Map from {@code owningBucketOrd, roundedDate} to {@code bucketOrdinal}.
     */
    private LongKeyedBucketOrds bucketOrds;
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
     * The min and max of each bucket's keys. min lives in indices of the form
     * {@code 2n} and max in {@code 2n + 1}.
     */
    private LongArray bounds;
    /**
     * A reference to the collector so we can
     * {@link MergingBucketsDeferringCollector#mergeBuckets(long[])}.
     */
    private MergingBucketsDeferringCollector deferringCollector;
    /**
     * An underestimate of the number of buckets that are "live" in the
     * current rounding for each {@code owningBucketOrdinal}. 
     */
    private IntArray liveBucketCountUnderestimate;
    /**
     * An over estimate of the number of wasted buckets. When this gets
     * too high we {@link #rebucket()} which sets it to 0.
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

    AutoDateHistogramAggregator(
        String name,
        AggregatorFactories factories,
        int numBuckets,
        RoundingInfo[] roundingInfos,
        Function<Rounding, Rounding.Prepared> roundingPreparer,
        @Nullable ValuesSource valuesSource,
        DocValueFormat formatter,
        SearchContext aggregationContext,
        Aggregator parent,
        boolean collectsFromSingleBucket,
        Map<String, Object> metadata
    )
        throws IOException {

        super(name, factories, aggregationContext, parent, metadata);
        this.targetBuckets = numBuckets;
        this.valuesSource = (ValuesSource.Numeric) valuesSource;
        this.formatter = formatter;
        this.roundingInfos = roundingInfos;
        this.roundingPreparer = roundingPreparer;
        this.collectsFromSingleBucket = collectsFromSingleBucket;
        assert roundingInfos.length < 127 : "Rounding must fit in a signed byte";
        roundingIndices = context.bigArrays().newByteArray(1, true);
        bounds = context.bigArrays().newLongArray(2, true);
        bounds.set(0, Long.MAX_VALUE);
        bounds.set(1, Long.MIN_VALUE);
        preparedRoundings = new Rounding.Prepared[roundingInfos.length];
        // Prepare the first rounding because we know we'll need it.
        preparedRoundings[0] = roundingPreparer.apply(roundingInfos[0].rounding);
        bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), collectsFromSingleBucket);
        liveBucketCountUnderestimate = context.bigArrays().newIntArray(1, true);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public DeferringBucketCollector getDeferringCollector() {
        deferringCollector = new MergingBucketsDeferringCollector(context, descendsFromGlobalAggregator(parent()));
        return deferringCollector;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        SortedNumericDocValues values = valuesSource.longValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    int valuesCount = values.docValueCount();

                    long previousRounded = Long.MIN_VALUE;
                    byte roundingIdx = roundingIndexFor(owningBucketOrd);
                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        long rounded = preparedRoundings[roundingIdx].round(value);
                        assert rounded >= previousRounded;
                        if (rounded == previousRounded) {
                            continue;
                        }
                        roundingIdx = collectValue(sub, owningBucketOrd, roundingIdx, doc, rounded);
                        previousRounded = rounded;
                    }
                }
            }

            private byte collectValue(LeafBucketCollector sub, long owningBucketOrd, byte roundingIdx, int doc, long rounded)
                    throws IOException {
                long bucketOrd = bucketOrds.add(owningBucketOrd, rounded);
                if (bucketOrd < 0) { // already seen
                    bucketOrd = -1 - bucketOrd;
                    collectExistingBucket(sub, doc, bucketOrd);
                    return roundingIdx;
                }
                collectBucket(sub, doc, bucketOrd);
                liveBucketCountUnderestimate = context.bigArrays().grow(liveBucketCountUnderestimate, owningBucketOrd + 1);
                int estimatedBucketCount = liveBucketCountUnderestimate.increment(owningBucketOrd, 1);
                return increaseRounding(owningBucketOrd, estimatedBucketCount, rounded, roundingIdx);
            }

            /**
             * Increase the rounding of {@code owningBucketOrd} using
             * estimated, bucket counts, {@link #rebucket() rebucketing} the all
             * buckets if the estimated number of wasted buckets is too high.
             */
            private byte increaseRounding(long owningBucketOrd, int oldEstimatedBucketCount, long newKey, byte oldRounding) {
                if (oldRounding >= roundingInfos.length - 1) {
                    return oldRounding;
                }
                if (bounds.size() < owningBucketOrd * 2 + 2) {
                    long oldSize = bounds.size();
                    bounds = context.bigArrays().grow(bounds, owningBucketOrd * 2 + 2);
                    for (long b = oldSize; b < bounds.size(); b++) {
                        bounds.set(b, (b & 1L) == 0L ? Long.MAX_VALUE : Long.MIN_VALUE);
                    }
                }
                long min = Math.min(bounds.get(owningBucketOrd * 2), newKey);
                bounds.set(owningBucketOrd * 2, min);
                long max = Math.max(bounds.get(owningBucketOrd * 2 + 1), newKey);
                bounds.set(owningBucketOrd * 2 + 1, max);
                if (oldEstimatedBucketCount <= targetBuckets * roundingInfos[oldRounding].getMaximumInnerInterval()
                        && max - min <= targetBuckets * roundingInfos[oldRounding].getMaximumRoughEstimateDurationMillis()) {
                    return oldRounding;
                }
                long oldRoughDuration = roundingInfos[oldRounding].roughEstimateDurationMillis;
                byte newRounding = oldRounding;
                int newEstimatedBucketCount;
                do {
                    newRounding++;
                    double ratio = (double) oldRoughDuration / (double) roundingInfos[newRounding].getRoughEstimateDurationMillis();
                    newEstimatedBucketCount = (int) Math.ceil(oldEstimatedBucketCount * ratio);
                } while (newRounding < roundingInfos.length - 1 && (
                    newEstimatedBucketCount > targetBuckets * roundingInfos[newRounding].getMaximumInnerInterval()
                        || max - min > targetBuckets * roundingInfos[newRounding].getMaximumRoughEstimateDurationMillis()));
                setRounding(owningBucketOrd, newRounding);
                bounds.set(owningBucketOrd * 2, preparedRoundings[newRounding].round(bounds.get(owningBucketOrd * 2)));
                bounds.set(owningBucketOrd * 2 + 1, preparedRoundings[newRounding].round(bounds.get(owningBucketOrd * 2 + 1)));
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
        try (LongKeyedBucketOrds oldOrds = bucketOrds) {
            long[] mergeMap = new long[Math.toIntExact(oldOrds.size())];
            bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), collectsFromSingleBucket);
            for (long owningBucketOrd = 0; owningBucketOrd <= oldOrds.maxOwningBucketOrd(); owningBucketOrd++) {
                LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = oldOrds.ordsEnum(owningBucketOrd);
                Rounding.Prepared preparedRounding = preparedRoundings[roundingIndexFor(owningBucketOrd)];
                while (ordsEnum.next()) {
                    long oldKey = ordsEnum.value();
                    long newKey = preparedRounding.round(oldKey);
                    long newBucketOrd = bucketOrds.add(owningBucketOrd, newKey);
                    mergeMap[(int) ordsEnum.ord()] = newBucketOrd >= 0 ? newBucketOrd : -1 - newBucketOrd;
                }
                liveBucketCountUnderestimate = context.bigArrays().grow(liveBucketCountUnderestimate, owningBucketOrd + 1);
                liveBucketCountUnderestimate.set(owningBucketOrd, Math.toIntExact(bucketOrds.bucketsInOrd(owningBucketOrd)));
            }
            mergeBuckets(mergeMap, bucketOrds.size());
            if (deferringCollector != null) {
                deferringCollector.mergeBuckets(mergeMap);
            }
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        correctRounding(owningBucketOrds);
        /*
         * Now that we have the perfect rounding rebucket everything to merge
         * all of the buckets together that we were too lazy to merge while
         * collecting.
         *
         * TODO it'd be faster if we could apply the merging on the fly as we
         * replay the hits and build the buckets. How much faster is
         * *interesting*. Performance tests with a couple of sub-`stats` aggs
         * show `date_histogram` to have about the same performance as
         * `auto_date_histogram` so there isn't really much to be gained here.
         * But if there is a non-delaying but selectivate aggregation "above"
         * this one then the performance gain could be substantial.
         */
        rebucket();
        return buildAggregationsForVariableBuckets(owningBucketOrds, bucketOrds,
                (bucketValue, docCount, subAggregationResults) ->
                    new InternalAutoDateHistogram.Bucket(bucketValue, docCount, formatter, subAggregationResults),
                (owningBucketOrd, buckets) -> {
                    // the contract of the histogram aggregation is that shards must return
                    // buckets ordered by key in ascending order
                    CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

                    // value source will be null for unmapped fields
                    InternalAutoDateHistogram.BucketInfo emptyBucketInfo = new InternalAutoDateHistogram.BucketInfo(roundingInfos,
                            roundingIndexFor(owningBucketOrd), buildEmptySubAggregations());

                    return new InternalAutoDateHistogram(name, buckets, targetBuckets, emptyBucketInfo, formatter, metadata(), 1);
                });
    }

    /**
     * Pick the correct rounding for the specifies {@code owningBucketOrds}.
     */
    private void correctRounding(long[] owningBucketOrds) {
        for (long owningBucketOrd : owningBucketOrds) {
            byte oldRounding = roundingIndexFor(owningBucketOrd);
            if (oldRounding >= roundingInfos.length - 1) {
                continue;
            }
            byte newRounding = (byte)(oldRounding - 1);
            long count;
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            do {
                newRounding++;
                try (LongHash perfect = new LongHash(liveBucketCountUnderestimate.get(owningBucketOrd), context.bigArrays())) {
                    LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrd);
                    Rounding.Prepared preparedRounding = preparedRoundings[roundingIndexFor(owningBucketOrd)];
                    while (ordsEnum.next()) {
                        long oldKey = ordsEnum.value();
                        long newKey = preparedRounding.round(oldKey);
                        min = Math.min(min, newKey);
                        max = Math.max(max, newKey);
                        bucketOrds.add(owningBucketOrd, newKey);
                    }
                    count = perfect.size();
                }
            } while (newRounding < roundingInfos.length - 1 && (
                    count > targetBuckets * roundingInfos[newRounding].getMaximumInnerInterval()
                        || max - min > targetBuckets * roundingInfos[newRounding].getMaximumRoughEstimateDurationMillis()));
            setRounding(owningBucketOrd, newRounding);
            wastedBucketsOverestimate += bucketOrds.bucketsInOrd(owningBucketOrd) - count;
        }
    }

    private void setRounding(long owningBucketOrd, byte newRounding) {
        roundingIndices = context.bigArrays().grow(roundingIndices, owningBucketOrd + 1);
        roundingIndices.set(owningBucketOrd, newRounding);
        if (preparedRoundings[newRounding] == null) {
            preparedRoundings[newRounding] = roundingPreparer.apply(roundingInfos[newRounding].rounding);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalAutoDateHistogram.BucketInfo emptyBucketInfo = new InternalAutoDateHistogram.BucketInfo(roundingInfos, 0,
                buildEmptySubAggregations());
        return new InternalAutoDateHistogram(name, Collections.emptyList(), targetBuckets, emptyBucketInfo, formatter, metadata(), 1);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("surviving_buckets", bucketOrds.size());
        add.accept("wasted_buckets_overestimate", wastedBucketsOverestimate);
        add.accept("next_rebucket_at", nextRebucketAt);
        add.accept("rebucket_count", rebucketCount);
    }

    private byte roundingIndexFor(long owningBucketOrd) {
        return owningBucketOrd < roundingIndices.size() ? roundingIndices.get(owningBucketOrd) : 0;
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds, roundingIndices, bounds, liveBucketCountUnderestimate);
    }
}
