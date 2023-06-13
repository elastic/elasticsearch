/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Rounding.DateTimeUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AdaptingAggregator;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.range.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregatorSupplier;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Aggregator for {@code date_histogram} that rounds values using
 * {@link Rounding}. See {@link FromDateRange} which also aggregates for
 * {@code date_histogram} but does so by running a {@code range} aggregation
 * over the date and transforming the results. In general
 * {@link FromDateRange} is faster than {@link DateHistogramAggregator}
 * but {@linkplain DateHistogramAggregator} works when we can't precalculate
 * all of the {@link Rounding.Prepared#fixedRoundingPoints() fixed rounding points}.
 */
class DateHistogramAggregator extends BucketsAggregator implements SizedBucketAggregator {
    private static final Logger logger = LogManager.getLogger(DateHistogramAggregator.class);

    /**
     * Build an {@link Aggregator} for a {@code date_histogram} aggregation.
     * If we can determine the bucket boundaries from
     * {@link Rounding.Prepared#fixedRoundingPoints()} we use
     * {@link RangeAggregator} to do the actual collecting, otherwise we use
     * an specialized {@link DateHistogramAggregator Aggregator} specifically
     * for the {@code date_histogram}s. We prefer to delegate to the
     * {@linkplain RangeAggregator} because it can sometimes be further
     * optimized into a {@link FiltersAggregator}. Even when it can't be
     * optimized, it is going to be marginally faster and consume less memory
     * than the {@linkplain DateHistogramAggregator} because it doesn't need
     * to the round points and because it can pass precise cardinality
     * estimates to its child aggregations.
     */
    public static Aggregator build(
        String name,
        AggregatorFactories factories,
        Rounding rounding,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        @Nullable LongBounds extendedBounds,
        @Nullable LongBounds hardBounds,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        Rounding.Prepared preparedRounding = valuesSourceConfig.roundingPreparer(context).apply(rounding);
        Aggregator asRange = adaptIntoRangeOrNull(
            name,
            factories,
            rounding,
            preparedRounding,
            order,
            keyed,
            minDocCount,
            extendedBounds,
            hardBounds,
            valuesSourceConfig,
            context,
            parent,
            cardinality,
            metadata
        );
        if (asRange != null) {
            return asRange;
        }
        return new DateHistogramAggregator(
            name,
            factories,
            rounding,
            preparedRounding,
            order,
            keyed,
            minDocCount,
            extendedBounds,
            hardBounds,
            valuesSourceConfig,
            context,
            parent,
            cardinality,
            metadata
        );
    }

    private static FromDateRange adaptIntoRangeOrNull(
        String name,
        AggregatorFactories factories,
        Rounding rounding,
        Rounding.Prepared preparedRounding,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        @Nullable LongBounds extendedBounds,
        @Nullable LongBounds hardBounds,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        long[] fixedRoundingPoints = preparedRounding.fixedRoundingPoints();
        if (fixedRoundingPoints == null) {
            logger.trace("couldn't adapt [{}], no fixed rounding points in [{}]", name, preparedRounding);
            return null;
        }
        // Range aggs use a double to aggregate and we don't want to lose precision.
        long min = fixedRoundingPoints[0];
        long max = fixedRoundingPoints[fixedRoundingPoints.length - 1];
        if (min < -RangeAggregator.MAX_ACCURATE_BOUND || min > RangeAggregator.MAX_ACCURATE_BOUND) {
            logger.trace("couldn't adapt [{}], min outside accurate bounds", name);
            return null;
        }
        if (max < -RangeAggregator.MAX_ACCURATE_BOUND || max > RangeAggregator.MAX_ACCURATE_BOUND) {
            logger.trace("couldn't adapt [{}], max outside accurate bounds", name);
            return null;
        }
        RangeAggregatorSupplier rangeSupplier = context.getValuesSourceRegistry()
            .getAggregator(RangeAggregationBuilder.REGISTRY_KEY, valuesSourceConfig);
        if (rangeSupplier == null) {
            logger.trace(
                "couldn't adapt [{}], no range for [{}/{}]",
                name,
                valuesSourceConfig.fieldContext().field(),
                valuesSourceConfig.fieldType()
            );
            return null;
        }
        RangeAggregator.Range[] ranges = ranges(hardBounds, fixedRoundingPoints);
        return new DateHistogramAggregator.FromDateRange(
            parent,
            factories,
            subAggregators -> rangeSupplier.build(
                name,
                subAggregators,
                valuesSourceConfig,
                InternalDateRange.FACTORY,
                ranges,
                false,
                context,
                parent,
                cardinality,
                metadata
            ),
            valuesSourceConfig.format(),
            rounding,
            preparedRounding,
            order,
            minDocCount,
            extendedBounds,
            keyed,
            fixedRoundingPoints
        );
    }

    private static RangeAggregator.Range[] ranges(LongBounds hardBounds, long[] fixedRoundingPoints) {
        if (hardBounds == null) {
            RangeAggregator.Range[] ranges = new RangeAggregator.Range[fixedRoundingPoints.length];
            for (int i = 0; i < fixedRoundingPoints.length - 1; i++) {
                ranges[i] = new RangeAggregator.Range(null, (double) fixedRoundingPoints[i], (double) fixedRoundingPoints[i + 1]);
            }
            ranges[ranges.length - 1] = new RangeAggregator.Range(null, (double) fixedRoundingPoints[fixedRoundingPoints.length - 1], null);
            return ranges;
        }
        List<RangeAggregator.Range> ranges = new ArrayList<>(fixedRoundingPoints.length);
        for (int i = 0; i < fixedRoundingPoints.length - 1; i++) {
            if (hardBounds.contain(fixedRoundingPoints[i])) {
                ranges.add(new RangeAggregator.Range(null, (double) fixedRoundingPoints[i], (double) fixedRoundingPoints[i + 1]));
            }
        }
        if (hardBounds.contain(fixedRoundingPoints[fixedRoundingPoints.length - 1])) {
            ranges.add(new RangeAggregator.Range(null, (double) fixedRoundingPoints[fixedRoundingPoints.length - 1], null));
        }
        return ranges.toArray(RangeAggregator.Range[]::new);
    }

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat formatter;
    private final Rounding rounding;
    /**
     * The rounding prepared for rewriting the data in the shard.
     */
    private final Rounding.Prepared preparedRounding;
    private final BucketOrder order;
    private final boolean keyed;

    private final long minDocCount;
    private final LongBounds extendedBounds;
    private final LongBounds hardBounds;

    private final LongKeyedBucketOrds bucketOrds;

    DateHistogramAggregator(
        String name,
        AggregatorFactories factories,
        Rounding rounding,
        Rounding.Prepared preparedRounding,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        @Nullable LongBounds extendedBounds,
        @Nullable LongBounds hardBounds,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, CardinalityUpperBound.MANY, metadata);
        this.rounding = rounding;
        this.preparedRounding = preparedRounding;
        this.order = order;
        order.validate(this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.hardBounds = hardBounds;
        // TODO: Stop using null here
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
        this.formatter = valuesSourceConfig.format();

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
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        SortedNumericDocValues values = valuesSource.longValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    int valuesCount = values.docValueCount();

                    long previousRounded = Long.MIN_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        long rounded = preparedRounding.round(value);
                        assert rounded >= previousRounded;
                        if (rounded == previousRounded) {
                            continue;
                        }
                        if (hardBounds == null || hardBounds.contain(rounded)) {
                            long bucketOrd = bucketOrds.add(owningBucketOrd, rounded);
                            if (bucketOrd < 0) { // already seen
                                bucketOrd = -1 - bucketOrd;
                                collectExistingBucket(sub, doc, bucketOrd);
                            } else {
                                collectBucket(sub, doc, bucketOrd);
                            }
                        }
                        previousRounded = rounded;
                    }
                }
            }
        };
    }

    @Override
    public void merge(Map<Long, List<AggregationAndBucket>> toMerge, BigArrays bigArrays) {
        // It is an article of faith that all the aggregations have the same list of subAggregators in the same order
        List<Map<Long, List<AggregationAndBucket>>> nextLayer = new ArrayList<>(subAggregators.length);
        for (int i = 0; i < subAggregators.length; i++) {
            nextLayer.add(new HashMap<>());
        }
        for (Map.Entry<Long, List<AggregationAndBucket>> mergeRow : toMerge.entrySet()) {
            List<Map<Long, List<AggregationAndBucket>>> nextLayerBuckets = mergeBucket(mergeRow.getValue(), mergeRow.getKey());
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

    public List<Map<Long, List<AggregationAndBucket>>> mergeBucket(List<AggregationAndBucket> others, long thisBucket) {
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
                    ((DateHistogramAggregator) other.aggregator()).bucketOrds.keyOrderedIterator(other.bucketOrdinal()),
                    other.aggregator()
                )
            );
        }

        // Is it possible for the queue to be empty? Maybe this should check for >1, since we always add ourselves?
        if (pq.size() > 0) {
            // Buckets matching the current key
            List<AggregationAndBucket> currentBuckets = new ArrayList<>();
            long key = pq.top().current();
            do {
                final IteratorAndAggregator top = pq.top();

                if (top.current() != key) {
                    // the key changes, reduce what we already buffered and reset the buffer for current buckets

                    // Deal with key doesn't exist for this aggregation
                    long mergeLeaderBucketOrd = bucketOrds.add(thisBucket, key);
                    if (mergeLeaderBucketOrd < 0) { // already seen
                        mergeLeaderBucketOrd = -1 - mergeLeaderBucketOrd;
                        // we don't need to update the doc count because we already have it
                    } else {
                        grow(mergeLeaderBucketOrd + 1);
                        // TODO: Set the doc count to zero here
                    }

                    for (int i = 0; i < subAggregators.length; i++) {
                        Map<Long, List<AggregationAndBucket>> zwomp = nextLayer.get(i);
                        assert zwomp.containsKey(mergeLeaderBucketOrd) == false;
                        zwomp.put(mergeLeaderBucketOrd, new ArrayList<>(currentBuckets.size()));
                    }

                    for (AggregationAndBucket other : currentBuckets) {
                        if (other.aggregator() == this) {
                            // We already handled our case above
                            continue;
                        }

                        // NOCOMMIT: the cast is pretty kludgy here; probably just want a method on Aggregator to do this.
                        DateHistogramAggregator otherAggregator = (DateHistogramAggregator) other.aggregator();
                        // Compute the key for the other agg
                        long otherBucketOrd = otherAggregator.bucketOrds.find(other.bucketOrdinal(), key);
                        // TODO: Update docCounts

                        // For each sub-aggregation, build a list of the aggregators-bucket pairs for this key and owning ordinal
                        for (int i = 0; i < subAggregators.length; i++) {
                            nextLayer.get(i).get(mergeLeaderBucketOrd)
                                .add(new AggregationAndBucket(otherBucketOrd, other.aggregator().subAggregators()[i]));
                        }
                    }

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
        }
        return nextLayer;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForVariableBuckets(owningBucketOrds, bucketOrds, (bucketValue, docCount, subAggregationResults) -> {
            return new InternalDateHistogram.Bucket(bucketValue, docCount, keyed, formatter, subAggregationResults);
        }, (owningBucketOrd, buckets) -> {
            // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
            CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

            InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalDateHistogram.EmptyBucketInfo(rounding.withoutOffset(), buildEmptySubAggregations(), extendedBounds)
                : null;
            return new InternalDateHistogram(
                name,
                buckets,
                order,
                minDocCount,
                rounding.offset(),
                emptyBucketInfo,
                formatter,
                keyed,
                metadata()
            );
        });
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
            ? new InternalDateHistogram.EmptyBucketInfo(rounding.withoutOffset(), buildEmptySubAggregations(), extendedBounds)
            : null;
        return new InternalDateHistogram(
            name,
            Collections.emptyList(),
            order,
            minDocCount,
            rounding.offset(),
            emptyBucketInfo,
            formatter,
            keyed,
            metadata()
        );
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        add.accept("total_buckets", bucketOrds.size());
    }

    /**
     * Returns the size of the bucket in specified units.
     *
     * If unitSize is null, returns 1.0
     */
    @Override
    public double bucketSize(long bucket, Rounding.DateTimeUnit unitSize) {
        if (unitSize != null) {
            return preparedRounding.roundingSize(bucketOrds.get(bucket), unitSize);
        } else {
            return 1.0;
        }
    }

    @Override
    public double bucketSize(Rounding.DateTimeUnit unitSize) {
        if (unitSize != null) {
            return preparedRounding.roundingSize(unitSize);
        } else {
            return 1.0;
        }
    }

    static class FromDateRange extends AdaptingAggregator implements SizedBucketAggregator {
        private final DocValueFormat format;
        private final Rounding rounding;
        private final Rounding.Prepared preparedRounding;
        private final BucketOrder order;
        private final long minDocCount;
        private final LongBounds extendedBounds;
        private final boolean keyed;
        private final long[] fixedRoundingPoints;

        FromDateRange(
            Aggregator parent,
            AggregatorFactories subAggregators,
            CheckedFunction<AggregatorFactories, Aggregator, IOException> delegate,
            DocValueFormat format,
            Rounding rounding,
            Rounding.Prepared preparedRounding,
            BucketOrder order,
            long minDocCount,
            LongBounds extendedBounds,
            boolean keyed,
            long[] fixedRoundingPoints
        ) throws IOException {
            super(parent, subAggregators, delegate);
            this.format = format;
            this.rounding = rounding;
            this.preparedRounding = preparedRounding;
            this.order = order;
            order.validate(this);
            this.minDocCount = minDocCount;
            this.extendedBounds = extendedBounds;
            this.keyed = keyed;
            this.fixedRoundingPoints = fixedRoundingPoints;
        }

        @Override
        protected InternalAggregation adapt(InternalAggregation delegateResult) {
            InternalDateRange range = (InternalDateRange) delegateResult;
            List<InternalDateHistogram.Bucket> buckets = new ArrayList<>(range.getBuckets().size());
            // This code below was converted from a regular for loop to a stream forEach to avoid
            // JDK-8285835. It needs to stay in this form until we upgrade our JDK distribution to
            // pick up a fix for the compiler crash.
            range.getBuckets().stream().forEach(rangeBucket -> {
                if (rangeBucket.getDocCount() > 0) {
                    buckets.add(
                        new InternalDateHistogram.Bucket(
                            rangeBucket.getFrom().toInstant().toEpochMilli(),
                            rangeBucket.getDocCount(),
                            keyed,
                            format,
                            rangeBucket.getAggregations()
                        )
                    );
                }
            });

            CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

            InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalDateHistogram.EmptyBucketInfo(rounding.withoutOffset(), buildEmptySubAggregations(), extendedBounds)
                : null;
            return new InternalDateHistogram(
                range.getName(),
                buckets,
                order,
                minDocCount,
                rounding.offset(),
                emptyBucketInfo,
                format,
                keyed,
                range.getMetadata()
            );
        }

        public final InternalAggregations buildEmptySubAggregations() {
            List<InternalAggregation> aggs = new ArrayList<>();
            for (Aggregator aggregator : subAggregators()) {
                aggs.add(aggregator.buildEmptyAggregation());
            }
            return InternalAggregations.from(aggs);
        }

        @Override
        public double bucketSize(long bucket, DateTimeUnit unitSize) {
            if (unitSize != null) {
                long startPoint = bucket < fixedRoundingPoints.length ? fixedRoundingPoints[(int) bucket] : Long.MIN_VALUE;
                return preparedRounding.roundingSize(startPoint, unitSize);
            } else {
                return 1.0;
            }
        }

        @Override
        public double bucketSize(DateTimeUnit unitSize) {
            if (unitSize != null) {
                return preparedRounding.roundingSize(unitSize);
            } else {
                return 1.0;
            }
        }
    }
}
