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
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AdaptingAggregator;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.range.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregatorSupplier;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An aggregator for date values. Every date is rounded down using a configured
 * {@link Rounding}.
 *
 * @see Rounding
 */
class DateHistogramAggregator extends BucketsAggregator implements SizedBucketAggregator {
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
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        Rounding.Prepared preparedRounding = valuesSourceConfig.roundingPreparer().apply(rounding);
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
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        if (hardBounds != null) {
            return null;
        }
        if (valuesSourceConfig.hasValues() == false) {
            return null;
        }
        long[] points = preparedRounding.fixedRoundingPoints();
        if (points == null) {
            return null;
        }
        // Range aggs use a double to aggregate and we don't want to lose precision.
        long max = points[points.length - 1];
        if ((double) max != max) {
            return null;
        }
        if ((double) points[0] != points[0]) {
            return null;
        }
        RangeAggregatorSupplier rangeSupplier = context.getQueryShardContext()
            .getValuesSourceRegistry()
            .getAggregator(RangeAggregationBuilder.REGISTRY_KEY, valuesSourceConfig);
        if (rangeSupplier == null) {
            return null;
        }
        RangeAggregator.Range[] ranges = new RangeAggregator.Range[points.length];
        for (int i = 0; i < points.length - 1; i++) {
            ranges[i] = new RangeAggregator.Range(null, (double) points[i], (double) points[i + 1]);
        }
        ranges[ranges.length - 1] = new RangeAggregator.Range(null, (double) points[points.length - 1], null);
        Aggregator delegate = rangeSupplier.build(
            name,
            factories,
            valuesSourceConfig,
            InternalDateRange.FACTORY,
            ranges,
            false,
            context,
            parent,
            cardinality,
            metadata
        );
        return new DateHistogramAggregator.FromDateRange(
            delegate,
            valuesSourceConfig.format(),
            rounding,
            order,
            minDocCount,
            extendedBounds,
            keyed
        );
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
        SearchContext aggregationContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {

        super(name, factories, aggregationContext, parent, CardinalityUpperBound.MANY, metadata);
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

        bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), cardinality);
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
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
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForVariableBuckets(owningBucketOrds, bucketOrds,
            (bucketValue, docCount, subAggregationResults) -> {
                return new InternalDateHistogram.Bucket(bucketValue, docCount, keyed, formatter, subAggregationResults);
            }, (owningBucketOrd, buckets) -> {
                // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
                CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

                // value source will be null for unmapped fields
                // Important: use `rounding` here, not `shardRounding`
                InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                        ? new InternalDateHistogram.EmptyBucketInfo(rounding.withoutOffset(), buildEmptySubAggregations(), extendedBounds)
                        : null;
                return new InternalDateHistogram(name, buckets, order, minDocCount, rounding.offset(), emptyBucketInfo, formatter,
                        keyed, metadata());
            });
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalDateHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                : null;
        return new InternalDateHistogram(name, Collections.emptyList(), order, minDocCount, rounding.offset(), emptyBucketInfo, formatter,
                keyed, metadata());
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

    private static class FromDateRange extends AdaptingAggregator {
        private final DocValueFormat format;
        private final Rounding rounding;
        private final BucketOrder order;
        private final long minDocCount;
        private final LongBounds extendedBounds;
        private final boolean keyed;

        FromDateRange(
            Aggregator delegate,
            DocValueFormat format,
            Rounding rounding,
            BucketOrder order,
            long minDocCount,
            LongBounds extendedBounds,
            boolean keyed
        ) {
            super(delegate);
            this.format = format;
            this.rounding = rounding;
            this.order = order;
            this.minDocCount = minDocCount;
            this.extendedBounds = extendedBounds;
            this.keyed = keyed;
        }

        @Override
        protected InternalAggregation adapt(InternalAggregation delegateResult) {
            InternalDateRange range = (InternalDateRange) delegateResult;
            List<InternalDateHistogram.Bucket> buckets = new ArrayList<>(range.getBuckets().size());
            for (InternalDateRange.Bucket rangeBucket : range.getBuckets()) {
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
            }
            CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

            // value source will be null for unmapped fields
            // Important: use `rounding` here, not `shardRounding`
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
    }
}
