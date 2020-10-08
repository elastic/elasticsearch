package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AdaptingAggregator;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.range.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregatorSupplier;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Adapts a {@link DateHistogramAggregator} results into {@link InternalDateHistogram}s.
 */
public class DateHistogramAdaptedFromDateRangeAggregator extends AdaptingAggregator {
    static DateHistogramAdaptedFromDateRangeAggregator buildOptimizedOrNull(
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
        RangeAggregator delegate = rangeSupplier.build(
            name,
            factories,
            (ValuesSource.Numeric) valuesSourceConfig.getValuesSource(),
            valuesSourceConfig.format(),
            InternalDateRange.FACTORY,
            ranges,
            false,
            context,
            parent,
            cardinality,
            metadata
        );
        return new DateHistogramAdaptedFromDateRangeAggregator(
            delegate,
            valuesSourceConfig.format(),
            rounding,
            order,
            minDocCount,
            extendedBounds,
            keyed
        );
    }

    private final DocValueFormat format;
    private final Rounding rounding;
    private final BucketOrder order;
    private final long minDocCount;
    private final LongBounds extendedBounds;
    private final boolean keyed;

    public DateHistogramAdaptedFromDateRangeAggregator(
        RangeAggregator delegate,
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
}
