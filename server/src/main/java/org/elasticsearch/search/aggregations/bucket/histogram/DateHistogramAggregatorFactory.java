/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class DateHistogramAggregatorFactory extends ValuesSourceAggregatorFactory {

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            DateHistogramAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.NUMERIC),
            DateHistogramAggregator::build,
            true
        );

        builder.register(DateHistogramAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.RANGE, DateRangeHistogramAggregator::new, true);
    }

    private final DateHistogramAggregationSupplier aggregatorSupplier;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final boolean downsampledResultsOffset;
    private final LongBounds extendedBounds;
    private final LongBounds hardBounds;
    private final Rounding rounding;

    public DateHistogramAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        boolean downsampledResultsOffset,
        Rounding rounding,
        LongBounds extendedBounds,
        LongBounds hardBounds,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        DateHistogramAggregationSupplier aggregationSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.aggregatorSupplier = aggregationSupplier;
        this.order = order;
        this.keyed = keyed;
        this.downsampledResultsOffset = downsampledResultsOffset;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.hardBounds = hardBounds;
        this.rounding = rounding;
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        // If min_doc_count is provided, we do not support them being larger than 1
        // This is because we cannot be sure about their relative scale when sampled
        if (getSamplingContext().map(SamplingContext::isSampled).orElse(false)) {
            if (minDocCount > 1) {
                throw new ElasticsearchStatusException(
                    "aggregation [{}] is within a sampling context; " + "min_doc_count, provided [{}], cannot be greater than 1",
                    RestStatus.BAD_REQUEST,
                    name(),
                    minDocCount
                );
            }
        }
        return aggregatorSupplier.build(
            name,
            factories,
            rounding,
            order,
            keyed,
            minDocCount,
            downsampledResultsOffset,
            extendedBounds,
            hardBounds,
            config,
            context,
            parent,
            cardinality,
            metadata
        );
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new DateHistogramAggregator(
            name,
            factories,
            rounding,
            null,
            order,
            keyed,
            minDocCount,
            downsampledResultsOffset,
            extendedBounds,
            hardBounds,
            config,
            context,
            parent,
            CardinalityUpperBound.NONE,
            metadata
        );
    }
}
