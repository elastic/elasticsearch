/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class DateHistogramAggregatorFactory extends ValuesSourceAggregatorFactory {
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(DateHistogramAggregatorFactory.class);

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            DateHistogramAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.NUMERIC),
            DateHistogramAggregator::build,
            true
        );

        builder.register(DateHistogramAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.RANGE, DateRangeHistogramAggregator::new, true);

        builder.register(
            DateHistogramAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.BOOLEAN,
            (
                name,
                factories,
                rounding,
                order,
                keyed,
                minDocCount,
                extendedBounds,
                hardBounds,
                valuesSourceConfig,
                context,
                parent,
                cardinality,
                metadata) -> {
                DEPRECATION_LOGGER.deprecate(
                    DeprecationCategory.AGGREGATIONS,
                    "date-histogram-boolean",
                    "Running DateHistogram aggregations on [boolean] fields is deprecated"
                );
                return DateHistogramAggregator.build(
                    name,
                    factories,
                    rounding,
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
            },
            true
        );
    }

    private final DateHistogramAggregationSupplier aggregatorSupplier;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final LongBounds extendedBounds;
    private final LongBounds hardBounds;
    private final Rounding rounding;

    public DateHistogramAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
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
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.hardBounds = hardBounds;
        this.rounding = rounding;
    }

    public long minDocCount() {
        return minDocCount;
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return aggregatorSupplier.build(
            name,
            factories,
            rounding,
            order,
            keyed,
            minDocCount,
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
