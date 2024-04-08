/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.aggregations.support.TimeSeriesValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Constructs the per-shard aggregator instance for histogram aggregation.  Selects the numeric or range field implementation based on the
 * field type.
 */
public final class HistogramAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final HistogramAggregatorSupplier aggregatorSupplier;
    private final double interval, offset;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final DoubleBounds extendedBounds;
    private final DoubleBounds hardBounds;

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(HistogramAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.RANGE, RangeHistogramAggregator::new, true);

        builder.register(
            HistogramAggregationBuilder.REGISTRY_KEY,
            List.of(
                CoreValuesSourceType.NUMERIC,
                CoreValuesSourceType.DATE,
                CoreValuesSourceType.BOOLEAN,
                TimeSeriesValuesSourceType.COUNTER
            ),
            NumericHistogramAggregator::new,
            true
        );
    }

    public HistogramAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        double interval,
        double offset,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        DoubleBounds extendedBounds,
        DoubleBounds hardBounds,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        HistogramAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.aggregatorSupplier = aggregatorSupplier;
        this.interval = interval;
        this.offset = offset;
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.hardBounds = hardBounds;
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
            interval,
            offset,
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
        return new NumericHistogramAggregator(
            name,
            factories,
            interval,
            offset,
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
