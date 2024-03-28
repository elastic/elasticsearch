/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.exponentialhistogram.agg;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ExponentialHistogramAggregatorFactory extends ValuesSourceAggregatorFactory {

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            ExponentialHistogramAggregationBuilder.REGISTRY_KEY,
            ExponentialHistogramValuesSourceType.HISTOGRAM,
            ExponentialHistogramAggregator::new,
            true
        );
    }

    private final ExponentialHistogramAggregatorSupplier aggregatorSupplier;
    private final int maxBuckets;
    private final int maxScale;

    ExponentialHistogramAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        int maxBuckets,
        int maxScale,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        ExponentialHistogramAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.aggregatorSupplier = aggregatorSupplier;
        this.maxBuckets = maxBuckets;
        this.maxScale = maxScale;
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        if (cardinality != CardinalityUpperBound.ONE) {
            throw new IllegalArgumentException(
                "["
                    + ExponentialHistogramAggregationBuilder.NAME
                    + "] cannot be nested inside an aggregation that collects more than a single bucket."
            );
        }
        return aggregatorSupplier.build(name, factories, maxBuckets, maxScale, config, context, parent, metadata);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new ExponentialHistogramAggregator(
            name,
            factories,
            maxBuckets,
            maxScale,
            config,
            context,
            parent,
            metadata
        );
    }
}
