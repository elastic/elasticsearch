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
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramValuesSourceType;

import java.io.IOException;
import java.util.Map;

public class ExponentialHistogramPercentilesAggregatorFactory extends ValuesSourceAggregatorFactory {

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            ExponentialHistogramPercentilesAggregationBuilder.REGISTRY_KEY,
            ExponentialHistogramValuesSourceType.HISTOGRAM,
            ExponentialHistogramPercentilesAggregator::new,
            true
        );
    }

    private final ExponentialHistogramPercentilesAggregatorSupplier aggregatorSupplier;
    private final int maxBuckets;
    private final int maxScale;
    private final double[] percentiles;

    ExponentialHistogramPercentilesAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        int maxBuckets,
        int maxScale,
        double[] percentiles,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        ExponentialHistogramPercentilesAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.aggregatorSupplier = aggregatorSupplier;
        this.maxBuckets = maxBuckets;
        this.maxScale = maxScale;
        this.percentiles = percentiles;
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return aggregatorSupplier.build(
            name, factories, maxBuckets, maxScale, percentiles, config, context, parent, metadata
        );
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new ExponentialHistogramPercentilesAggregator(
            name,
            factories,
            maxBuckets,
            maxScale,
            percentiles,
            config,
            context,
            parent,
            metadata
        );
    }
}
