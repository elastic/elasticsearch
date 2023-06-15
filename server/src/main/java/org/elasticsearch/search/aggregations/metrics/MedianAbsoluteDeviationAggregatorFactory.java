/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.Map;

public class MedianAbsoluteDeviationAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final MedianAbsoluteDeviationAggregatorSupplier aggregatorSupplier;
    private final double compression;
    private final TDigestExecutionHint executionHint;

    MedianAbsoluteDeviationAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        double compression,
        TDigestExecutionHint executionHint,
        MedianAbsoluteDeviationAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.aggregatorSupplier = aggregatorSupplier;
        this.compression = compression;
        this.executionHint = (executionHint.isEmpty() && context.getClusterSettings() != null)
            ? context.getClusterSettings().get(TDigestState.EXECUTION_HINT)
            : executionHint;
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            MedianAbsoluteDeviationAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.NUMERIC,
            MedianAbsoluteDeviationAggregator::new,
            true
        );
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalMedianAbsoluteDeviation empty = InternalMedianAbsoluteDeviation.empty(
            name,
            metadata,
            config.format(),
            compression,
            executionHint
        );
        return new NonCollectingSingleMetricAggregator(name, context, parent, empty, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return aggregatorSupplier.build(name, config, config.format(), context, parent, metadata, compression, executionHint);
    }
}
