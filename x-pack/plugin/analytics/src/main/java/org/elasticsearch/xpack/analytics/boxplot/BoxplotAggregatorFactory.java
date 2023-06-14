/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.metrics.NonCollectingMultiMetricAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class BoxplotAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final double compression;
    private final String executionHint;
    private final BoxplotAggregatorSupplier aggregatorSupplier;

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            BoxplotAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.NUMERIC, AnalyticsValuesSourceType.HISTOGRAM),
            BoxplotAggregator::new,
            true
        );
    }

    BoxplotAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        double compression,
        String executionHint,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        BoxplotAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.compression = compression;
        this.executionHint = executionHint;
        this.aggregatorSupplier = aggregatorSupplier;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalBoxplot empty = InternalBoxplot.empty(name, compression, executionHint, config.format(), metadata);
        final Predicate<String> hasMetric = InternalBoxplot.Metrics::hasMetric;
        return new NonCollectingMultiMetricAggregator(name, context, parent, empty, hasMetric, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return aggregatorSupplier.build(name, config, config.format(), compression, executionHint, context, parent, metadata);
    }
}
