/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder.RoundingInfo;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public final class AutoDateHistogramAggregatorFactory extends ValuesSourceAggregatorFactory {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(AutoDateHistogramAggregator.class);

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            AutoDateHistogramAggregationBuilder.REGISTRY_KEY,
            Arrays.asList(CoreValuesSourceType.DATE, CoreValuesSourceType.NUMERIC),
            AutoDateHistogramAggregator::build,
            true
        );

        builder.register(
            AutoDateHistogramAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.BOOLEAN,
            (
                String name,
                AggregatorFactories factories,
                int targetBuckets,
                RoundingInfo[] roundingInfos,
                ValuesSourceConfig valuesSourceConfig,
                AggregationContext context,
                Aggregator parent,
                CardinalityUpperBound cardinality,
                Map<String, Object> metadata) -> {

                DEPRECATION_LOGGER.deprecate(
                    DeprecationCategory.AGGREGATIONS,
                    "auto-date-histogram-boolean",
                    "Running AutoIntervalDateHistogram aggregations on [boolean] fields is deprecated"
                );
                return AutoDateHistogramAggregator.build(
                    name,
                    factories,
                    targetBuckets,
                    roundingInfos,
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

    private final AutoDateHistogramAggregatorSupplier aggregatorSupplier;
    private final int numBuckets;
    private RoundingInfo[] roundingInfos;

    public AutoDateHistogramAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        int numBuckets,
        RoundingInfo[] roundingInfos,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        AutoDateHistogramAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);

        this.aggregatorSupplier = aggregatorSupplier;
        this.numBuckets = numBuckets;
        this.roundingInfos = roundingInfos;
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return aggregatorSupplier.build(name, factories, numBuckets, roundingInfos, config, context, parent, cardinality, metadata);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return AutoDateHistogramAggregator.build(
            name,
            factories,
            numBuckets,
            roundingInfos,
            config,
            context,
            parent,
            CardinalityUpperBound.NONE,
            metadata
        );
    }
}
