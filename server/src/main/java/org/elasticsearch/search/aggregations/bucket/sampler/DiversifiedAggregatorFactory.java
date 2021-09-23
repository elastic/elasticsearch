/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregator.ExecutionMode;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DiversifiedAggregatorFactory extends ValuesSourceAggregatorFactory {

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            DiversifiedAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN),
            (
                String name,
                int shardSize,
                AggregatorFactories factories,
                AggregationContext context,
                Aggregator parent,
                Map<String, Object> metadata,
                ValuesSourceConfig valuesSourceConfig,
                int maxDocsPerValue,
                String executionHint) -> new DiversifiedNumericSamplerAggregator(
                    name,
                    shardSize,
                    factories,
                    context,
                    parent,
                    metadata,
                    valuesSourceConfig,
                    maxDocsPerValue
                ),
            true
        );

        builder.register(
            DiversifiedAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.KEYWORD,
            (
                String name,
                int shardSize,
                AggregatorFactories factories,
                AggregationContext context,
                Aggregator parent,
                Map<String, Object> metadata,
                ValuesSourceConfig valuesSourceConfig,
                int maxDocsPerValue,
                String executionHint) -> {
                ExecutionMode execution = null;
                if (executionHint != null) {
                    execution = ExecutionMode.fromString(executionHint);
                }

                // In some cases using ordinals is just not supported: override it
                if (execution == null) {
                    execution = ExecutionMode.GLOBAL_ORDINALS;
                }
                if ((execution.needsGlobalOrdinals()) && (valuesSourceConfig.hasOrdinals() == false)) {
                    execution = ExecutionMode.MAP;
                }
                return execution.create(name, factories, shardSize, maxDocsPerValue, valuesSourceConfig, context, parent, metadata);
            },
            true
        );
    }

    private final DiversifiedAggregatorSupplier aggregatorSupplier;
    private final int shardSize;
    private final int maxDocsPerValue;
    private final String executionHint;

    DiversifiedAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        int shardSize,
        int maxDocsPerValue,
        String executionHint,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        DiversifiedAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.shardSize = shardSize;
        this.maxDocsPerValue = maxDocsPerValue;
        this.executionHint = executionHint;
        this.aggregatorSupplier = aggregatorSupplier;
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {

        return aggregatorSupplier.build(name, shardSize, factories, context, parent, metadata, config, maxDocsPerValue, executionHint);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final UnmappedSampler aggregation = new UnmappedSampler(name, metadata);

        return new NonCollectingAggregator(name, context, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }
}
