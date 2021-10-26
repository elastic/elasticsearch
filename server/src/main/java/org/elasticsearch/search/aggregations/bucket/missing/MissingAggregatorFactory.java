/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.missing;

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

public class MissingAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final MissingAggregatorSupplier aggregatorSupplier;

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(MissingAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.ALL_CORE, MissingAggregator::new, true);
    }

    public MissingAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        MissingAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.aggregatorSupplier = aggregatorSupplier;
    }

    @Override
    protected MissingAggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new MissingAggregator(name, factories, config, context, parent, CardinalityUpperBound.NONE, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return aggregatorSupplier.build(name, factories, config, context, parent, cardinality, metadata);
    }
}
