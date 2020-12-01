/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.stringstats;

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

class StringStatsAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final boolean showDistribution;

    StringStatsAggregatorFactory(String name, ValuesSourceConfig config,
                                 Boolean showDistribution,
                                 AggregationContext context,
                                 AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metadata)
                                    throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.showDistribution = showDistribution;
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            StringStatsAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.BYTES, StringStatsAggregator::new, true);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new StringStatsAggregator(name, null, showDistribution, config.format(), context, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return context.getValuesSourceRegistry()
            .getAggregator(StringStatsAggregationBuilder.REGISTRY_KEY, config)
            .build(name, config.getValuesSource(), showDistribution, config.format(), context, parent, metadata);
    }

}
