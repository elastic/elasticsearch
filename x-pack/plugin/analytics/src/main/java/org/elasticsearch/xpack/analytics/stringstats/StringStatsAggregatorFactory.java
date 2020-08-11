/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.stringstats;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

class StringStatsAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final boolean showDistribution;

    StringStatsAggregatorFactory(String name, ValuesSourceConfig config,
                                 Boolean showDistribution,
                                 QueryShardContext queryShardContext,
                                 AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metadata)
                                    throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.showDistribution = showDistribution;
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            StringStatsAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.BYTES, StringStatsAggregator::new, true);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            Map<String, Object> metadata) throws IOException {
        return new StringStatsAggregator(name, null, showDistribution, config.format(), searchContext, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext,
                                          Aggregator parent,
                                          CardinalityUpperBound cardinality,
                                          Map<String, Object> metadata) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(StringStatsAggregationBuilder.REGISTRY_KEY, config)
            .build(name, config.getValuesSource(), showDistribution, config.format(), searchContext, parent, metadata);
    }

}
