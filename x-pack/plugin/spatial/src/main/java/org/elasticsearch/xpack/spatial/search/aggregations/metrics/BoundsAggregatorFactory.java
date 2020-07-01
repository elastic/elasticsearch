/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

class BoundsAggregatorFactory extends ValuesSourceAggregatorFactory {

    BoundsAggregatorFactory(String name,
                            ValuesSourceConfig config,
                            QueryShardContext queryShardContext,
                            AggregatorFactory parent,
                            AggregatorFactories.Builder subFactoriesBuilder,
                            Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            Map<String, Object> metadata) throws IOException {
        return new PointBoundsAggregator(name, searchContext, parent, config, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext,
                                          Aggregator parent,
                                          boolean collectsFromSingleBucket,
                                          Map<String, Object> metadata) throws IOException {
        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry()
            .getAggregator(config, BoundsAggregationBuilder.NAME);

        if (aggregatorSupplier instanceof BoundsAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected "
                + BoundsAggregatorSupplier.class.getName() + ", found [" + aggregatorSupplier.getClass().toString() + "]");
        }

        return ((BoundsAggregatorSupplier) aggregatorSupplier).build(name, searchContext, parent, config, metadata);
    }
}
