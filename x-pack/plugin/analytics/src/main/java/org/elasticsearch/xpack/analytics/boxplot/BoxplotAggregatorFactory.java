/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public class BoxplotAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource> {

    private final double compression;

    BoxplotAggregatorFactory(String name,
                             ValuesSourceConfig<ValuesSource> config,
                             double compression,
                             QueryShardContext queryShardContext,
                             AggregatorFactory parent,
                             AggregatorFactories.Builder subFactoriesBuilder,
                             Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.compression = compression;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                        Aggregator parent,
                                        Map<String, Object> metadata)
        throws IOException {
        return new BoxplotAggregator(name, null, config.format(), compression, searchContext, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource,
                                          SearchContext searchContext,
                                          Aggregator parent,
                                          boolean collectsFromSingleBucket,
                                          Map<String, Object> metadata) throws IOException {
        return new BoxplotAggregator(name, valuesSource, config.format(), compression, searchContext, parent, metadata);
    }

}
