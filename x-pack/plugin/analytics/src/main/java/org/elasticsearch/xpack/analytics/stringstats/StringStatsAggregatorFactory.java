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
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class StringStatsAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource.Bytes> {

    private final boolean showDistribution;

    StringStatsAggregatorFactory(String name, ValuesSourceConfig<ValuesSource.Bytes> config,
                                 Boolean showDistribution,
                                 QueryShardContext queryShardContext,
                                 AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData)
                                    throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metaData);
        this.showDistribution = showDistribution;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            List<PipelineAggregator> pipelineAggregators,
                                            Map<String, Object> metaData) throws IOException {
        return new StringStatsAggregator(name, showDistribution,null, config.format(), searchContext, parent,
                                         pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource.Bytes valuesSource,
                                          SearchContext searchContext,
                                          Aggregator parent,
                                          boolean collectsFromSingleBucket,
                                          List<PipelineAggregator> pipelineAggregators,
                                          Map<String, Object> metaData) throws IOException {
        return new StringStatsAggregator(name, showDistribution, valuesSource, config.format(), searchContext, parent,
                                         pipelineAggregators, metaData);
    }

}
