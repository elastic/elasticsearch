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
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BoxplotAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource.Numeric> {

    private final double compression;

    BoxplotAggregatorFactory(String name,
                             ValuesSourceConfig<ValuesSource.Numeric> config,
                             double compression,
                             QueryShardContext queryShardContext,
                             AggregatorFactory parent,
                             AggregatorFactories.Builder subFactoriesBuilder,
                             Map<String, Object> metaData) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metaData);
        this.compression = compression;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                        Aggregator parent,
                                        List<PipelineAggregator> pipelineAggregators,
                                        Map<String, Object> metaData)
        throws IOException {
        return new BoxplotAggregator(name, null, config.format(), compression, searchContext, parent,
            pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource.Numeric valuesSource,
                                          SearchContext searchContext,
                                          Aggregator parent,
                                          boolean collectsFromSingleBucket,
                                          List<PipelineAggregator> pipelineAggregators,
                                          Map<String, Object> metaData) throws IOException {
        return new BoxplotAggregator(name, valuesSource, config.format(), compression, searchContext, parent,
            pipelineAggregators, metaData);
    }

}
