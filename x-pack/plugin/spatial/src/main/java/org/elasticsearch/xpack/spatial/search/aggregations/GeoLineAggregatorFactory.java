/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSource;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

final class GeoLineAggregatorFactory extends MultiValuesSourceAggregatorFactory<ValuesSource> {

    GeoLineAggregatorFactory(String name,
                             Map<String, ValuesSourceConfig<ValuesSource>> configs,
                             DocValueFormat format, QueryShardContext queryShardContext, AggregatorFactory parent,
                             AggregatorFactories.Builder subFactoriesBuilder,
                             Map<String, Object> metaData) throws IOException {
        super(name, configs, format, queryShardContext, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                                        Map<String, Object> metaData) throws IOException {
        return new GeoLineAggregator(name, null, searchContext, parent, pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext, Map<String, ValuesSourceConfig<ValuesSource>> configs,
                                          DocValueFormat format, Aggregator parent, boolean collectsFromSingleBucket,
                                          List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        MultiValuesSource.AnyMultiValuesSource valuesSources = new MultiValuesSource
            .AnyMultiValuesSource(configs, searchContext.getQueryShardContext());
        return new GeoLineAggregator(name, valuesSources, searchContext, parent, pipelineAggregators, metaData);
    }
}
