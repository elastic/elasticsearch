/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.aggregations.pca;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ArrayValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

final class PCAAggregatorFactory
    extends ArrayValuesSourceAggregatorFactory<ValuesSource.Numeric> {
    private final MultiValueMode multiValueMode;
    private final boolean useCovariance;

    PCAAggregatorFactory(String name,
                         Map<String, ValuesSourceConfig<ValuesSource.Numeric>> configs, MultiValueMode multiValueMode,
                         boolean useCovariance, QueryShardContext queryShardContext, AggregatorFactory parent,
                         AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData) throws IOException {
        super(name, configs, queryShardContext, parent, subFactoriesBuilder, metaData);
        this.multiValueMode = multiValueMode;
        this.useCovariance = useCovariance;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                        Aggregator parent,
                                        List<PipelineAggregator> pipelineAggregators,
                                        Map<String, Object> metaData)
        throws IOException {
        return new PCAAggregator(name, null, searchContext, parent, multiValueMode, useCovariance, pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(Map<String, ValuesSource.Numeric> valuesSources,
                                          SearchContext searchContext,
                                          Aggregator parent,
                                          boolean collectsFromSingleBucket,
                                          List<PipelineAggregator> pipelineAggregators,
                                          Map<String, Object> metaData) throws IOException {
        return new PCAAggregator(name, valuesSources, searchContext, parent, multiValueMode, useCovariance, pipelineAggregators, metaData);
    }
}
