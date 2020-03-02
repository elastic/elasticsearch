/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class ValueCountAggregatorFactory extends ValuesSourceAggregatorFactory {

    public static void registerAggregators(ValuesSourceRegistry valuesSourceRegistry) {
        valuesSourceRegistry.registerAny(ValueCountAggregationBuilder.NAME,
            new ValueCountAggregatorSupplier() {
                @Override
                public Aggregator build(String name,
                                        ValuesSource valuesSource,
                                        SearchContext aggregationContext,
                                        Aggregator parent,
                                        List<PipelineAggregator> pipelineAggregators,
                                        Map<String, Object> metaData) throws IOException {
                    return new ValueCountAggregator(name, valuesSource, aggregationContext, parent, pipelineAggregators, metaData);
                }
            });
    }

    ValueCountAggregatorFactory(String name, ValuesSourceConfig config, QueryShardContext queryShardContext,
                                AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder,
                                Map<String, Object> metaData) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            List<PipelineAggregator> pipelineAggregators,
                                            Map<String, Object> metaData) throws IOException {
        return new ValueCountAggregator(name, null, searchContext, parent, pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource,
                                            SearchContext searchContext,
                                            Aggregator parent,
                                            boolean collectsFromSingleBucket,
                                            List<PipelineAggregator> pipelineAggregators,
                                            Map<String, Object> metaData) throws IOException {
        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry().getAggregator(config.valueSourceType(),
            ValueCountAggregationBuilder.NAME);
        if (aggregatorSupplier instanceof ValueCountAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected ValueCountAggregatorSupplier, found [" +
                aggregatorSupplier.getClass().toString() + "]");
        }
        return ((ValueCountAggregatorSupplier) aggregatorSupplier)
            .build(name, valuesSource, searchContext, parent, pipelineAggregators,metaData);
    }
}
