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
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

class CardinalityAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final Long precisionThreshold;

    CardinalityAggregatorFactory(String name, ValuesSourceConfig config,
                                    Long precisionThreshold,
                                    QueryShardContext queryShardContext,
                                    AggregatorFactory parent,
                                    AggregatorFactories.Builder subFactoriesBuilder,
                                    Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.precisionThreshold = precisionThreshold;
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(CardinalityAggregationBuilder.NAME, CoreValuesSourceType.ALL_CORE,
            (CardinalityAggregatorSupplier) CardinalityAggregator::new);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            Map<String, Object> metadata) throws IOException {
        return new CardinalityAggregator(name, null, precision(), searchContext, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource,
                                            SearchContext searchContext,
                                            Aggregator parent,
                                            boolean collectsFromSingleBucket,
                                            Map<String, Object> metadata) throws IOException {
        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry().getAggregator(config.valueSourceType(),
            CardinalityAggregationBuilder.NAME);
        if (aggregatorSupplier instanceof CardinalityAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected CardinalityAggregatorSupplier, found [" +
                aggregatorSupplier.getClass().toString() + "]");
        }
        CardinalityAggregatorSupplier cardinalityAggregatorSupplier = (CardinalityAggregatorSupplier) aggregatorSupplier;
        return cardinalityAggregatorSupplier.build(name, valuesSource, precision(), searchContext, parent, metadata);
    }

    private int precision() {
        return precisionThreshold == null
                ? HyperLogLogPlusPlus.DEFAULT_PRECISION
                : HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
    }
}
