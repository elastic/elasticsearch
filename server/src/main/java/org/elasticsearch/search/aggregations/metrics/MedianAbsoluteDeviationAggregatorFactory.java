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
import org.elasticsearch.search.DocValueFormat;
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

public class MedianAbsoluteDeviationAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final double compression;

    MedianAbsoluteDeviationAggregatorFactory(String name,
                                             ValuesSourceConfig config,
                                             QueryShardContext queryShardContext,
                                             AggregatorFactory parent,
                                             AggregatorFactories.Builder subFactoriesBuilder,
                                             Map<String, Object> metadata,
                                             double compression) throws IOException {

        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.compression = compression;
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(MedianAbsoluteDeviationAggregationBuilder.NAME,
            CoreValuesSourceType.NUMERIC,
            new MedianAbsoluteDeviationAggregatorSupplier() {
                @Override
                public Aggregator build(String name,
                                        ValuesSource valuesSource,
                                        DocValueFormat format,
                                        SearchContext context,
                                        Aggregator parent,
                                        Map<String, Object> metadata,
                                        double compression) throws IOException {
                    return new MedianAbsoluteDeviationAggregator(
                        name,
                        context,
                        parent,
                        metadata,
                        (ValuesSource.Numeric) valuesSource,
                        format,
                        compression
                    );
                }
            });
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                        Aggregator parent,
                                        Map<String, Object> metadata) throws IOException {

        return new MedianAbsoluteDeviationAggregator(
            name,
            searchContext,
            parent,
            metadata,
            null,
            config.format(),
            compression
        );
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource,
                                            SearchContext searchContext,
                                            Aggregator parent,
                                            boolean collectsFromSingleBucket,
                                            Map<String, Object> metadata) throws IOException {
        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry().getAggregator(config.valueSourceType(),
            MedianAbsoluteDeviationAggregationBuilder.NAME);

        if (aggregatorSupplier instanceof MedianAbsoluteDeviationAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected MedianAbsoluteDeviationAggregatorSupplier, found [" +
                aggregatorSupplier.getClass().toString() + "]");
        }
        return ((MedianAbsoluteDeviationAggregatorSupplier) aggregatorSupplier).build(name, valuesSource, config.format(),
            searchContext, parent, metadata, compression);
    }
}
