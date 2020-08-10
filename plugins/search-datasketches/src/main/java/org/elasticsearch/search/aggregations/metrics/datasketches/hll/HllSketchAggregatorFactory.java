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
package org.elasticsearch.search.aggregations.metrics.datasketches.hll;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public class HllSketchAggregatorFactory extends ValuesSourceAggregatorFactory {
    private final Integer defaultLgk;

    /**
     * Constructor of HllSketchAggregatorFactory
     * @param name name of aggregator
     * @param config config about value source
     * @param defaultLgk default lgK when any hll union construct
     * @param queryShardContext context about query,index and shard
     * @param parent parent of this AggregatorFactory
     * @param subFactoriesBuilder sub factories of this AggregatorFactory
     * @param metaData metadata of this AggregatorFactory
     * @throws IOException throws by it's super class
     */
    public HllSketchAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        Integer defaultLgk,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metaData
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metaData);
        this.defaultLgk = defaultLgk;
    }

    /**
     * register HllSketchAggregatorSupplier
     * @param builder provide by hll aggregation builder
     */
    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            HllSketchAggregationBuilder.NAME,
            CoreValuesSourceType.ALL_CORE,
            (HllSketchAggregatorSupplier) HllSketchAggregator::new
        );
    }

    /**
     * create unmapped HllSketchAggregator
     * @param searchContext Context about query
     * @param parent parent Aggregator
     * @param metadata metadata of this Aggregator
     * @return Aggregator unmapped
     * @throws IOException throws by HllSketchAggregator's super class
     */
    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new HllSketchAggregator(this.name, config, this.defaultLgk(), searchContext, parent, metadata);
    }

    /**
     * create internal HllSketchAggregator
     * @param searchContext Context about query
     * @param parent parent Aggregator
     * @param collectsFromSingleBucket non used parameter
     * @param metadata metadata of this Aggregator
     * @return internal Aggregator
     * @throws IOException throws by HllSketchAggregator's super class
     */
    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        boolean collectsFromSingleBucket,
        Map<String, Object> metadata
    ) throws IOException {
        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry()
            .getAggregator(config, HllSketchAggregationBuilder.NAME);
        if (aggregatorSupplier instanceof HllSketchAggregatorSupplier == false) {
            throw new AggregationExecutionException(
                "Registry miss-match - expected HllSketchAggregatorSupplier, found [" + aggregatorSupplier.getClass().toString() + "]"
            );
        }
        HllSketchAggregatorSupplier hllSketchAggregatorSupplier = (HllSketchAggregatorSupplier) aggregatorSupplier;
        return hllSketchAggregatorSupplier.build(name, config, defaultLgk(), searchContext, parent, metadata);
    }

    private int defaultLgk() {
        return this.defaultLgk == null ? 15 : defaultLgk;
    }
}
