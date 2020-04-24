/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.boxplot;

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
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BoxplotAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final double compression;

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(BoxplotAggregationBuilder.NAME,
            List.of(CoreValuesSourceType.NUMERIC, AnalyticsValuesSourceType.HISTOGRAM),
            (BoxplotAggregatorSupplier) BoxplotAggregator::new);
    }

    BoxplotAggregatorFactory(String name,
                             ValuesSourceConfig config,
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
        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry().getAggregator(config.valueSourceType(),
            BoxplotAggregationBuilder.NAME);

        if (aggregatorSupplier instanceof BoxplotAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected BoxplotAggregatorSupplier, found [" +
                aggregatorSupplier.getClass().toString() + "]");
        }
        return ((BoxplotAggregatorSupplier) aggregatorSupplier).build(name, valuesSource, config.format(), compression,
            searchContext, parent, metadata);
    }
}
