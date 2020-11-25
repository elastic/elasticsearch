/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoLineMultiValuesSource;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Map;

final class GeoLineAggregatorFactory extends MultiValuesSourceAggregatorFactory {

    private boolean includeSort;
    private SortOrder sortOrder;
    private int size;

    GeoLineAggregatorFactory(String name,
                             Map<String, ValuesSourceConfig> configs,
                             DocValueFormat format, AggregationContext aggregationContext, AggregatorFactory parent,
                             AggregatorFactories.Builder subFactoriesBuilder,
                             Map<String, Object> metaData, boolean includeSort, SortOrder sortOrder, int size) throws IOException {
        super(name, configs, format, aggregationContext, parent, subFactoriesBuilder, metaData);
        this.includeSort = includeSort;
        this.sortOrder = sortOrder;
        this.size = size;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                        Aggregator parent,
                                        Map<String, Object> metaData) throws IOException {
        return new GeoLineAggregator(name, null, searchContext, parent, metaData, includeSort, sortOrder, size);
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext,
                                          Map<String, ValuesSourceConfig> configs,
                                          DocValueFormat format,
                                          Aggregator parent,
                                          CardinalityUpperBound cardinality,
                                          Map<String, Object> metaData) throws IOException {
        GeoLineMultiValuesSource valuesSources =
            new GeoLineMultiValuesSource(configs, searchContext.getQueryShardContext());
        return new GeoLineAggregator(name, valuesSources, searchContext, parent, metaData, includeSort, sortOrder, size);
    }

    @Override
    public String getStatsSubtype() {
        return configs.get(GeoLineAggregationBuilder.POINT_FIELD.getPreferredName()).valueSourceType().typeName();
    }
}
