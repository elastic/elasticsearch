/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TopMetricsAggregatorFactory extends AggregatorFactory {
    private final List<SortBuilder<?>> sortBuilders;
    private final MultiValuesSourceFieldConfig metricField;

    public TopMetricsAggregatorFactory(String name, QueryShardContext queryShardContext, AggregatorFactory parent,
            Builder subFactoriesBuilder, Map<String, Object> metaData, List<SortBuilder<?>> sortBuilders,
            MultiValuesSourceFieldConfig metricField) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metaData);
        this.sortBuilders = sortBuilders;
        this.metricField = metricField;
    }

    @Override
    protected TopMetricsAggregator createInternal(SearchContext searchContext, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        ValuesSourceConfig metricFieldSource = ValuesSourceConfig.resolve(queryShardContext, ValueType.NUMERIC,
                metricField.getFieldName(), metricField.getScript(), metricField.getMissing(), metricField.getTimeZone(), null,
            CoreValuesSourceType.NUMERIC, TopMetricsAggregationBuilder.NAME);
        ValuesSource.Numeric metricValueSource = (ValuesSource.Numeric) metricFieldSource.toValuesSource(queryShardContext::nowInMillis);
        if (metricValueSource == null) {
            return createUnmapped(searchContext, parent, pipelineAggregators, metaData);
        }
        BucketedSort bucketedSort = sortBuilders.get(0).buildBucketedSort(searchContext.getQueryShardContext());

        return new TopMetricsAggregator(name, searchContext, parent, pipelineAggregators, metaData, bucketedSort,
                metricField.getFieldName(), metricValueSource);
    }

    private TopMetricsAggregator createUnmapped(SearchContext searchContext, Aggregator parent,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        return new TopMetricsAggregator(name, searchContext, parent, pipelineAggregators, metaData, null, metricField.getFieldName(),
                null);
    }

}
