/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TopMetricsAggregatorFactory extends AggregatorFactory {
    /**
     * Index setting describing the maximum number of top metrics that
     * can be collected per bucket. This defaults to a low number because
     * there can be a *huge* number of buckets
     */
    public static final Setting<Integer> MAX_BUCKET_SIZE =
        Setting.intSetting("index.top_metrics_max_size", 10, 1, Property.Dynamic, Property.IndexScope);

    private final List<SortBuilder<?>> sortBuilders;
    private final int size;
    private final MultiValuesSourceFieldConfig metricField;

    public TopMetricsAggregatorFactory(String name, QueryShardContext queryShardContext, AggregatorFactory parent,
            Builder subFactoriesBuilder, Map<String, Object> metaData, List<SortBuilder<?>> sortBuilders,
            int size, MultiValuesSourceFieldConfig metricField) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metaData);
        this.sortBuilders = sortBuilders;
        this.size = size;
        this.metricField = metricField;
    }

    @Override
    protected TopMetricsAggregator createInternal(SearchContext searchContext, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        ValuesSourceConfig<ValuesSource.Numeric> metricFieldSource = ValuesSourceConfig.resolve(queryShardContext, ValueType.NUMERIC,
                metricField.getFieldName(), metricField.getScript(), metricField.getMissing(), metricField.getTimeZone(), null);
        ValuesSource.Numeric metricValueSource = metricFieldSource.toValuesSource(queryShardContext);
        int maxBucketSize = MAX_BUCKET_SIZE.get(searchContext.getQueryShardContext().getIndexSettings().getSettings());
        if (size > maxBucketSize) {
            throw new IllegalArgumentException("[top_metrics.size] must not be more than [" + maxBucketSize + "] but was [" + size
                    + "]. This limit can be set by changing the [" + MAX_BUCKET_SIZE.getKey()
                    + "] index level setting.");
        }
        if (metricValueSource == null) {
            return createUnmapped(searchContext, parent, pipelineAggregators, metaData);
        }
        return new TopMetricsAggregator(name, searchContext, parent, pipelineAggregators, metaData, size, metricField.getFieldName(),
                sortBuilders.get(0), metricValueSource);
    }

    private TopMetricsAggregator createUnmapped(SearchContext searchContext, Aggregator parent,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        return new TopMetricsAggregator(name, searchContext, parent, pipelineAggregators, metaData, size, metricField.getFieldName(),
                null, null);
    }

}
