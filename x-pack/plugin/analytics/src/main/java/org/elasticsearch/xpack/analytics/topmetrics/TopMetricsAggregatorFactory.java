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
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xpack.core.analytics.AnalyticsSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class TopMetricsAggregatorFactory extends AggregatorFactory {
    private final List<SortBuilder<?>> sortBuilders;
    private final int size;
    private final List<MultiValuesSourceFieldConfig> metricFields;

    public TopMetricsAggregatorFactory(String name, QueryShardContext queryShardContext, AggregatorFactory parent,
            Builder subFactoriesBuilder, Map<String, Object> metaData, List<SortBuilder<?>> sortBuilders,
            int size, List<MultiValuesSourceFieldConfig> metricFields) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metaData);
        this.sortBuilders = sortBuilders;
        this.size = size;
        this.metricFields = metricFields;
    }

    @Override
    protected TopMetricsAggregator createInternal(SearchContext searchContext, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        int maxBucketSize = AnalyticsSettings.MAX_BUCKET_SIZE_SETTING.get(searchContext.getQueryShardContext().getIndexSettings().getSettings());
        if (size > maxBucketSize) {
            throw new IllegalArgumentException("[top_metrics.size] must not be more than [" + maxBucketSize + "] but was [" + size
                    + "]. This limit can be set by changing the [" + AnalyticsSettings.MAX_BUCKET_SIZE_SETTING.getKey()
                    + "] index level setting.");
        }
        List<TopMetricsAggregator.MetricSource> metricSources = metricFields.stream().map(config -> {
                    ValuesSourceConfig<ValuesSource.Numeric> resolved = ValuesSourceConfig.resolve(
                            searchContext.getQueryShardContext(), ValueType.NUMERIC,
                            config.getFieldName(), config.getScript(), config.getMissing(), config.getTimeZone(), null);
                    return new TopMetricsAggregator.MetricSource(config.getFieldName(), resolved.format(),
                            resolved.toValuesSource(searchContext.getQueryShardContext()));
                }).collect(toList());
        return new TopMetricsAggregator(name, searchContext, parent, pipelineAggregators, metaData, size,
                sortBuilders.get(0), metricSources);
    }
}
