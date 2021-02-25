/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xpack.analytics.topmetrics.TopMetricsAggregator.MetricValues;
import org.elasticsearch.xpack.analytics.topmetrics.TopMetricsAggregator.MetricValuesSupplier;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.analytics.topmetrics.TopMetricsAggregationBuilder.REGISTRY_KEY;

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
    private final List<MultiValuesSourceFieldConfig> metricFields;

    public TopMetricsAggregatorFactory(String name, AggregationContext context, AggregatorFactory parent,
            Builder subFactoriesBuilder, Map<String, Object> metadata, List<SortBuilder<?>> sortBuilders,
            int size, List<MultiValuesSourceFieldConfig> metricFields) throws IOException {
        super(name, context, parent, subFactoriesBuilder, metadata);
        this.sortBuilders = sortBuilders;
        this.size = size;
        this.metricFields = metricFields;
    }

    @Override
    protected TopMetricsAggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        int maxBucketSize = MAX_BUCKET_SIZE.get(context.getIndexSettings().getSettings());
        if (size > maxBucketSize) {
            throw new IllegalArgumentException("[top_metrics.size] must not be more than [" + maxBucketSize + "] but was [" + size
                    + "]. This limit can be set by changing the [" + MAX_BUCKET_SIZE.getKey()
                    + "] index level setting.");
        }
        MetricValues[] metricValues = new MetricValues[metricFields.size()];
        for (int i = 0; i < metricFields.size(); i++) {
            MultiValuesSourceFieldConfig config = metricFields.get(i);
            ValuesSourceConfig vsConfig = ValuesSourceConfig.resolve(
                context,
                null,
                config.getFieldName(),
                config.getScript(),
                config.getMissing(),
                config.getTimeZone(),
                null,
                CoreValuesSourceType.NUMERIC
            );
            MetricValuesSupplier supplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, vsConfig);
            metricValues[i] = supplier.build(size, context.bigArrays(), config.getFieldName(), vsConfig);
        }
        return new TopMetricsAggregator(name, context, parent, metadata, size, sortBuilders.get(0), metricValues);
    }
}
