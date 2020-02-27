/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.xpack.analytics.action.AnalyticsInfoTransportAction;
import org.elasticsearch.xpack.analytics.action.AnalyticsUsageTransportAction;
import org.elasticsearch.xpack.analytics.action.TransportAnalyticsStatsAction;
import org.elasticsearch.xpack.analytics.boxplot.BoxplotAggregationBuilder;
import org.elasticsearch.xpack.analytics.boxplot.InternalBoxplot;
import org.elasticsearch.xpack.analytics.cumulativecardinality.CumulativeCardinalityPipelineAggregationBuilder;
import org.elasticsearch.xpack.analytics.cumulativecardinality.CumulativeCardinalityPipelineAggregator;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;
import org.elasticsearch.xpack.analytics.stringstats.InternalStringStats;
import org.elasticsearch.xpack.analytics.stringstats.StringStatsAggregationBuilder;
import org.elasticsearch.xpack.analytics.topmetrics.InternalTopMetrics;
import org.elasticsearch.xpack.analytics.topmetrics.TopMetricsAggregationBuilder;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonList;

public class AnalyticsPlugin extends Plugin implements SearchPlugin, ActionPlugin, MapperPlugin {

    // TODO this should probably become more structured
    public static AtomicLong cumulativeCardUsage = new AtomicLong(0);
    public static AtomicLong topMetricsUsage = new AtomicLong(0);

    public AnalyticsPlugin() { }

    public static XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

    @Override
    public List<PipelineAggregationSpec> getPipelineAggregations() {
        return singletonList(
            new PipelineAggregationSpec(
                CumulativeCardinalityPipelineAggregationBuilder.NAME,
                CumulativeCardinalityPipelineAggregationBuilder::new,
                CumulativeCardinalityPipelineAggregator::new,
                CumulativeCardinalityPipelineAggregationBuilder.PARSER)
        );
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return Arrays.asList(
            new AggregationSpec(
                StringStatsAggregationBuilder.NAME,
                StringStatsAggregationBuilder::new,
                StringStatsAggregationBuilder.PARSER).addResultReader(InternalStringStats::new),
            new AggregationSpec(
                BoxplotAggregationBuilder.NAME,
                BoxplotAggregationBuilder::new,
                BoxplotAggregationBuilder.PARSER)
                .addResultReader(InternalBoxplot::new),
            new AggregationSpec(
                TopMetricsAggregationBuilder.NAME,
                TopMetricsAggregationBuilder::new,
                track(TopMetricsAggregationBuilder.PARSER, topMetricsUsage))
                .addResultReader(InternalTopMetrics::new)
        );
    }

    @Override
    public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(XPackUsageFeatureAction.ANALYTICS, AnalyticsUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.ANALYTICS, AnalyticsInfoTransportAction.class),
            new ActionHandler<>(AnalyticsStatsAction.INSTANCE, TransportAnalyticsStatsAction.class));
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(HistogramFieldMapper.CONTENT_TYPE, new HistogramFieldMapper.TypeParser());
    }

    /**
     * Track successful parsing.
     */
    private static <T> ContextParser<String, T> track(ContextParser<String, T> realParser, AtomicLong usage) {
        return (parser, name) -> {
            T value = realParser.parse(parser, name);
            // Intentionally doesn't count unless the parser returns cleanly.
            usage.addAndGet(1);
            return value;
        };
    }
}
