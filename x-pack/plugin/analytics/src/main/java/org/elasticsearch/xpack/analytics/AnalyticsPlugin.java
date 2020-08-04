/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.analytics.action.AnalyticsInfoTransportAction;
import org.elasticsearch.xpack.analytics.action.AnalyticsUsageTransportAction;
import org.elasticsearch.xpack.analytics.action.TransportAnalyticsStatsAction;
import org.elasticsearch.xpack.analytics.aggregations.AnalyticsAggregatorFactory;
import org.elasticsearch.xpack.analytics.normalize.NormalizePipelineAggregationBuilder;
import org.elasticsearch.xpack.analytics.boxplot.BoxplotAggregationBuilder;
import org.elasticsearch.xpack.analytics.boxplot.InternalBoxplot;
import org.elasticsearch.xpack.analytics.cumulativecardinality.CumulativeCardinalityPipelineAggregationBuilder;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;
import org.elasticsearch.xpack.analytics.movingPercentiles.MovingPercentilesPipelineAggregationBuilder;
import org.elasticsearch.xpack.analytics.stringstats.InternalStringStats;
import org.elasticsearch.xpack.analytics.stringstats.StringStatsAggregationBuilder;
import org.elasticsearch.xpack.analytics.topmetrics.InternalTopMetrics;
import org.elasticsearch.xpack.analytics.topmetrics.TopMetricsAggregationBuilder;
import org.elasticsearch.xpack.analytics.topmetrics.TopMetricsAggregatorFactory;
import org.elasticsearch.xpack.analytics.ttest.InternalTTest;
import org.elasticsearch.xpack.analytics.ttest.PairedTTestState;
import org.elasticsearch.xpack.analytics.ttest.TTestAggregationBuilder;
import org.elasticsearch.xpack.analytics.ttest.TTestState;
import org.elasticsearch.xpack.analytics.ttest.UnpairedTTestState;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

public class AnalyticsPlugin extends Plugin implements SearchPlugin, ActionPlugin, MapperPlugin {
    private final AnalyticsUsage usage = new AnalyticsUsage();

    public AnalyticsPlugin() { }

    public static XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

    @Override
    public List<PipelineAggregationSpec> getPipelineAggregations() {
        List<PipelineAggregationSpec> pipelineAggs = new ArrayList<>();
        pipelineAggs.add(new PipelineAggregationSpec(
            CumulativeCardinalityPipelineAggregationBuilder.NAME,
            CumulativeCardinalityPipelineAggregationBuilder::new,
            usage.track(AnalyticsStatsAction.Item.CUMULATIVE_CARDINALITY,
                checkLicense(CumulativeCardinalityPipelineAggregationBuilder.PARSER))));
        pipelineAggs.add(new PipelineAggregationSpec(
            MovingPercentilesPipelineAggregationBuilder.NAME,
            MovingPercentilesPipelineAggregationBuilder::new,
            usage.track(AnalyticsStatsAction.Item.MOVING_PERCENTILES,
                checkLicense(MovingPercentilesPipelineAggregationBuilder.PARSER))));
        pipelineAggs.add(new PipelineAggregationSpec(
            NormalizePipelineAggregationBuilder.NAME,
            NormalizePipelineAggregationBuilder::new,
            usage.track(AnalyticsStatsAction.Item.NORMALIZE,
                checkLicense(NormalizePipelineAggregationBuilder.PARSER))));
        return pipelineAggs;
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return Arrays.asList(
            new AggregationSpec(
                StringStatsAggregationBuilder.NAME,
                StringStatsAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.STRING_STATS, checkLicense(StringStatsAggregationBuilder.PARSER)))
                .addResultReader(InternalStringStats::new)
                .setAggregatorRegistrar(StringStatsAggregationBuilder::registerAggregators),
            new AggregationSpec(
                BoxplotAggregationBuilder.NAME,
                BoxplotAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.BOXPLOT, checkLicense(BoxplotAggregationBuilder.PARSER)))
                .addResultReader(InternalBoxplot::new)
                .setAggregatorRegistrar(BoxplotAggregationBuilder::registerAggregators),
            new AggregationSpec(
                TopMetricsAggregationBuilder.NAME,
                TopMetricsAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.TOP_METRICS, checkLicense(TopMetricsAggregationBuilder.PARSER)))
                .addResultReader(InternalTopMetrics::new),
            new AggregationSpec(
                TTestAggregationBuilder.NAME,
                TTestAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.T_TEST, checkLicense(TTestAggregationBuilder.PARSER)))
                .addResultReader(InternalTTest::new)
                .setAggregatorRegistrar(TTestAggregationBuilder::registerUsage)
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
    public List<Setting<?>> getSettings() {
        return singletonList(TopMetricsAggregatorFactory.MAX_BUCKET_SIZE);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(HistogramFieldMapper.CONTENT_TYPE, new HistogramFieldMapper.TypeParser());
    }

    @Override
    public List<Consumer<ValuesSourceRegistry.Builder>> getAggregationExtentions() {
            return List.of(
                AnalyticsAggregatorFactory::registerPercentilesAggregator,
                AnalyticsAggregatorFactory::registerPercentileRanksAggregator,
                AnalyticsAggregatorFactory::registerHistoBackedSumAggregator,
                AnalyticsAggregatorFactory::registerHistoBackedValueCountAggregator,
                AnalyticsAggregatorFactory::registerHistoBackedAverageAggregator,
                AnalyticsAggregatorFactory::registerHistoBackedHistogramAggregator
            );
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry,
            Environment environment, NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver, Supplier<RepositoriesService> repositoriesServiceSupplier) {
        return singletonList(usage);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
            new NamedWriteableRegistry.Entry(TTestState.class, PairedTTestState.NAME, PairedTTestState::new),
            new NamedWriteableRegistry.Entry(TTestState.class, UnpairedTTestState.NAME, UnpairedTTestState::new)
        );
    }

    private static <T> ContextParser<String, T> checkLicense(ContextParser<String, T> realParser) {
        return (parser, name) -> {
            if (getLicenseState().checkFeature(XPackLicenseState.Feature.ANALYTICS) == false) {
                throw LicenseUtils.newComplianceException(XPackField.ANALYTICS);
            }
            return realParser.parse(parser, name);
        };
    }
}
