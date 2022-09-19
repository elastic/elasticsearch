/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.analytics;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.stats.AnalyticsStatsAction;
import org.elasticsearch.analytics.action.TransportAnalyticsStatsAction;
import org.elasticsearch.analytics.aggregations.AnalyticsAggregatorFactory;
import org.elasticsearch.analytics.boxplot.BoxplotAggregationBuilder;
import org.elasticsearch.analytics.boxplot.InternalBoxplot;
import org.elasticsearch.analytics.cumulativecardinality.CumulativeCardinalityPipelineAggregationBuilder;
import org.elasticsearch.analytics.cumulativecardinality.InternalSimpleLongValue;
import org.elasticsearch.analytics.mapper.HistogramFieldMapper;
import org.elasticsearch.analytics.movingPercentiles.MovingPercentilesPipelineAggregationBuilder;
import org.elasticsearch.analytics.multiterms.InternalMultiTerms;
import org.elasticsearch.analytics.multiterms.MultiTermsAggregationBuilder;
import org.elasticsearch.analytics.normalize.NormalizePipelineAggregationBuilder;
import org.elasticsearch.analytics.rate.InternalRate;
import org.elasticsearch.analytics.rate.RateAggregationBuilder;
import org.elasticsearch.analytics.stringstats.InternalStringStats;
import org.elasticsearch.analytics.stringstats.StringStatsAggregationBuilder;
import org.elasticsearch.analytics.topmetrics.InternalTopMetrics;
import org.elasticsearch.analytics.topmetrics.TopMetricsAggregationBuilder;
import org.elasticsearch.analytics.topmetrics.TopMetricsAggregatorFactory;
import org.elasticsearch.analytics.ttest.InternalTTest;
import org.elasticsearch.analytics.ttest.PairedTTestState;
import org.elasticsearch.analytics.ttest.TTestAggregationBuilder;
import org.elasticsearch.analytics.ttest.TTestState;
import org.elasticsearch.analytics.ttest.UnpairedTTestState;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class AnalyticsPlugin extends Plugin implements SearchPlugin, ActionPlugin, MapperPlugin {
    private final AnalyticsUsage usage = new AnalyticsUsage();

    public AnalyticsPlugin() {}

    @Override
    public List<PipelineAggregationSpec> getPipelineAggregations() {
        List<PipelineAggregationSpec> pipelineAggs = new ArrayList<>();
        pipelineAggs.add(
            new PipelineAggregationSpec(
                CumulativeCardinalityPipelineAggregationBuilder.NAME,
                CumulativeCardinalityPipelineAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.CUMULATIVE_CARDINALITY, CumulativeCardinalityPipelineAggregationBuilder.PARSER)
            ).addResultReader(InternalSimpleLongValue.NAME, InternalSimpleLongValue::new)
        );
        pipelineAggs.add(
            new PipelineAggregationSpec(
                MovingPercentilesPipelineAggregationBuilder.NAME,
                MovingPercentilesPipelineAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.MOVING_PERCENTILES, MovingPercentilesPipelineAggregationBuilder.PARSER)
            )
        );
        pipelineAggs.add(
            new PipelineAggregationSpec(
                NormalizePipelineAggregationBuilder.NAME,
                NormalizePipelineAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.NORMALIZE, NormalizePipelineAggregationBuilder.PARSER)
            )
        );
        return pipelineAggs;
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return List.of(
            new AggregationSpec(
                StringStatsAggregationBuilder.NAME,
                StringStatsAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.STRING_STATS, StringStatsAggregationBuilder.PARSER)
            ).addResultReader(InternalStringStats::new).setAggregatorRegistrar(StringStatsAggregationBuilder::registerAggregators),
            new AggregationSpec(
                BoxplotAggregationBuilder.NAME,
                BoxplotAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.BOXPLOT, BoxplotAggregationBuilder.PARSER)
            ).addResultReader(InternalBoxplot::new).setAggregatorRegistrar(BoxplotAggregationBuilder::registerAggregators),
            new AggregationSpec(
                TopMetricsAggregationBuilder.NAME,
                TopMetricsAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.TOP_METRICS, TopMetricsAggregationBuilder.PARSER)
            ).addResultReader(InternalTopMetrics::new).setAggregatorRegistrar(TopMetricsAggregationBuilder::registerAggregators),
            new AggregationSpec(
                TTestAggregationBuilder.NAME,
                TTestAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.T_TEST, TTestAggregationBuilder.PARSER)
            ).addResultReader(InternalTTest::new).setAggregatorRegistrar(TTestAggregationBuilder::registerUsage),
            new AggregationSpec(
                RateAggregationBuilder.NAME,
                RateAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.RATE, RateAggregationBuilder.PARSER)
            ).addResultReader(InternalRate::new).setAggregatorRegistrar(RateAggregationBuilder::registerAggregators),
            new AggregationSpec(
                MultiTermsAggregationBuilder.NAME,
                MultiTermsAggregationBuilder::new,
                usage.track(AnalyticsStatsAction.Item.MULTI_TERMS, MultiTermsAggregationBuilder.PARSER)
            ).addResultReader(InternalMultiTerms::new).setAggregatorRegistrar(MultiTermsAggregationBuilder::registerAggregators)
        );
    }

    @Override
    public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(AnalyticsStatsAction.INSTANCE, TransportAnalyticsStatsAction.class));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(TopMetricsAggregatorFactory.MAX_BUCKET_SIZE);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(HistogramFieldMapper.CONTENT_TYPE, HistogramFieldMapper.PARSER);
    }

    @Override
    public List<Consumer<ValuesSourceRegistry.Builder>> getAggregationExtentions() {
        return List.of(
            AnalyticsAggregatorFactory::registerPercentilesAggregator,
            AnalyticsAggregatorFactory::registerPercentileRanksAggregator,
            AnalyticsAggregatorFactory::registerHistoBackedSumAggregator,
            AnalyticsAggregatorFactory::registerHistoBackedValueCountAggregator,
            AnalyticsAggregatorFactory::registerHistoBackedAverageAggregator,
            AnalyticsAggregatorFactory::registerHistoBackedHistogramAggregator,
            AnalyticsAggregatorFactory::registerHistoBackedMinggregator,
            AnalyticsAggregatorFactory::registerHistoBackedMaxggregator,
            AnalyticsAggregatorFactory::registerHistoBackedRangeAggregator
        );
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationDeciders allocationDeciders
    ) {
        return List.of(usage);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(TTestState.class, PairedTTestState.NAME, PairedTTestState::new),
            new NamedWriteableRegistry.Entry(TTestState.class, UnpairedTTestState.NAME, UnpairedTTestState::new)
        );
    }
}
