/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.lucene.LuceneOperator;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.AbstractPageMappingToIteratorOperator;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.LimitOperator;
import org.elasticsearch.compute.operator.MvExpandOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.compute.operator.topn.TopNOperatorStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.esql.EsqlInfoTransportAction;
import org.elasticsearch.xpack.esql.EsqlUsageTransportAction;
import org.elasticsearch.xpack.esql.action.EsqlAsyncGetResultAction;
import org.elasticsearch.xpack.esql.action.EsqlAsyncStopAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.elasticsearch.xpack.esql.action.RestEsqlAsyncQueryAction;
import org.elasticsearch.xpack.esql.action.RestEsqlDeleteAsyncResultAction;
import org.elasticsearch.xpack.esql.action.RestEsqlGetAsyncResultAction;
import org.elasticsearch.xpack.esql.action.RestEsqlQueryAction;
import org.elasticsearch.xpack.esql.action.RestEsqlStopAsyncAction;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupOperator;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexOperator;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.expression.ExpressionWritables;
import org.elasticsearch.xpack.esql.plan.PlanWritables;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.querylog.EsqlQueryLog;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class EsqlPlugin extends Plugin implements ActionPlugin {
    public static final FeatureFlag INLINESTATS_FEATURE_FLAG = new FeatureFlag("esql_inlinestats");

    public static final String ESQL_WORKER_THREAD_POOL_NAME = "esql_worker";

    public static final Setting<Integer> QUERY_RESULT_TRUNCATION_MAX_SIZE = Setting.intSetting(
        "esql.query.result_truncation_max_size",
        10000,
        1,
        1000000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> QUERY_RESULT_TRUNCATION_DEFAULT_SIZE = Setting.intSetting(
        "esql.query.result_truncation_default_size",
        1000,
        1,
        10000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> QUERY_ALLOW_PARTIAL_RESULTS = Setting.boolSetting(
        "esql.query.allow_partial_results",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> ESQL_QUERYLOG_THRESHOLD_WARN_SETTING = Setting.timeSetting(
        "esql.querylog.threshold.warn",
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(Integer.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> ESQL_QUERYLOG_THRESHOLD_INFO_SETTING = Setting.timeSetting(
        "esql.querylog.threshold.info",
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(Integer.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING = Setting.timeSetting(
        "esql.querylog.threshold.debug",
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(Integer.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING = Setting.timeSetting(
        "esql.querylog.threshold.trace",
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(-1),
        TimeValue.timeValueMillis(Integer.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> ESQL_QUERYLOG_INCLUDE_USER_SETTING = Setting.boolSetting(
        "esql.querylog.include.user",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Tuning parameter for deciding when to use the "merge" stored field loader.
     * Think of it as "how similar to a sequential block of documents do I have to
     * be before I'll use the merge reader?" So a value of {@code 1} means I have to
     * be <strong>exactly</strong> a sequential block, like {@code 0, 1, 2, 3, .. 1299, 1300}.
     * A value of {@code .2} means we'll use the sequential reader even if we only
     * need one in ten documents.
     * <p>
     *     The default value of this was experimentally derived using a
     *     <a href="https://gist.github.com/nik9000/ac6857de10745aad210b6397915ff846">script</a>.
     *     And a little paranoia. A lower default value was looking good locally, but
     *     I'm concerned about the implications of effectively using this all the time.
     * </p>
     */
    public static final Setting<Double> STORED_FIELDS_SEQUENTIAL_PROPORTION = Setting.doubleSetting(
        "index.esql.stored_fields_sequential_proportion",
        0.20,
        0,
        1,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    public static final Setting<DataPartitioning> DEFAULT_DATA_PARTITIONING = Setting.enumSetting(
        DataPartitioning.class,
        "esql.default_data_partitioning",
        DataPartitioning.AUTO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    @Override
    public Collection<?> createComponents(PluginServices services) {
        CircuitBreaker circuitBreaker = services.indicesService().getBigArrays().breakerService().getBreaker("request");
        Objects.requireNonNull(circuitBreaker, "request circuit breaker wasn't set");
        Settings settings = services.clusterService().getSettings();
        ByteSizeValue maxPrimitiveArrayBlockSize = settings.getAsBytesSize(
            BlockFactory.MAX_BLOCK_PRIMITIVE_ARRAY_SIZE_SETTING,
            BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE
        );
        BigArrays bigArrays = services.indicesService().getBigArrays().withCircuitBreaking();
        var blockFactoryProvider = blockFactoryProvider(circuitBreaker, bigArrays, maxPrimitiveArrayBlockSize);
        setupSharedSecrets();
        return List.of(
            new PlanExecutor(
                new IndexResolver(services.client()),
                services.telemetryProvider().getMeterRegistry(),
                getLicenseState(),
                new EsqlQueryLog(services.clusterService().getClusterSettings(), services.slowLogFieldProvider())
            ),
            new ExchangeService(
                services.clusterService().getSettings(),
                services.threadPool(),
                ThreadPool.Names.SEARCH,
                blockFactoryProvider.blockFactory()
            ),
            blockFactoryProvider
        );
    }

    protected BlockFactoryProvider blockFactoryProvider(CircuitBreaker breaker, BigArrays bigArrays, ByteSizeValue maxPrimitiveArraySize) {
        return new BlockFactoryProvider(new BlockFactory(breaker, bigArrays, maxPrimitiveArraySize));
    }

    private void setupSharedSecrets() {
        try {
            // EsqlQueryRequestBuilder.<clinit> initializes the shared secret access
            MethodHandles.lookup().ensureInitialized(EsqlQueryRequestBuilder.class);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    // to be overriden by tests
    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    /**
     * The settings defined by the ESQL plugin.
     *
     * @return the settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            QUERY_RESULT_TRUNCATION_DEFAULT_SIZE,
            QUERY_RESULT_TRUNCATION_MAX_SIZE,
            QUERY_ALLOW_PARTIAL_RESULTS,
            ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING,
            ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING,
            ESQL_QUERYLOG_THRESHOLD_INFO_SETTING,
            ESQL_QUERYLOG_THRESHOLD_WARN_SETTING,
            ESQL_QUERYLOG_INCLUDE_USER_SETTING,
            STORED_FIELDS_SEQUENTIAL_PROPORTION,
            DEFAULT_DATA_PARTITIONING
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(EsqlQueryAction.INSTANCE, TransportEsqlQueryAction.class),
            new ActionHandler<>(EsqlAsyncGetResultAction.INSTANCE, TransportEsqlAsyncGetResultsAction.class),
            new ActionHandler<>(EsqlStatsAction.INSTANCE, TransportEsqlStatsAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.ESQL, EsqlUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.ESQL, EsqlInfoTransportAction.class),
            new ActionHandler<>(EsqlResolveFieldsAction.TYPE, EsqlResolveFieldsAction.class),
            new ActionHandler<>(EsqlSearchShardsAction.TYPE, EsqlSearchShardsAction.class),
            new ActionHandler<>(EsqlAsyncStopAction.INSTANCE, TransportEsqlAsyncStopAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(
            new RestEsqlQueryAction(),
            new RestEsqlAsyncQueryAction(),
            new RestEsqlGetAsyncResultAction(),
            new RestEsqlStopAsyncAction(),
            new RestEsqlDeleteAsyncResultAction()
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(DriverStatus.ENTRY);
        entries.add(AbstractPageMappingOperator.Status.ENTRY);
        entries.add(AbstractPageMappingToIteratorOperator.Status.ENTRY);
        entries.add(AggregationOperator.Status.ENTRY);
        entries.add(ExchangeSinkOperator.Status.ENTRY);
        entries.add(ExchangeSourceOperator.Status.ENTRY);
        entries.add(HashAggregationOperator.Status.ENTRY);
        entries.add(LimitOperator.Status.ENTRY);
        entries.add(LuceneOperator.Status.ENTRY);
        entries.add(TopNOperatorStatus.ENTRY);
        entries.add(MvExpandOperator.Status.ENTRY);
        entries.add(ValuesSourceReaderOperator.Status.ENTRY);
        entries.add(SingleValueQuery.ENTRY);
        entries.add(AsyncOperator.Status.ENTRY);
        entries.add(EnrichLookupOperator.Status.ENTRY);
        entries.add(LookupFromIndexOperator.Status.ENTRY);

        entries.addAll(ExpressionWritables.getNamedWriteables());
        entries.addAll(PlanWritables.getNamedWriteables());
        return entries;
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final int allocatedProcessors = EsExecutors.allocatedProcessors(settings);
        return List.of(
            // TODO: Maybe have two types of threadpools for workers: one for CPU-bound and one for I/O-bound tasks.
            // And we should also reduce the number of threads of the CPU-bound threadpool to allocatedProcessors.
            new FixedExecutorBuilder(
                settings,
                ESQL_WORKER_THREAD_POOL_NAME,
                ThreadPool.searchOrGetThreadPoolSize(allocatedProcessors),
                1000,
                ESQL_WORKER_THREAD_POOL_NAME,
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }
}
