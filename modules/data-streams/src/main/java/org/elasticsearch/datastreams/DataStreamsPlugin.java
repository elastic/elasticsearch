/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DataStreamsStatsAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamMappingsAction;
import org.elasticsearch.action.datastreams.GetDataStreamSettingsAction;
import org.elasticsearch.action.datastreams.MigrateToDataStreamAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.datastreams.PastTimeSeriesIndexCreationAction;
import org.elasticsearch.action.datastreams.PromoteDataStreamAction;
import org.elasticsearch.action.datastreams.PutDataStreamOptionsAction;
import org.elasticsearch.action.datastreams.UpdateDataStreamMappingsAction;
import org.elasticsearch.action.datastreams.UpdateDataStreamSettingsAction;
import org.elasticsearch.action.datastreams.lifecycle.ExplainDataStreamLifecycleAction;
import org.elasticsearch.action.datastreams.lifecycle.GetDataStreamLifecycleAction;
import org.elasticsearch.action.datastreams.lifecycle.PutDataStreamLifecycleAction;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.action.TransportCreateDataStreamAction;
import org.elasticsearch.datastreams.action.TransportDataStreamsStatsAction;
import org.elasticsearch.datastreams.action.TransportDeleteDataStreamAction;
import org.elasticsearch.datastreams.action.TransportGetDataStreamMappingsAction;
import org.elasticsearch.datastreams.action.TransportGetDataStreamSettingsAction;
import org.elasticsearch.datastreams.action.TransportGetDataStreamsAction;
import org.elasticsearch.datastreams.action.TransportMigrateToDataStreamAction;
import org.elasticsearch.datastreams.action.TransportModifyDataStreamsAction;
import org.elasticsearch.datastreams.action.TransportPastTimeSeriesIndexCreationAction;
import org.elasticsearch.datastreams.action.TransportPromoteDataStreamAction;
import org.elasticsearch.datastreams.action.TransportUpdateDataStreamMappingsAction;
import org.elasticsearch.datastreams.action.TransportUpdateDataStreamSettingsAction;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDestinationLifecycle;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsIndexingListener;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsService;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsShardEventListener;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsShutdownListener;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsTemplateRegistry;
import org.elasticsearch.datastreams.derivedmetrics.action.GetDerivedMetricsStatsAction;
import org.elasticsearch.datastreams.derivedmetrics.action.TransportGetDerivedMetricsStatsAction;
import org.elasticsearch.datastreams.derivedmetrics.rest.RestDerivedMetricsStatsAction;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.datastreams.lifecycle.action.DeleteDataStreamLifecycleAction;
import org.elasticsearch.datastreams.lifecycle.action.GetDataStreamLifecycleStatsAction;
import org.elasticsearch.datastreams.lifecycle.action.TransportDeleteDataStreamLifecycleAction;
import org.elasticsearch.datastreams.lifecycle.action.TransportExplainDataStreamLifecycleAction;
import org.elasticsearch.datastreams.lifecycle.action.TransportGetDataStreamLifecycleAction;
import org.elasticsearch.datastreams.lifecycle.action.TransportGetDataStreamLifecycleStatsAction;
import org.elasticsearch.datastreams.lifecycle.action.TransportPutDataStreamLifecycleAction;
import org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthIndicatorService;
import org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthInfoPublisher;
import org.elasticsearch.datastreams.lifecycle.rest.RestDataStreamLifecycleStatsAction;
import org.elasticsearch.datastreams.lifecycle.rest.RestDeleteDataStreamLifecycleAction;
import org.elasticsearch.datastreams.lifecycle.rest.RestExplainDataStreamLifecycleAction;
import org.elasticsearch.datastreams.lifecycle.rest.RestGetDataStreamLifecycleAction;
import org.elasticsearch.datastreams.lifecycle.rest.RestPutDataStreamLifecycleAction;
import org.elasticsearch.datastreams.lifecycle.transitions.steps.MarkIndexForDLMForceMergeAction;
import org.elasticsearch.datastreams.lifecycle.transitions.steps.TransportMarkIndexForDLMForceMergeAction;
import org.elasticsearch.datastreams.options.action.DeleteDataStreamOptionsAction;
import org.elasticsearch.datastreams.options.action.GetDataStreamOptionsAction;
import org.elasticsearch.datastreams.options.action.TransportDeleteDataStreamOptionsAction;
import org.elasticsearch.datastreams.options.action.TransportGetDataStreamOptionsAction;
import org.elasticsearch.datastreams.options.action.TransportPutDataStreamOptionsAction;
import org.elasticsearch.datastreams.options.rest.RestDeleteDataStreamOptionsAction;
import org.elasticsearch.datastreams.options.rest.RestGetDataStreamOptionsAction;
import org.elasticsearch.datastreams.options.rest.RestPutDataStreamOptionsAction;
import org.elasticsearch.datastreams.rest.RestCreateDataStreamAction;
import org.elasticsearch.datastreams.rest.RestDataStreamsStatsAction;
import org.elasticsearch.datastreams.rest.RestDeleteDataStreamAction;
import org.elasticsearch.datastreams.rest.RestGetDataStreamMappingsAction;
import org.elasticsearch.datastreams.rest.RestGetDataStreamSettingsAction;
import org.elasticsearch.datastreams.rest.RestGetDataStreamsAction;
import org.elasticsearch.datastreams.rest.RestMigrateToDataStreamAction;
import org.elasticsearch.datastreams.rest.RestModifyDataStreamsAction;
import org.elasticsearch.datastreams.rest.RestPromoteDataStreamAction;
import org.elasticsearch.datastreams.rest.RestUpdateDataStreamMappingsAction;
import org.elasticsearch.datastreams.rest.RestUpdateDataStreamSettingsAction;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.index.ES95CodecClusterSettingProvider;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.CircuitBreakerPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.DATA_STREAM_LIFECYCLE_ORIGIN;

public class DataStreamsPlugin extends Plugin implements ActionPlugin, ExtensiblePlugin, HealthPlugin, CircuitBreakerPlugin {

    public static final int TIME_SERIES_POLL_INTERVAL_DEFAULT = 3;
    public static final Setting<TimeValue> TIME_SERIES_POLL_INTERVAL = Setting.timeSetting(
        "time_series.poll_interval",
        TimeValue.timeValueMinutes(TIME_SERIES_POLL_INTERVAL_DEFAULT),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueMinutes(10),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final TimeValue MAX_LOOK_AHEAD_TIME = TimeValue.timeValueHours(2);
    public static final int LOOK_AHEAD_TIME_DEFAULT = 9;
    public static final Setting<TimeValue> LOOK_AHEAD_TIME = Setting.timeSetting(
        "index.look_ahead_time",
        TimeValue.timeValueMinutes(LOOK_AHEAD_TIME_DEFAULT),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueDays(7), // is effectively 2h now.
        Setting.Property.IndexScope,
        Setting.Property.Dynamic,
        Setting.Property.ServerlessPublic
    );

    /**
     * Returns the look ahead time and lowers it when it to 2 hours if it is configured to more than 2 hours.
     */
    public static TimeValue getLookAheadTime(Settings settings) {
        TimeValue lookAheadTime = DataStreamsPlugin.LOOK_AHEAD_TIME.get(settings);
        if (lookAheadTime.compareTo(DataStreamsPlugin.MAX_LOOK_AHEAD_TIME) > 0) {
            lookAheadTime = DataStreamsPlugin.MAX_LOOK_AHEAD_TIME;
        }
        return lookAheadTime;
    }

    public static final String LIFECYCLE_CUSTOM_INDEX_METADATA_KEY = "data_stream_lifecycle";
    public static final Setting<TimeValue> LOOK_BACK_TIME = Setting.timeSetting(
        "index.look_back_time",
        TimeValue.timeValueHours(2),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueDays(7),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic,
        Setting.Property.ServerlessPublic
    );

    private final SetOnce<DataStreamLifecycleService> dataLifecycleInitialisationService = new SetOnce<>();
    private final SetOnce<DataStreamLifecycleHealthInfoPublisher> dataStreamLifecycleErrorsPublisher = new SetOnce<>();
    private final SetOnce<DataStreamLifecycleHealthIndicatorService> dataStreamLifecycleHealthIndicatorService = new SetOnce<>();
    private final SetOnce<ClusterService> clusterService = new SetOnce<>();
    private final SetOnce<DerivedMetricsService> derivedMetricsService = new SetOnce<>();
    private final SetOnce<DerivedMetricsTemplateRegistry> derivedMetricsTemplateRegistry = new SetOnce<>();
    private final SetOnce<DerivedMetricsShutdownListener> derivedMetricsShutdownListener = new SetOnce<>();
    private final SetOnce<DerivedMetricsDestinationLifecycle> derivedMetricsDestinationLifecycle = new SetOnce<>();
    private final SetOnce<CircuitBreaker> derivedMetricsBreaker = new SetOnce<>();
    private final Settings settings;
    private DownsamplingOperations downsamplingOperations = DownsamplingOperations.noop();

    public DataStreamsPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        List<DownsamplingOperations> extensions = loader.loadExtensions(DownsamplingOperations.class);
        if (extensions.size() > 1) {
            throw new IllegalStateException(
                "Expected at most one DownsamplingOperations implementation, found: " + extensions.stream().map(Object::getClass).toList()
            );
        }
        if (extensions.isEmpty() == false) {
            downsamplingOperations = extensions.get(0);
        }
    }

    protected Clock getClock() {
        return Clock.systemUTC();
    }

    // The dependency of index.look_ahead_time is a cluster setting and currently there is no clean validation approach for this:
    static void additionalLookAheadTimeValidation(TimeValue lookAhead, TimeValue timeSeriesPollInterval) {
        if (lookAhead.compareTo(timeSeriesPollInterval) < 0) {
            final String message = String.format(
                Locale.ROOT,
                "failed to parse value%s for setting [%s], must be lower than setting [%s] which is [%s]",
                " [" + lookAhead.getStringRep() + "]",
                LOOK_AHEAD_TIME.getKey(),
                TIME_SERIES_POLL_INTERVAL.getKey(),
                timeSeriesPollInterval.getStringRep()
            );
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> pluginSettings = new ArrayList<>();
        pluginSettings.add(TIME_SERIES_POLL_INTERVAL);
        pluginSettings.add(LOOK_AHEAD_TIME);
        pluginSettings.add(LOOK_BACK_TIME);
        pluginSettings.add(DataStreamIndexSettingsProvider.SUPPORT_SEQ_NO_DISABLED);
        pluginSettings.add(DataStreamIndexSettingsProvider.SUPPORT_SYNTHETIC_ID);
        pluginSettings.add(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL_SETTING);
        pluginSettings.add(DataStreamLifecycleService.DATA_STREAM_MERGE_POLICY_TARGET_FLOOR_SEGMENT_SETTING);
        pluginSettings.add(DataStreamLifecycleService.DATA_STREAM_MERGE_POLICY_TARGET_FACTOR_SETTING);
        pluginSettings.add(DataStreamLifecycleService.DLM_CREATED_SETTING);
        pluginSettings.add(DataStreamLifecycleService.DATA_STREAM_MAX_DOWNSAMPLING_INDICES_IN_PROGRESS_SETTING);
        pluginSettings.add(TransportPastTimeSeriesIndexCreationAction.PAST_TSDB_INDEX_INTERVAL);
        pluginSettings.add(DerivedMetricsService.FLUSH_INTERVAL);
        pluginSettings.add(DerivedMetricsService.FLUSH_GRACE_PERIOD);
        pluginSettings.add(DerivedMetricsService.MAX_SERIES_PER_NODE);
        pluginSettings.add(DerivedMetricsService.BULK_SIZE);
        pluginSettings.add(DerivedMetricsService.MAX_SERIES_PER_STREAM);
        pluginSettings.add(DerivedMetricsService.MAX_IN_FLIGHT_BULKS);
        pluginSettings.add(DerivedMetricsService.MEMORY_PRESSURE_POLICY);
        pluginSettings.add(DerivedMetricsService.HISTOGRAM_BUCKETS);
        pluginSettings.add(DerivedMetricsService.MAX_DIMENSION_CARDINALITY);
        pluginSettings.add(DerivedMetricsService.INDEXING_PRESSURE_CEILING);
        return pluginSettings;
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {

        Collection<Object> components = new ArrayList<>();
        clusterService.set(services.clusterService());
        var updateTimeSeriesRangeService = new UpdateTimeSeriesRangeService(
            services.environment().settings(),
            services.threadPool(),
            services.clusterService()
        );
        IndexScopedSettings indexScopedSettings = services.indicesService().getIndexScopedSettings();
        indexScopedSettings.addSettingsUpdateConsumer(LOOK_AHEAD_TIME, value -> {
            TimeValue timeSeriesPollInterval = updateTimeSeriesRangeService.pollInterval;
            additionalLookAheadTimeValidation(value, timeSeriesPollInterval);
        });
        components.add(updateTimeSeriesRangeService);
        dataStreamLifecycleErrorsPublisher.set(
            new DataStreamLifecycleHealthInfoPublisher(settings, services.client(), services.clusterService(), services.dlmErrorStore())
        );

        dataLifecycleInitialisationService.set(
            new DataStreamLifecycleService(
                settings,
                new OriginSettingClient(services.client(), DATA_STREAM_LIFECYCLE_ORIGIN),
                services.clusterService(),
                getClock(),
                services.threadPool(),
                services.threadPool()::absoluteTimeInMillis,
                services.dlmErrorStore(),
                services.allocationService(),
                dataStreamLifecycleErrorsPublisher.get(),
                services.dataStreamGlobalRetentionSettings(),
                downsamplingOperations
            )
        );
        dataLifecycleInitialisationService.get().init();
        dataStreamLifecycleHealthIndicatorService.set(new DataStreamLifecycleHealthIndicatorService(services.projectResolver()));

        components.add(dataLifecycleInitialisationService.get());
        components.add(dataStreamLifecycleErrorsPublisher.get());

        derivedMetricsService.set(
            new DerivedMetricsService(
                settings,
                services.client(),
                services.threadPool(),
                // Everything the buffer allocates has to land on the derived metrics breaker rather than the request breaker it would
                // otherwise share, which means a BigArrays bound to this breaker by name. The pages are long lived rather than
                // per-request, so giving up the recycler costs nothing.
                new BigArrays(null, services.bigArrays().breakerService(), DerivedMetricsService.BREAKER_NAME).withCircuitBreaking(),
                services.indexingPressure(),
                services.telemetryProvider().getMeterRegistry(),
                // The persistent node ID rather than the node name: node.name is typically the pod name in a containerised deployment
                // and changes on every restart, and this is a tsid dimension, so every rename would mint a fresh set of series.
                services.nodeEnvironment().nodeId(),
                services.clusterService().getNodeName()
            )
        );
        derivedMetricsService.get().init();
        derivedMetricsShutdownListener.set(new DerivedMetricsShutdownListener(services.clusterService(), derivedMetricsService.get()));
        derivedMetricsShutdownListener.get().init();
        derivedMetricsTemplateRegistry.set(new DerivedMetricsTemplateRegistry(services.client(), services.clusterService()));
        derivedMetricsTemplateRegistry.get().init();
        derivedMetricsDestinationLifecycle.set(
            new DerivedMetricsDestinationLifecycle(
                services.client(),
                services.clusterService(),
                services.dataStreamGlobalRetentionSettings()
            )
        );
        derivedMetricsDestinationLifecycle.get().init();
        components.add(derivedMetricsService.get());
        components.add(derivedMetricsTemplateRegistry.get());
        components.add(derivedMetricsDestinationLifecycle.get());
        return components;
    }

    /**
     * Derived metrics do their periodic flushing and all of their emission on their own pool.
     *
     * <p>Without one they would run on {@code management}, which is capped at five threads, has an unbounded queue that never rejects, and
     * carries dynamic mapping updates and cluster-info collection. A derived metrics flush storm there would delay work the cluster cannot
     * afford to have delayed, and would do it invisibly, since nothing would ever be shed.
     *
     * <p>Small and bounded on purpose: this is background work that should be shed rather than queued when it cannot keep up, and the
     * shedding is counted. Operators can resize it through {@code data_streams.derived_metrics.thread_pool}.
     */
    public static final String DERIVED_METRICS_THREAD_POOL = "derived_metrics";
    private static final int DERIVED_METRICS_THREAD_POOL_QUEUE_SIZE = 128;

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(
            new FixedExecutorBuilder(
                settings,
                DERIVED_METRICS_THREAD_POOL,
                ThreadPool.oneEighthAllocatedProcessors(EsExecutors.allocatedProcessors(settings)),
                DERIVED_METRICS_THREAD_POOL_QUEUE_SIZE,
                "data_streams.derived_metrics.thread_pool",
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            )
        );
    }

    /**
     * Derived metrics buffer node-local state whose size is driven by dimension cardinality, so it gets its own breaker rather than
     * being invisible. Everything the buffer allocates goes through BigArrays against this breaker, which makes it both bounded and
     * reportable through {@code _nodes/stats/breakers}.
     */
    @Override
    public BreakerSettings getCircuitBreaker(Settings settings) {
        return BreakerSettings.updateFromSettings(
            new BreakerSettings(
                DerivedMetricsService.BREAKER_NAME,
                DerivedMetricsService.defaultBreakerLimit(),
                DerivedMetricsService.DEFAULT_BREAKER_OVERHEAD,
                CircuitBreaker.Type.MEMORY,
                CircuitBreaker.Durability.TRANSIENT
            ),
            settings
        );
    }

    @Override
    public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
        assert circuitBreaker.getName().equals(DerivedMetricsService.BREAKER_NAME);
        derivedMetricsBreaker.set(circuitBreaker);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        DerivedMetricsService service = derivedMetricsService.get();
        if (service != null) {
            // The mapping decides whether a configured path can be read from the already-parsed document rather than from _source, and
            // IndexModule cannot hand out a MapperService here. The shard listener fills this in once a shard of the index exists.
            AtomicReference<MapperService> mappers = new AtomicReference<>();
            indexModule.addIndexOperationListener(
                new DerivedMetricsIndexingListener(clusterService.get(), service, indexModule.getIndex(), mappers::get)
            );
            // flush what a shard collected before it leaves this node, so an avoidable loss is avoided
            indexModule.addIndexEventListener(new DerivedMetricsShardEventListener(service, mappers));
        }
    }

    @Override
    public List<ActionHandler> getActions() {
        List<ActionHandler> actions = new ArrayList<>();
        actions.add(new ActionHandler(PastTimeSeriesIndexCreationAction.INSTANCE, TransportPastTimeSeriesIndexCreationAction.class));
        actions.add(new ActionHandler(CreateDataStreamAction.INSTANCE, TransportCreateDataStreamAction.class));
        actions.add(new ActionHandler(DeleteDataStreamAction.INSTANCE, TransportDeleteDataStreamAction.class));
        actions.add(new ActionHandler(GetDataStreamAction.INSTANCE, TransportGetDataStreamsAction.class));
        actions.add(new ActionHandler(DataStreamsStatsAction.INSTANCE, TransportDataStreamsStatsAction.class));
        actions.add(new ActionHandler(MigrateToDataStreamAction.INSTANCE, TransportMigrateToDataStreamAction.class));
        actions.add(new ActionHandler(PromoteDataStreamAction.INSTANCE, TransportPromoteDataStreamAction.class));
        actions.add(new ActionHandler(ModifyDataStreamsAction.INSTANCE, TransportModifyDataStreamsAction.class));
        actions.add(new ActionHandler(PutDataStreamLifecycleAction.INSTANCE, TransportPutDataStreamLifecycleAction.class));
        actions.add(new ActionHandler(GetDataStreamLifecycleAction.INSTANCE, TransportGetDataStreamLifecycleAction.class));
        actions.add(new ActionHandler(DeleteDataStreamLifecycleAction.INSTANCE, TransportDeleteDataStreamLifecycleAction.class));
        actions.add(new ActionHandler(ExplainDataStreamLifecycleAction.INSTANCE, TransportExplainDataStreamLifecycleAction.class));
        actions.add(new ActionHandler(GetDataStreamLifecycleStatsAction.INSTANCE, TransportGetDataStreamLifecycleStatsAction.class));
        actions.add(new ActionHandler(GetDataStreamOptionsAction.INSTANCE, TransportGetDataStreamOptionsAction.class));
        actions.add(new ActionHandler(PutDataStreamOptionsAction.INSTANCE, TransportPutDataStreamOptionsAction.class));
        actions.add(new ActionHandler(DeleteDataStreamOptionsAction.INSTANCE, TransportDeleteDataStreamOptionsAction.class));
        actions.add(new ActionHandler(GetDataStreamSettingsAction.INSTANCE, TransportGetDataStreamSettingsAction.class));
        actions.add(new ActionHandler(UpdateDataStreamSettingsAction.INSTANCE, TransportUpdateDataStreamSettingsAction.class));
        actions.add(new ActionHandler(GetDataStreamMappingsAction.INSTANCE, TransportGetDataStreamMappingsAction.class));
        actions.add(new ActionHandler(UpdateDataStreamMappingsAction.INSTANCE, TransportUpdateDataStreamMappingsAction.class));
        actions.add(new ActionHandler(MarkIndexForDLMForceMergeAction.TYPE, TransportMarkIndexForDLMForceMergeAction.class));
        actions.add(new ActionHandler(GetDerivedMetricsStatsAction.INSTANCE, TransportGetDerivedMetricsStatsAction.class));
        return actions;
    }

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        List<RestHandler> handlers = new ArrayList<>();
        handlers.add(new RestCreateDataStreamAction());
        handlers.add(new RestDeleteDataStreamAction());
        handlers.add(new RestGetDataStreamsAction());
        handlers.add(new RestDataStreamsStatsAction());
        handlers.add(new RestMigrateToDataStreamAction());
        handlers.add(new RestPromoteDataStreamAction());
        handlers.add(new RestModifyDataStreamsAction(clusterSupportsFeature));
        handlers.add(new RestPutDataStreamLifecycleAction());
        handlers.add(new RestGetDataStreamLifecycleAction());
        handlers.add(new RestDeleteDataStreamLifecycleAction());
        handlers.add(new RestExplainDataStreamLifecycleAction());
        handlers.add(new RestDataStreamLifecycleStatsAction());
        handlers.add(new RestGetDataStreamOptionsAction());
        handlers.add(new RestPutDataStreamOptionsAction());
        handlers.add(new RestDeleteDataStreamOptionsAction());
        handlers.add(new RestGetDataStreamSettingsAction());
        handlers.add(new RestUpdateDataStreamSettingsAction());
        handlers.add(new RestGetDataStreamMappingsAction());
        handlers.add(new RestUpdateDataStreamMappingsAction());
        handlers.add(new RestDerivedMetricsStatsAction());
        return handlers;
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        return List.of(
            new DataStreamIndexSettingsProvider(parameters.mapperServiceFactory(), settings),
            new ES95CodecClusterSettingProvider(parameters.clusterService().getClusterSettings())
        );
    }

    @Override
    public void close() throws IOException {
        DerivedMetricsTemplateRegistry templateRegistry = derivedMetricsTemplateRegistry.get();
        if (templateRegistry != null) {
            templateRegistry.close();
        }
        DerivedMetricsDestinationLifecycle destinationLifecycle = derivedMetricsDestinationLifecycle.get();
        if (destinationLifecycle != null) {
            destinationLifecycle.close();
        }
        DerivedMetricsShutdownListener shutdownListener = derivedMetricsShutdownListener.get();
        if (shutdownListener != null) {
            shutdownListener.close();
        }
        try {
            IOUtils.close(dataLifecycleInitialisationService.get(), derivedMetricsService.get());
        } catch (IOException e) {
            throw new ElasticsearchException("unable to close the data stream lifecycle service", e);
        }
    }

    @Override
    public Collection<HealthIndicatorService> getHealthIndicatorServices() {
        return List.of(dataStreamLifecycleHealthIndicatorService.get());
    }
}
