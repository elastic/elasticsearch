/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersAction;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.RollupILMAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.ilm.UnfollowAction;
import org.elasticsearch.xpack.core.ilm.WaitForSnapshotAction;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction;
import org.elasticsearch.xpack.core.ilm.action.MoveToStepAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.core.ilm.action.RetryAction;
import org.elasticsearch.xpack.core.ilm.action.StartILMAction;
import org.elasticsearch.xpack.core.ilm.action.StopILMAction;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.action.DeleteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotRetentionAction;
import org.elasticsearch.xpack.core.slm.action.GetSLMStatusAction;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleStatsAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.StartSLMAction;
import org.elasticsearch.xpack.core.slm.action.StopSLMAction;
import org.elasticsearch.xpack.ilm.action.ReservedLifecycleAction;
import org.elasticsearch.xpack.ilm.action.RestDeleteLifecycleAction;
import org.elasticsearch.xpack.ilm.action.RestExplainLifecycleAction;
import org.elasticsearch.xpack.ilm.action.RestGetLifecycleAction;
import org.elasticsearch.xpack.ilm.action.RestGetStatusAction;
import org.elasticsearch.xpack.ilm.action.RestMigrateToDataTiersAction;
import org.elasticsearch.xpack.ilm.action.RestMoveToStepAction;
import org.elasticsearch.xpack.ilm.action.RestPutLifecycleAction;
import org.elasticsearch.xpack.ilm.action.RestRemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.ilm.action.RestRetryAction;
import org.elasticsearch.xpack.ilm.action.RestStartILMAction;
import org.elasticsearch.xpack.ilm.action.RestStopAction;
import org.elasticsearch.xpack.ilm.action.TransportDeleteLifecycleAction;
import org.elasticsearch.xpack.ilm.action.TransportExplainLifecycleAction;
import org.elasticsearch.xpack.ilm.action.TransportGetLifecycleAction;
import org.elasticsearch.xpack.ilm.action.TransportGetStatusAction;
import org.elasticsearch.xpack.ilm.action.TransportMigrateToDataTiersAction;
import org.elasticsearch.xpack.ilm.action.TransportMoveToStepAction;
import org.elasticsearch.xpack.ilm.action.TransportPutLifecycleAction;
import org.elasticsearch.xpack.ilm.action.TransportRemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.ilm.action.TransportRetryAction;
import org.elasticsearch.xpack.ilm.action.TransportStartILMAction;
import org.elasticsearch.xpack.ilm.action.TransportStopILMAction;
import org.elasticsearch.xpack.ilm.history.ILMHistoryStore;
import org.elasticsearch.xpack.ilm.history.ILMHistoryTemplateRegistry;
import org.elasticsearch.xpack.slm.SLMInfoTransportAction;
import org.elasticsearch.xpack.slm.SLMUsageTransportAction;
import org.elasticsearch.xpack.slm.SlmHealthIndicatorService;
import org.elasticsearch.xpack.slm.SnapshotLifecycleService;
import org.elasticsearch.xpack.slm.SnapshotLifecycleTask;
import org.elasticsearch.xpack.slm.SnapshotRetentionService;
import org.elasticsearch.xpack.slm.SnapshotRetentionTask;
import org.elasticsearch.xpack.slm.action.RestDeleteSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.RestExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.RestExecuteSnapshotRetentionAction;
import org.elasticsearch.xpack.slm.action.RestGetSLMStatusAction;
import org.elasticsearch.xpack.slm.action.RestGetSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.RestGetSnapshotLifecycleStatsAction;
import org.elasticsearch.xpack.slm.action.RestPutSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.RestStartSLMAction;
import org.elasticsearch.xpack.slm.action.RestStopSLMAction;
import org.elasticsearch.xpack.slm.action.TransportDeleteSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.TransportExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.TransportExecuteSnapshotRetentionAction;
import org.elasticsearch.xpack.slm.action.TransportGetSLMStatusAction;
import org.elasticsearch.xpack.slm.action.TransportGetSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.TransportGetSnapshotLifecycleStatsAction;
import org.elasticsearch.xpack.slm.action.TransportPutSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.TransportStartSLMAction;
import org.elasticsearch.xpack.slm.action.TransportStopSLMAction;
import org.elasticsearch.xpack.slm.history.SnapshotHistoryStore;
import org.elasticsearch.xpack.slm.history.SnapshotLifecycleTemplateRegistry;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;

public class IndexLifecycle extends Plugin implements ActionPlugin, HealthPlugin {

    public static final List<NamedXContentRegistry.Entry> NAMED_X_CONTENT_ENTRIES = xContentEntries();

    private final SetOnce<IndexLifecycleService> indexLifecycleInitialisationService = new SetOnce<>();
    private final SetOnce<ILMHistoryStore> ilmHistoryStore = new SetOnce<>();
    private final SetOnce<SnapshotLifecycleService> snapshotLifecycleService = new SetOnce<>();
    private final SetOnce<SnapshotRetentionService> snapshotRetentionService = new SetOnce<>();
    private final SetOnce<SnapshotHistoryStore> snapshotHistoryStore = new SetOnce<>();
    private final SetOnce<IlmHealthIndicatorService> ilmHealthIndicatorService = new SetOnce<>();
    private final SetOnce<SlmHealthIndicatorService> slmHealthIndicatorService = new SetOnce<>();
    private final SetOnce<ReservedLifecycleAction> reservedLifecycleAction = new SetOnce<>();
    private final Settings settings;

    public IndexLifecycle(Settings settings) {
        this.settings = settings;
    }

    // overridable by tests
    protected Clock getClock() {
        return Clock.systemUTC();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING,
            LifecycleSettings.LIFECYCLE_NAME_SETTING,
            LifecycleSettings.LIFECYCLE_ORIGINATION_DATE_SETTING,
            LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE_SETTING,
            LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING,
            LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING,
            LifecycleSettings.LIFECYCLE_STEP_MASTER_TIMEOUT_SETTING,
            LifecycleSettings.LIFECYCLE_STEP_WAIT_TIME_THRESHOLD_SETTING,
            RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING,
            LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING,
            LifecycleSettings.SLM_RETENTION_SCHEDULE_SETTING,
            LifecycleSettings.SLM_RETENTION_DURATION_SETTING,
            LifecycleSettings.SLM_MINIMUM_INTERVAL_SETTING,
            LifecycleSettings.SLM_HEALTH_FAILED_SNAPSHOT_WARN_THRESHOLD_SETTING
        );
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
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
        IndexNameExpressionResolver expressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer
    ) {
        final List<Object> components = new ArrayList<>();
        ILMHistoryTemplateRegistry ilmTemplateRegistry = new ILMHistoryTemplateRegistry(
            settings,
            clusterService,
            threadPool,
            client,
            xContentRegistry
        );
        ilmTemplateRegistry.initialize();
        ilmHistoryStore.set(new ILMHistoryStore(new OriginSettingClient(client, INDEX_LIFECYCLE_ORIGIN), clusterService, threadPool));
        /*
         * Here we use threadPool::absoluteTimeInMillis rather than System::currentTimeInMillis because snapshot start time is set using
         * ThreadPool.absoluteTimeInMillis(). ThreadPool.absoluteTimeInMillis() returns a cached time that can be several hundred
         * milliseconds behind System.currentTimeMillis(). The result is that a snapshot taken after a policy is created can have a start
         * time that is before the policy's (or action's) start time if System::currentTimeInMillis is used here.
         */
        LongSupplier nowSupplier = threadPool::absoluteTimeInMillis;
        indexLifecycleInitialisationService.set(
            new IndexLifecycleService(
                settings,
                client,
                clusterService,
                threadPool,
                getClock(),
                nowSupplier,
                xContentRegistry,
                ilmHistoryStore.get(),
                getLicenseState()
            )
        );
        components.add(indexLifecycleInitialisationService.get());

        SnapshotLifecycleTemplateRegistry templateRegistry = new SnapshotLifecycleTemplateRegistry(
            settings,
            clusterService,
            threadPool,
            client,
            xContentRegistry
        );
        templateRegistry.initialize();
        snapshotHistoryStore.set(new SnapshotHistoryStore(new OriginSettingClient(client, INDEX_LIFECYCLE_ORIGIN), clusterService));
        snapshotLifecycleService.set(
            new SnapshotLifecycleService(
                settings,
                () -> new SnapshotLifecycleTask(client, clusterService, snapshotHistoryStore.get()),
                clusterService,
                getClock()
            )
        );
        snapshotLifecycleService.get().init();
        snapshotRetentionService.set(
            new SnapshotRetentionService(
                settings,
                () -> new SnapshotRetentionTask(client, clusterService, System::nanoTime, snapshotHistoryStore.get()),
                getClock()
            )
        );
        snapshotRetentionService.get().init(clusterService);
        components.addAll(Arrays.asList(snapshotLifecycleService.get(), snapshotHistoryStore.get(), snapshotRetentionService.get()));
        ilmHealthIndicatorService.set(new IlmHealthIndicatorService(clusterService));
        slmHealthIndicatorService.set(new SlmHealthIndicatorService(clusterService));
        reservedLifecycleAction.set(new ReservedLifecycleAction(xContentRegistry, client, XPackPlugin.getSharedLicenseState()));

        return components;
    }

    @Override
    public List<Entry> getNamedWriteables() {
        return Collections.emptyList();
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return NAMED_X_CONTENT_ENTRIES;
    }

    private static List<NamedXContentRegistry.Entry> xContentEntries() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(
            Arrays.asList(
                // Custom Metadata
                new NamedXContentRegistry.Entry(
                    Metadata.Custom.class,
                    new ParseField(IndexLifecycleMetadata.TYPE),
                    parser -> IndexLifecycleMetadata.PARSER.parse(parser, null)
                ),
                new NamedXContentRegistry.Entry(
                    Metadata.Custom.class,
                    new ParseField(SnapshotLifecycleMetadata.TYPE),
                    parser -> SnapshotLifecycleMetadata.PARSER.parse(parser, null)
                ),
                // Lifecycle Types
                new NamedXContentRegistry.Entry(
                    LifecycleType.class,
                    new ParseField(TimeseriesLifecycleType.TYPE),
                    (p, c) -> TimeseriesLifecycleType.INSTANCE
                ),
                // Lifecycle Actions
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(FreezeAction.NAME), FreezeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SetPriorityAction.NAME), SetPriorityAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(UnfollowAction.NAME), UnfollowAction::parse),
                new NamedXContentRegistry.Entry(
                    LifecycleAction.class,
                    new ParseField(WaitForSnapshotAction.NAME),
                    WaitForSnapshotAction::parse
                ),
                new NamedXContentRegistry.Entry(
                    LifecycleAction.class,
                    new ParseField(SearchableSnapshotAction.NAME),
                    SearchableSnapshotAction::parse
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MigrateAction.NAME), MigrateAction::parse)
            )
        );

        // TSDB Downsampling / Rollup
        if (IndexSettings.isTimeSeriesModeEnabled()) {
            entries.add(
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RollupILMAction.NAME), RollupILMAction::parse)
            );
        }
        return List.copyOf(entries);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings unused,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        List<RestHandler> handlers = new ArrayList<>();

        handlers.addAll(
            Arrays.asList(
                // add ILM rest handlers
                new RestPutLifecycleAction(),
                new RestGetLifecycleAction(),
                new RestDeleteLifecycleAction(),
                new RestExplainLifecycleAction(),
                new RestRemoveIndexLifecyclePolicyAction(),
                new RestMoveToStepAction(),
                new RestRetryAction(),
                new RestStopAction(),
                new RestStartILMAction(),
                new RestGetStatusAction(),
                new RestMigrateToDataTiersAction(),

                // add SLM rest headers
                new RestPutSnapshotLifecycleAction(),
                new RestDeleteSnapshotLifecycleAction(),
                new RestGetSnapshotLifecycleAction(),
                new RestExecuteSnapshotLifecycleAction(),
                new RestGetSnapshotLifecycleStatsAction(),
                new RestExecuteSnapshotRetentionAction(),
                new RestStopSLMAction(),
                new RestStartSLMAction(),
                new RestGetSLMStatusAction()
            )
        );
        return handlers;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var ilmUsageAction = new ActionHandler<>(XPackUsageFeatureAction.INDEX_LIFECYCLE, IndexLifecycleUsageTransportAction.class);
        var ilmInfoAction = new ActionHandler<>(XPackInfoFeatureAction.INDEX_LIFECYCLE, IndexLifecycleInfoTransportAction.class);
        var slmUsageAction = new ActionHandler<>(XPackUsageFeatureAction.SNAPSHOT_LIFECYCLE, SLMUsageTransportAction.class);
        var slmInfoAction = new ActionHandler<>(XPackInfoFeatureAction.SNAPSHOT_LIFECYCLE, SLMInfoTransportAction.class);
        var migrateToDataTiersAction = new ActionHandler<>(MigrateToDataTiersAction.INSTANCE, TransportMigrateToDataTiersAction.class);
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(ilmUsageAction);
        actions.add(ilmInfoAction);
        actions.add(slmUsageAction);
        actions.add(slmInfoAction);
        actions.add(migrateToDataTiersAction);
        actions.addAll(
            Arrays.asList(
                // add ILM actions
                new ActionHandler<>(PutLifecycleAction.INSTANCE, TransportPutLifecycleAction.class),
                new ActionHandler<>(GetLifecycleAction.INSTANCE, TransportGetLifecycleAction.class),
                new ActionHandler<>(DeleteLifecycleAction.INSTANCE, TransportDeleteLifecycleAction.class),
                new ActionHandler<>(ExplainLifecycleAction.INSTANCE, TransportExplainLifecycleAction.class),
                new ActionHandler<>(RemoveIndexLifecyclePolicyAction.INSTANCE, TransportRemoveIndexLifecyclePolicyAction.class),
                new ActionHandler<>(MoveToStepAction.INSTANCE, TransportMoveToStepAction.class),
                new ActionHandler<>(RetryAction.INSTANCE, TransportRetryAction.class),
                new ActionHandler<>(StartILMAction.INSTANCE, TransportStartILMAction.class),
                new ActionHandler<>(StopILMAction.INSTANCE, TransportStopILMAction.class),
                new ActionHandler<>(GetStatusAction.INSTANCE, TransportGetStatusAction.class),

                // add SLM actions
                new ActionHandler<>(PutSnapshotLifecycleAction.INSTANCE, TransportPutSnapshotLifecycleAction.class),
                new ActionHandler<>(DeleteSnapshotLifecycleAction.INSTANCE, TransportDeleteSnapshotLifecycleAction.class),
                new ActionHandler<>(GetSnapshotLifecycleAction.INSTANCE, TransportGetSnapshotLifecycleAction.class),
                new ActionHandler<>(ExecuteSnapshotLifecycleAction.INSTANCE, TransportExecuteSnapshotLifecycleAction.class),
                new ActionHandler<>(GetSnapshotLifecycleStatsAction.INSTANCE, TransportGetSnapshotLifecycleStatsAction.class),
                new ActionHandler<>(ExecuteSnapshotRetentionAction.INSTANCE, TransportExecuteSnapshotRetentionAction.class),
                new ActionHandler<>(StartSLMAction.INSTANCE, TransportStartSLMAction.class),
                new ActionHandler<>(StopSLMAction.INSTANCE, TransportStopSLMAction.class),
                new ActionHandler<>(GetSLMStatusAction.INSTANCE, TransportGetSLMStatusAction.class)
            )
        );
        return actions;
    }

    List<ReservedClusterStateHandler<?>> reservedClusterStateHandlers() {
        return List.of(reservedLifecycleAction.get());
    }

    @Override
    public Collection<HealthIndicatorService> getHealthIndicatorServices() {
        return List.of(ilmHealthIndicatorService.get(), slmHealthIndicatorService.get());
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        assert indexLifecycleInitialisationService.get() != null;
        indexModule.addIndexEventListener(indexLifecycleInitialisationService.get());
    }

    @Override
    public void close() {
        try {
            IOUtils.close(
                indexLifecycleInitialisationService.get(),
                ilmHistoryStore.get(),
                snapshotLifecycleService.get(),
                snapshotRetentionService.get()
            );
        } catch (IOException e) {
            throw new ElasticsearchException("unable to close index lifecycle services", e);
        }
    }
}
