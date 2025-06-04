/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.ReservedProjectStateHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersAction;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
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
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction;
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

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;

public class IndexLifecycle extends Plugin implements ActionPlugin, HealthPlugin {

    public static final List<NamedXContentRegistry.Entry> NAMED_X_CONTENT_ENTRIES = xContentEntries();

    private final SetOnce<IndexLifecycleService> indexLifecycleInitialisationService = new SetOnce<>();
    private final SetOnce<ILMHistoryStore> ilmHistoryStore = new SetOnce<>();
    private final SetOnce<IlmHealthIndicatorService> ilmHealthIndicatorService = new SetOnce<>();
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
        return List.of(
            LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING,
            LifecycleSettings.LIFECYCLE_NAME_SETTING,
            LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING,
            LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING,
            LifecycleSettings.LIFECYCLE_STEP_MASTER_TIMEOUT_SETTING,
            LifecycleSettings.LIFECYCLE_STEP_WAIT_TIME_THRESHOLD_SETTING,
            LifecycleSettings.LIFECYCLE_ROLLOVER_ONLY_IF_HAS_DOCUMENTS_SETTING,
            LifecycleSettings.LIFECYCLE_SKIP_SETTING,
            RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING,
            IlmHealthIndicatorService.MAX_TIME_ON_ACTION_SETTING,
            IlmHealthIndicatorService.MAX_TIME_ON_STEP_SETTING,
            IlmHealthIndicatorService.MAX_RETRIES_PER_STEP_SETTING
        );
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        final List<Object> components = new ArrayList<>();
        ILMHistoryTemplateRegistry ilmTemplateRegistry = new ILMHistoryTemplateRegistry(
            settings,
            services.clusterService(),
            services.threadPool(),
            services.client(),
            services.xContentRegistry(),
            services.projectResolver()
        );
        ilmTemplateRegistry.initialize();
        ilmHistoryStore.set(
            new ILMHistoryStore(
                new OriginSettingClient(services.client(), INDEX_LIFECYCLE_ORIGIN),
                services.clusterService(),
                services.threadPool()
            )
        );
        /*
         * Here we use threadPool::absoluteTimeInMillis rather than System::currentTimeInMillis because snapshot start time is set using
         * ThreadPool.absoluteTimeInMillis(). ThreadPool.absoluteTimeInMillis() returns a cached time that can be several hundred
         * milliseconds behind System.currentTimeMillis(). The result is that a snapshot taken after a policy is created can have a start
         * time that is before the policy's (or action's) start time if System::currentTimeInMillis is used here.
         */
        LongSupplier nowSupplier = services.threadPool()::absoluteTimeInMillis;
        indexLifecycleInitialisationService.set(
            new IndexLifecycleService(
                settings,
                services.client(),
                services.clusterService(),
                services.threadPool(),
                getClock(),
                nowSupplier,
                services.xContentRegistry(),
                ilmHistoryStore.get(),
                getLicenseState()
            )
        );
        components.add(indexLifecycleInitialisationService.get());

        ilmHealthIndicatorService.set(
            new IlmHealthIndicatorService(
                services.clusterService(),
                new IlmHealthIndicatorService.StagnatingIndicesFinder(
                    services.clusterService(),
                    IlmHealthIndicatorService.RULES_BY_ACTION_CONFIG.values(),
                    System::currentTimeMillis
                )
            )
        );
        reservedLifecycleAction.set(
            new ReservedLifecycleAction(services.xContentRegistry(), services.client(), XPackPlugin.getSharedLicenseState())
        );

        return components;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return NAMED_X_CONTENT_ENTRIES;
    }

    private static List<NamedXContentRegistry.Entry> xContentEntries() {
        return List.of(
            // Custom Metadata
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(IndexLifecycleMetadata.TYPE),
                parser -> IndexLifecycleMetadata.PARSER.parse(parser, null)
            ),
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(LifecycleOperationMetadata.TYPE),
                parser -> LifecycleOperationMetadata.PARSER.parse(parser, null)
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
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MigrateAction.NAME), MigrateAction::parse),
            // TSDB Downsampling
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DownsampleAction.NAME), DownsampleAction::parse)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings unused,
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
            new RestMigrateToDataTiersAction()
        );
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(XPackUsageFeatureAction.INDEX_LIFECYCLE, IndexLifecycleUsageTransportAction.class),
            new ActionHandler(XPackInfoFeatureAction.INDEX_LIFECYCLE, IndexLifecycleInfoTransportAction.class),
            new ActionHandler(MigrateToDataTiersAction.INSTANCE, TransportMigrateToDataTiersAction.class),
            new ActionHandler(ILMActions.PUT, TransportPutLifecycleAction.class),
            new ActionHandler(GetLifecycleAction.INSTANCE, TransportGetLifecycleAction.class),
            new ActionHandler(DeleteLifecycleAction.INSTANCE, TransportDeleteLifecycleAction.class),
            new ActionHandler(ExplainLifecycleAction.INSTANCE, TransportExplainLifecycleAction.class),
            new ActionHandler(RemoveIndexLifecyclePolicyAction.INSTANCE, TransportRemoveIndexLifecyclePolicyAction.class),
            new ActionHandler(ILMActions.MOVE_TO_STEP, TransportMoveToStepAction.class),
            new ActionHandler(ILMActions.RETRY, TransportRetryAction.class),
            new ActionHandler(ILMActions.START, TransportStartILMAction.class),
            new ActionHandler(ILMActions.STOP, TransportStopILMAction.class),
            new ActionHandler(GetStatusAction.INSTANCE, TransportGetStatusAction.class)
        );
    }

    List<ReservedProjectStateHandler<?>> reservedProjectStateHandlers() {
        return List.of(reservedLifecycleAction.get());
    }

    @Override
    public Collection<HealthIndicatorService> getHealthIndicatorServices() {
        return List.of(ilmHealthIndicatorService.get());
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        assert indexLifecycleInitialisationService.get() != null;
        indexModule.addIndexEventListener(indexLifecycleInitialisationService.get());
    }

    @Override
    public void close() {
        try {
            IOUtils.close(indexLifecycleInitialisationService.get(), ilmHistoryStore.get());
        } catch (IOException e) {
            throw new ElasticsearchException("unable to close index lifecycle services", e);
        }
    }
}
