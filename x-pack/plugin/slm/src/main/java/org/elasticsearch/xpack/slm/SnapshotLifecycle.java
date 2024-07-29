/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.slm;

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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.snapshots.RegisteredPolicySnapshots;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
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
import org.elasticsearch.xpack.slm.action.ReservedSnapshotAction;
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
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;

public class SnapshotLifecycle extends Plugin implements ActionPlugin, HealthPlugin {

    public static final List<NamedXContentRegistry.Entry> NAMED_X_CONTENT_ENTRIES = xContentEntries();

    private final SetOnce<SnapshotLifecycleService> snapshotLifecycleService = new SetOnce<>();
    private final SetOnce<SnapshotRetentionService> snapshotRetentionService = new SetOnce<>();
    private final SetOnce<SnapshotHistoryStore> snapshotHistoryStore = new SetOnce<>();
    private final SetOnce<SlmHealthIndicatorService> slmHealthIndicatorService = new SetOnce<>();
    private final Settings settings;

    public SnapshotLifecycle(Settings settings) {
        this.settings = settings;
    }

    // overridable by tests
    protected Clock getClock() {
        return Clock.systemUTC();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
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
    public Collection<?> createComponents(PluginServices services) {
        Client client = services.client();
        ClusterService clusterService = services.clusterService();
        ThreadPool threadPool = services.threadPool();
        final List<Object> components = new ArrayList<>();

        SnapshotLifecycleTemplateRegistry templateRegistry = new SnapshotLifecycleTemplateRegistry(
            settings,
            clusterService,
            services.featureService(),
            threadPool,
            client,
            services.xContentRegistry()
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
        Collections.addAll(components, snapshotLifecycleService.get(), snapshotHistoryStore.get(), snapshotRetentionService.get());
        slmHealthIndicatorService.set(new SlmHealthIndicatorService(clusterService));

        return components;
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return NAMED_X_CONTENT_ENTRIES;
    }

    private static List<NamedXContentRegistry.Entry> xContentEntries() {
        return Arrays.asList(
            // Custom Metadata
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(SnapshotLifecycleMetadata.TYPE),
                parser -> SnapshotLifecycleMetadata.PARSER.parse(parser, null)
            ),
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(RegisteredPolicySnapshots.TYPE),
                RegisteredPolicySnapshots::parse
            )
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
        List<RestHandler> handlers = new ArrayList<>();

        handlers.addAll(
            Arrays.asList(
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
        var slmUsageAction = new ActionHandler<>(XPackUsageFeatureAction.SNAPSHOT_LIFECYCLE, SLMUsageTransportAction.class);
        var slmInfoAction = new ActionHandler<>(XPackInfoFeatureAction.SNAPSHOT_LIFECYCLE, SLMInfoTransportAction.class);
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(slmUsageAction);
        actions.add(slmInfoAction);
        actions.addAll(
            Arrays.asList(
                // add SLM actions
                new ActionHandler<>(PutSnapshotLifecycleAction.INSTANCE, TransportPutSnapshotLifecycleAction.class),
                new ActionHandler<>(DeleteSnapshotLifecycleAction.INSTANCE, TransportDeleteSnapshotLifecycleAction.class),
                new ActionHandler<>(GetSnapshotLifecycleAction.INSTANCE, TransportGetSnapshotLifecycleAction.class),
                new ActionHandler<>(ExecuteSnapshotLifecycleAction.INSTANCE, TransportExecuteSnapshotLifecycleAction.class),
                new ActionHandler<>(GetSnapshotLifecycleStatsAction.INSTANCE, TransportGetSnapshotLifecycleStatsAction.class),
                new ActionHandler<>(ExecuteSnapshotRetentionAction.INSTANCE, TransportExecuteSnapshotRetentionAction.class),
                new ActionHandler<>(TransportSLMGetExpiredSnapshotsAction.INSTANCE, TransportSLMGetExpiredSnapshotsAction.class),
                new ActionHandler<>(StartSLMAction.INSTANCE, TransportStartSLMAction.class),
                new ActionHandler<>(StopSLMAction.INSTANCE, TransportStopSLMAction.class),
                new ActionHandler<>(GetSLMStatusAction.INSTANCE, TransportGetSLMStatusAction.class)
            )
        );
        return actions;
    }

    List<ReservedClusterStateHandler<?>> reservedClusterStateHandlers() {
        return List.of(new ReservedSnapshotAction());
    }

    @Override
    public Collection<HealthIndicatorService> getHealthIndicatorServices() {
        return List.of(slmHealthIndicatorService.get());
    }

    @Override
    public void close() {
        try {
            IOUtils.close(snapshotLifecycleService.get(), snapshotRetentionService.get());
        } catch (IOException e) {
            throw new ElasticsearchException("unable to close snapshot lifecycle services", e);
        }
    }
}
