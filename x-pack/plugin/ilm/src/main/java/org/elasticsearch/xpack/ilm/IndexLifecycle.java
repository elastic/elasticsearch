/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackSettings;
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
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.ilm.UnfollowAction;
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
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore;
import org.elasticsearch.xpack.core.slm.history.SnapshotLifecycleTemplateRegistry;
import org.elasticsearch.xpack.ilm.action.RestDeleteLifecycleAction;
import org.elasticsearch.xpack.ilm.action.RestExplainLifecycleAction;
import org.elasticsearch.xpack.ilm.action.RestGetLifecycleAction;
import org.elasticsearch.xpack.ilm.action.RestGetStatusAction;
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
import org.elasticsearch.xpack.ilm.action.TransportMoveToStepAction;
import org.elasticsearch.xpack.ilm.action.TransportPutLifecycleAction;
import org.elasticsearch.xpack.ilm.action.TransportRemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.ilm.action.TransportRetryAction;
import org.elasticsearch.xpack.ilm.action.TransportStartILMAction;
import org.elasticsearch.xpack.ilm.action.TransportStopILMAction;
import org.elasticsearch.xpack.slm.SnapshotLifecycleService;
import org.elasticsearch.xpack.slm.SnapshotLifecycleTask;
import org.elasticsearch.xpack.slm.action.RestDeleteSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.RestExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.RestGetSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.RestPutSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.TransportDeleteSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.TransportExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.TransportGetSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.action.TransportPutSnapshotLifecycleAction;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;

public class IndexLifecycle extends Plugin implements ActionPlugin {
    private final SetOnce<IndexLifecycleService> indexLifecycleInitialisationService = new SetOnce<>();
    private final SetOnce<SnapshotLifecycleService> snapshotLifecycleService = new SetOnce<>();
    private final SetOnce<SnapshotHistoryStore> snapshotHistoryStore = new SetOnce<>();
    private Settings settings;
    private boolean enabled;

    public IndexLifecycle(Settings settings) {
        this.settings = settings;
        this.enabled = XPackSettings.INDEX_LIFECYCLE_ENABLED.get(settings);
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
            LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING,
            RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING,
            LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING);
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        if (enabled == false) {
            return emptyList();
        }
        indexLifecycleInitialisationService.set(new IndexLifecycleService(settings, client, clusterService, threadPool,
                getClock(), System::currentTimeMillis, xContentRegistry));
        SnapshotLifecycleTemplateRegistry templateRegistry = new SnapshotLifecycleTemplateRegistry(settings, clusterService, threadPool,
            client, xContentRegistry);
        snapshotHistoryStore.set(new SnapshotHistoryStore(settings, new OriginSettingClient(client, INDEX_LIFECYCLE_ORIGIN), clusterService
        ));
        snapshotLifecycleService.set(new SnapshotLifecycleService(settings,
            () -> new SnapshotLifecycleTask(client, clusterService, snapshotHistoryStore.get()), clusterService, getClock()));
        return Arrays.asList(indexLifecycleInitialisationService.get(), snapshotLifecycleService.get(), snapshotHistoryStore.get());
    }

    @Override
    public List<Entry> getNamedWriteables() {
        return Collections.emptyList();
    }

    @Override
    public List<org.elasticsearch.common.xcontent.NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
            // Custom Metadata
            new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(IndexLifecycleMetadata.TYPE),
                parser -> IndexLifecycleMetadata.PARSER.parse(parser, null)),
            new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(SnapshotLifecycleMetadata.TYPE),
                parser -> SnapshotLifecycleMetadata.PARSER.parse(parser, null)),
            // Lifecycle Types
            new NamedXContentRegistry.Entry(LifecycleType.class, new ParseField(TimeseriesLifecycleType.TYPE),
                (p, c) -> TimeseriesLifecycleType.INSTANCE),
            // Lifecycle Actions
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(FreezeAction.NAME), FreezeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SetPriorityAction.NAME), SetPriorityAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(UnfollowAction.NAME), UnfollowAction::parse)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        if (enabled == false) {
            return emptyList();
        }
        return Arrays.asList(
                new RestPutLifecycleAction(restController),
                new RestGetLifecycleAction(restController),
                new RestDeleteLifecycleAction(restController),
                new RestExplainLifecycleAction(restController),
                new RestRemoveIndexLifecyclePolicyAction(restController),
                new RestMoveToStepAction(restController),
                new RestRetryAction(restController),
                new RestStopAction(restController),
                new RestStartILMAction(restController),
                new RestGetStatusAction(restController),
                // Snapshot lifecycle actions
                new RestPutSnapshotLifecycleAction(restController),
                new RestDeleteSnapshotLifecycleAction(restController),
                new RestGetSnapshotLifecycleAction(restController),
                new RestExecuteSnapshotLifecycleAction(restController)
            );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var usageAction =
            new ActionHandler<>(XPackUsageFeatureAction.INDEX_LIFECYCLE, IndexLifecycleUsageTransportAction.class);
        var infoAction =
            new ActionHandler<>(XPackInfoFeatureAction.INDEX_LIFECYCLE, IndexLifecycleInfoTransportAction.class);
        if (enabled == false) {
            return Arrays.asList(usageAction, infoAction);
        }
        return Arrays.asList(
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
                // Snapshot lifecycle actions
                new ActionHandler<>(PutSnapshotLifecycleAction.INSTANCE, TransportPutSnapshotLifecycleAction.class),
                new ActionHandler<>(DeleteSnapshotLifecycleAction.INSTANCE, TransportDeleteSnapshotLifecycleAction.class),
                new ActionHandler<>(GetSnapshotLifecycleAction.INSTANCE, TransportGetSnapshotLifecycleAction.class),
                new ActionHandler<>(ExecuteSnapshotLifecycleAction.INSTANCE, TransportExecuteSnapshotLifecycleAction.class),
                usageAction,
                infoAction);
    }

    @Override
    public void close() {
        try {
            IOUtils.close(indexLifecycleInitialisationService.get(), snapshotLifecycleService.get());
        } catch (IOException e) {
            throw new ElasticsearchException("unable to close index lifecycle services", e);
        }
    }
}
