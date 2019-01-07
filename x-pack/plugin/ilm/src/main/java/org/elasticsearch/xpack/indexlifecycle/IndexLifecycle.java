/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.indexlifecycle.AllocateAction;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.ForceMergeAction;
import org.elasticsearch.xpack.core.indexlifecycle.FreezeAction;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.ReadOnlyAction;
import org.elasticsearch.xpack.core.indexlifecycle.RolloverAction;
import org.elasticsearch.xpack.core.indexlifecycle.ShrinkAction;
import org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetStatusAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.MoveToStepAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.RemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.RetryAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.StartILMAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.StopILMAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestDeleteLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestExplainLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestGetLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestGetStatusAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestMoveToStepAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestPutLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestRemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestRetryAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestStartILMAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestStopAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportDeleteLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportExplainLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportGetLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportGetStatusAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportMoveToStepAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportPutLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportRemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportRetryAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportStartILMAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportStopILMAction;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

public class IndexLifecycle extends Plugin implements ActionPlugin {
    private final SetOnce<IndexLifecycleService> indexLifecycleInitialisationService = new SetOnce<>();
    private Settings settings;
    private boolean enabled;
    private boolean transportClientMode;

    public IndexLifecycle(Settings settings) {
        this.settings = settings;
        this.enabled = XPackSettings.INDEX_LIFECYCLE_ENABLED.get(settings);
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
    }

    // overridable by tests
    protected Clock getClock() {
        return Clock.systemUTC();
    }

    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();

        if (transportClientMode) {
            return modules;
        }

        modules.add(b -> XPackPlugin.bindFeatureSet(b, IndexLifecycleFeatureSet.class));

        return modules;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING,
            LifecycleSettings.LIFECYCLE_NAME_SETTING,
            LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING,
            RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING);
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        if (enabled == false || transportClientMode) {
            return emptyList();
        }
        indexLifecycleInitialisationService
            .set(new IndexLifecycleService(settings, client, clusterService, getClock(), System::currentTimeMillis, xContentRegistry));
        return Collections.singletonList(indexLifecycleInitialisationService.get());
    }

    @Override
    public List<Entry> getNamedWriteables() {
        return Arrays.asList();
    }

    @Override
    public List<org.elasticsearch.common.xcontent.NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
            // Custom Metadata
            new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(IndexLifecycleMetadata.TYPE),
                parser -> IndexLifecycleMetadata.PARSER.parse(parser, null)),
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
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(FreezeAction.NAME), FreezeAction::parse)
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
                new RestPutLifecycleAction(settings, restController),
                new RestGetLifecycleAction(settings, restController),
                new RestDeleteLifecycleAction(settings, restController),
                new RestExplainLifecycleAction(settings, restController),
                new RestRemoveIndexLifecyclePolicyAction(settings, restController),
                new RestMoveToStepAction(settings, restController),
                new RestRetryAction(settings, restController),
                new RestStopAction(settings, restController),
                new RestStartILMAction(settings, restController),
                new RestGetStatusAction(settings, restController)
            );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return emptyList();
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
                new ActionHandler<>(GetStatusAction.INSTANCE, TransportGetStatusAction.class));
    }

    @Override
    public void close() {
        IndexLifecycleService lifecycleService = indexLifecycleInitialisationService.get();
        if (lifecycleService != null) {
            lifecycleService.close();
        }
    }
}
