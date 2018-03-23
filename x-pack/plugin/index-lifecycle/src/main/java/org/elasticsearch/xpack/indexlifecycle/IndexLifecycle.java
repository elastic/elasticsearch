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
import org.elasticsearch.cluster.NamedDiff;
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
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.ReplicasAction;
import org.elasticsearch.xpack.core.indexlifecycle.RolloverAction;
import org.elasticsearch.xpack.core.indexlifecycle.ShrinkAction;
import org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.PutLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestDeleteLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestGetLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestPutLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportPutLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportGetLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.TransportDeleteLifcycleAction;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

public class IndexLifecycle extends Plugin implements ActionPlugin {
    public static final String NAME = "index_lifecycle";
    public static final String BASE_PATH = "/_xpack/index_lifecycle/";
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
            LifecycleSettings.LIFECYCLE_PHASE_SETTING,
            LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE_SETTING,
            LifecycleSettings.LIFECYCLE_PHASE_TIME_SETTING,
            LifecycleSettings.LIFECYCLE_ACTION_TIME_SETTING,
            LifecycleSettings.LIFECYCLE_ACTION_SETTING,
            LifecycleSettings.LIFECYCLE_STEP_TIME_SETTING,
            LifecycleSettings.LIFECYCLE_STEP_SETTING);
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
            .set(new IndexLifecycleService(settings, client, clusterService, getClock(), threadPool, System::currentTimeMillis));
        return Collections.singletonList(indexLifecycleInitialisationService.get());
    }

    @Override
    public List<Entry> getNamedWriteables() {
        return Arrays.asList(
                // Custom metadata
                new NamedWriteableRegistry.Entry(MetaData.Custom.class, IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, IndexLifecycleMetadata.TYPE,
                        IndexLifecycleMetadata.IndexLifecycleMetadataDiff::new),

                // Lifecycle types
                new NamedWriteableRegistry.Entry(LifecycleType.class, TimeseriesLifecycleType.TYPE,
                        (in) -> TimeseriesLifecycleType.INSTANCE),

                // Lifecycle actions
                new NamedWriteableRegistry.Entry(LifecycleAction.class, AllocateAction.NAME, AllocateAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ForceMergeAction.NAME, ForceMergeAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ReplicasAction.NAME, ReplicasAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, RolloverAction.NAME, RolloverAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ShrinkAction.NAME, ShrinkAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new));
    }

    @Override
    public List<org.elasticsearch.common.xcontent.NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
                // Custom metadata
                new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(IndexLifecycleMetadata.TYPE),
                        parser -> IndexLifecycleMetadata.PARSER.parse(parser, null)),
                // Lifecycle Types
                new NamedXContentRegistry.Entry(LifecycleType.class, new ParseField(TimeseriesLifecycleType.TYPE),
                        (p, c) -> TimeseriesLifecycleType.INSTANCE),
                // Lifecycle actions
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReplicasAction.NAME), ReplicasAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestPutLifecycleAction(settings, restController),
                new RestGetLifecycleAction(settings, restController),
                new RestDeleteLifecycleAction(settings, restController));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(PutLifecycleAction.INSTANCE, TransportPutLifecycleAction.class),
                new ActionHandler<>(GetLifecycleAction.INSTANCE, TransportGetLifecycleAction.class),
                new ActionHandler<>(DeleteLifecycleAction.INSTANCE, TransportDeleteLifcycleAction.class));
    }

    @Override
    public void close() {
        indexLifecycleInitialisationService.get().close();
    }
}
