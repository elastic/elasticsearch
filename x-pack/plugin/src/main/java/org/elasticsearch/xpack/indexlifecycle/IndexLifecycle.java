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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.indexlifecycle.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.GetLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.PutLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestDeleteLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestGetLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.action.RestPutLifecycleAction;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

public class IndexLifecycle extends Plugin {
    public static final String NAME = "index_lifecycle";
    public static final String BASE_PATH = "/_xpack/index_lifecycle/";
    public static final String THREAD_POOL_NAME = NAME;
    private final SetOnce<IndexLifecycleService> indexLifecycleInitialisationService = new SetOnce<>();
    private Settings settings;
    private boolean enabled;
    private boolean transportClientMode;

    // NORELEASE: we should probably change the default to something other than three seconds for initial release
    public static final Setting<TimeValue> LIFECYCLE_POLL_INTERVAL_SETTING = Setting.positiveTimeSetting("indices.lifecycle.poll_interval",
        TimeValue.timeValueSeconds(3), Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> LIFECYCLE_NAME_SETTING = Setting.simpleString("index.lifecycle.name",
        Setting.Property.Dynamic, Setting.Property.IndexScope);
    public static final Setting<String> LIFECYCLE_PHASE_SETTING = Setting.simpleString("index.lifecycle.phase",
        Setting.Property.Dynamic, Setting.Property.IndexScope);
    public static final Setting<String> LIFECYCLE_ACTION_SETTING = Setting.simpleString("index.lifecycle.action",
            Setting.Property.Dynamic, Setting.Property.IndexScope);

    public IndexLifecycle(Settings settings) {
        this.settings = settings;
        this.enabled = XPackSettings.INDEX_LIFECYCLE_ENABLED.get(settings);
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
    }

    public Collection<Module> nodeModules() {
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
                LIFECYCLE_POLL_INTERVAL_SETTING,
            LIFECYCLE_NAME_SETTING,
                LIFECYCLE_PHASE_SETTING,
            LIFECYCLE_ACTION_SETTING);
    }

    public Collection<Object> createComponents(Client client, ClusterService clusterService, Clock clock,
            ThreadPool threadPool) {
        if (enabled == false || transportClientMode) {
            return emptyList();
        }
        indexLifecycleInitialisationService
                .set(new IndexLifecycleService(settings, client, clusterService, clock, threadPool, System::currentTimeMillis));
        return Collections.singletonList(indexLifecycleInitialisationService.get());
    }

    @Override
    public void close() throws IOException {
        indexLifecycleInitialisationService.get().close();
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
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
    }

    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestPutLifecycleAction(settings, restController),
                new RestGetLifecycleAction(settings, restController),
                new RestDeleteLifecycleAction(settings, restController));
    }

    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(PutLifecycleAction.INSTANCE, PutLifecycleAction.TransportAction.class),
                new ActionHandler<>(GetLifecycleAction.INSTANCE, GetLifecycleAction.TransportAction.class),
                new ActionHandler<>(DeleteLifecycleAction.INSTANCE, DeleteLifecycleAction.TransportAction.class));
    }

}
