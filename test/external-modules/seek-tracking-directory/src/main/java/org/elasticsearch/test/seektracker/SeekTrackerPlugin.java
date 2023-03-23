/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.seektracker;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class SeekTrackerPlugin extends Plugin implements ActionPlugin {

    /** Setting for enabling or disabling seek tracking. Defaults to false. */
    public static final Setting<Boolean> SEEK_TRACKING_ENABLED = Setting.boolSetting(
        "seektracker.enabled",
        false,
        Setting.Property.NodeScope
    );

    private final SeekStatsService seekStatsService = new SeekStatsService();
    private final boolean enabled;

    public SeekTrackerPlugin(Settings settings) {
        this.enabled = SEEK_TRACKING_ENABLED.get(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SEEK_TRACKING_ENABLED);
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
        AllocationService allocationService
    ) {
        return Collections.singletonList(seekStatsService);
    }

    // seeks per index/shard/file

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (enabled) {
            IndexSeekTracker seekTracker = seekStatsService.registerIndex(indexModule.getIndex().getName());
            indexModule.setDirectoryWrapper(new SeekTrackingDirectoryWrapper(seekTracker));
        }
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        if (enabled) {
            return Collections.singletonList(new RestSeekStatsAction());
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled) {
            return Collections.singletonList(new ActionHandler<>(SeekStatsAction.INSTANCE, TransportSeekStatsAction.class));
        } else {
            return Collections.emptyList();
        }
    }
}
