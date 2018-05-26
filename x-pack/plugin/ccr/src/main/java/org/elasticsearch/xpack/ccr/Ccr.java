/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.action.FollowIndexAction;
import org.elasticsearch.xpack.ccr.action.CreateAndFollowIndexAction;
import org.elasticsearch.xpack.ccr.action.ShardChangesAction;
import org.elasticsearch.xpack.ccr.action.ShardFollowNodeTask;
import org.elasticsearch.xpack.ccr.action.ShardFollowTask;
import org.elasticsearch.xpack.ccr.action.ShardFollowTasksExecutor;
import org.elasticsearch.xpack.ccr.action.UnfollowIndexAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.TransportBulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngineFactory;
import org.elasticsearch.xpack.ccr.rest.RestFollowIndexAction;
import org.elasticsearch.xpack.ccr.rest.RestCreateAndFollowIndexAction;
import org.elasticsearch.xpack.ccr.rest.RestUnfollowIndexAction;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.ccr.CcrSettings.CCR_ENABLED_SETTING;
import static org.elasticsearch.xpack.ccr.CcrSettings.CCR_FOLLOWING_INDEX_SETTING;

/**
 * Container class for CCR functionality.
 */
public class Ccr extends Plugin implements ActionPlugin, PersistentTaskPlugin, EnginePlugin {

    public static final String CCR_THREAD_POOL_NAME = "ccr";

    private final boolean enabled;
    private final Settings settings;

    /**
     * Construct an instance of the CCR container with the specified settings.
     *
     * @param settings the settings
     */
    public Ccr(final Settings settings) {
        this.settings = settings;
        this.enabled = CCR_ENABLED_SETTING.get(settings);
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(ClusterService clusterService,
                                                                       ThreadPool threadPool, Client client) {
        return Collections.singletonList(new ShardFollowTasksExecutor(settings, client, threadPool));
    }

    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return emptyList();
        }

        return Arrays.asList(
                new ActionHandler<>(ShardChangesAction.INSTANCE, ShardChangesAction.TransportAction.class),
                new ActionHandler<>(FollowIndexAction.INSTANCE, FollowIndexAction.TransportAction.class),
                new ActionHandler<>(UnfollowIndexAction.INSTANCE, UnfollowIndexAction.TransportAction.class),
                new ActionHandler<>(BulkShardOperationsAction.INSTANCE, TransportBulkShardOperationsAction.class),
                new ActionHandler<>(CreateAndFollowIndexAction.INSTANCE, CreateAndFollowIndexAction.TransportAction.class)
        );
    }

    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestUnfollowIndexAction(settings, restController),
                new RestFollowIndexAction(settings, restController),
                new RestCreateAndFollowIndexAction(settings, restController)
        );
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
                // Persistent action requests
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, ShardFollowTask.NAME,
                        ShardFollowTask::new),

                // Task statuses
                new NamedWriteableRegistry.Entry(Task.Status.class, ShardFollowNodeTask.Status.NAME,
                        ShardFollowNodeTask.Status::new)
        );
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
                // Persistent action requests
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(ShardFollowTask.NAME),
                        ShardFollowTask::fromXContent),

                // Task statuses
                new NamedXContentRegistry.Entry(ShardFollowNodeTask.Status.class, new ParseField(ShardFollowNodeTask.Status.NAME),
                        ShardFollowNodeTask.Status::fromXContent)
        );
    }

    /**
     * The settings defined by CCR.
     *
     * @return the settings
     */
    public List<Setting<?>> getSettings() {
        return CcrSettings.getSettings();
    }

    /**
     * The optional engine factory for CCR. This method inspects the index settings for the {@link CcrSettings#CCR_FOLLOWING_INDEX_SETTING}
     * setting to determine whether or not the engine implementation should be a following engine.
     *
     * @return the optional engine factory
     */
    public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
        if (CCR_FOLLOWING_INDEX_SETTING.get(indexSettings.getSettings())) {
            return Optional.of(new FollowingEngineFactory());
        } else {
            return Optional.empty();
        }
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (enabled == false) {
            return Collections.emptyList();
        }

        FixedExecutorBuilder ccrTp = new FixedExecutorBuilder(settings, CCR_THREAD_POOL_NAME,
                32, 100, "xpack.ccr.ccr_thread_pool");

        return Collections.singletonList(ccrTp);
    }

    protected XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

}
