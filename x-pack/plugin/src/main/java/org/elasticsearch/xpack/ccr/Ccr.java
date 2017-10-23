/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.ccr.action.FollowExistingIndexAction;
import org.elasticsearch.xpack.ccr.action.ShardChangesAction;
import org.elasticsearch.xpack.ccr.action.ShardFollowTask;
import org.elasticsearch.xpack.ccr.action.ShardFollowTasksExecutor;
import org.elasticsearch.xpack.ccr.action.UnfollowIndexAction;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngineFactory;
import org.elasticsearch.xpack.ccr.rest.RestFollowExistingIndexAction;
import org.elasticsearch.xpack.ccr.rest.RestUnfollowIndexAction;
import org.elasticsearch.xpack.persistent.PersistentTaskParams;
import org.elasticsearch.xpack.persistent.PersistentTasksExecutor;
import org.elasticsearch.xpack.security.InternalClient;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.ccr.CcrSettings.CCR_ENABLED_SETTING;
import static org.elasticsearch.xpack.ccr.CcrSettings.CCR_FOLLOWING_INDEX_SETTING;

/**
 * Container class for CCR functionality.
 */
public final class Ccr {

    public static final String CCR_THREAD_POOL_NAME = "ccr";

    private final boolean enabled;
    private final Settings settings;
    private final boolean tribeNode;
    private final boolean tribeNodeClient;

    /**
     * Construct an instance of the CCR container with the specified settings.
     *
     * @param settings the settings
     */
    public Ccr(final Settings settings) {
        this.settings = settings;
        this.enabled = CCR_ENABLED_SETTING.get(settings);
        this.tribeNode = XPackPlugin.isTribeNode(settings);
        this.tribeNodeClient = XPackPlugin.isTribeClientNode(settings);
    }

    public List<PersistentTasksExecutor<?>> createPersistentTasksExecutors(InternalClient internalClient, ThreadPool threadPool) {
        return Collections.singletonList(new ShardFollowTasksExecutor(settings, internalClient, threadPool));
    }

    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false || tribeNodeClient || tribeNode) {
            return emptyList();
        }

        return Arrays.asList(
                new ActionHandler<>(ShardChangesAction.INSTANCE, ShardChangesAction.TransportAction.class),
                new ActionHandler<>(FollowExistingIndexAction.INSTANCE, FollowExistingIndexAction.TransportAction.class),
                new ActionHandler<>(UnfollowIndexAction.INSTANCE, UnfollowIndexAction.TransportAction.class)
        );
    }

    public List<RestHandler> getRestHandlers(Settings settings, RestController restController) {
        return Arrays.asList(
                new RestUnfollowIndexAction(settings, restController),
                new RestFollowExistingIndexAction(settings, restController)
        );
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
                // Persistent action requests
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, ShardFollowTask.NAME,
                        ShardFollowTask::new),

                // Task statuses
                new NamedWriteableRegistry.Entry(Task.Status.class, ShardFollowTask.NAME,
                        ShardFollowTask.Status::new)
        );
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
                // Persistent action requests
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(ShardFollowTask.NAME),
                        ShardFollowTask::fromXContent),

                // Task statuses
                new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(ShardFollowTask.NAME),
                        ShardFollowTask.Status::fromXContent)
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
        if (enabled == false || tribeNode || tribeNodeClient) {
            return Collections.emptyList();
        }

        FixedExecutorBuilder ccrTp = new FixedExecutorBuilder(settings, CCR_THREAD_POOL_NAME,
                32, 100, "xpack.ccr.ccr_thread_pool");

        return Collections.singletonList(ccrTp);
    }

}
