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
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
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
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator;
import org.elasticsearch.xpack.core.ccr.action.DeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.action.ShardChangesAction;
import org.elasticsearch.xpack.ccr.action.ShardFollowTask;
import org.elasticsearch.xpack.ccr.action.ShardFollowTasksExecutor;
import org.elasticsearch.xpack.ccr.action.TransportCcrStatsAction;
import org.elasticsearch.xpack.ccr.action.TransportCreateAndFollowIndexAction;
import org.elasticsearch.xpack.ccr.action.TransportDeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.action.TransportFollowIndexAction;
import org.elasticsearch.xpack.ccr.action.TransportPutAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.action.TransportUnfollowIndexAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.TransportBulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngineFactory;
import org.elasticsearch.xpack.ccr.rest.RestCcrStatsAction;
import org.elasticsearch.xpack.ccr.rest.RestCreateAndFollowIndexAction;
import org.elasticsearch.xpack.ccr.rest.RestDeleteAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.rest.RestFollowIndexAction;
import org.elasticsearch.xpack.ccr.rest.RestPutAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.rest.RestUnfollowIndexAction;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.CreateAndFollowIndexAction;
import org.elasticsearch.xpack.core.ccr.action.FollowIndexAction;
import org.elasticsearch.xpack.core.ccr.action.UnfollowIndexAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.ccr.CcrSettings.CCR_FOLLOWING_INDEX_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.CCR_ENABLED_SETTING;

/**
 * Container class for CCR functionality.
 */
public class Ccr extends Plugin implements ActionPlugin, PersistentTaskPlugin, EnginePlugin {

    public static final String CCR_THREAD_POOL_NAME = "ccr";
    public static final String CCR_CUSTOM_METADATA_KEY = "ccr";
    public static final String CCR_CUSTOM_METADATA_LEADER_INDEX_SHARD_HISTORY_UUIDS = "leader_index_shard_history_uuids";
    public static final String CCR_CUSTOM_METADATA_LEADER_INDEX_UUID_KEY = "leader_index_uuid";

    private final boolean enabled;
    private final Settings settings;
    private final CcrLicenseChecker ccrLicenseChecker;

    /**
     * Construct an instance of the CCR container with the specified settings.
     *
     * @param settings the settings
     */
    @SuppressWarnings("unused") // constructed reflectively by the plugin infrastructure
    public Ccr(final Settings settings) {
        this(settings, new CcrLicenseChecker());
    }

    /**
     * Construct an instance of the CCR container with the specified settings and license checker.
     *
     * @param settings          the settings
     * @param ccrLicenseChecker the CCR license checker
     */
    Ccr(final Settings settings, final CcrLicenseChecker ccrLicenseChecker) {
        this.settings = settings;
        this.enabled = CCR_ENABLED_SETTING.get(settings);
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
    }

    @Override
    public Collection<Object> createComponents(
            final Client client,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ResourceWatcherService resourceWatcherService,
            final ScriptService scriptService,
            final NamedXContentRegistry xContentRegistry,
            final Environment environment,
            final NodeEnvironment nodeEnvironment,
            final NamedWriteableRegistry namedWriteableRegistry) {
        if (enabled == false) {
            return emptyList();
        }

        return Arrays.asList(
            ccrLicenseChecker,
            new AutoFollowCoordinator(settings, client, threadPool, clusterService, ccrLicenseChecker)
        );
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
                // internal actions
                new ActionHandler<>(BulkShardOperationsAction.INSTANCE, TransportBulkShardOperationsAction.class),
                new ActionHandler<>(ShardChangesAction.INSTANCE, ShardChangesAction.TransportAction.class),
                // stats action
                new ActionHandler<>(CcrStatsAction.INSTANCE, TransportCcrStatsAction.class),
                // follow actions
                new ActionHandler<>(CreateAndFollowIndexAction.INSTANCE, TransportCreateAndFollowIndexAction.class),
                new ActionHandler<>(FollowIndexAction.INSTANCE, TransportFollowIndexAction.class),
                new ActionHandler<>(UnfollowIndexAction.INSTANCE, TransportUnfollowIndexAction.class),
                // auto-follow actions
                new ActionHandler<>(DeleteAutoFollowPatternAction.INSTANCE, TransportDeleteAutoFollowPatternAction.class),
                new ActionHandler<>(PutAutoFollowPatternAction.INSTANCE, TransportPutAutoFollowPatternAction.class));
    }

    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        if (enabled == false) {
            return emptyList();
        }

        return Arrays.asList(
                // stats API
                new RestCcrStatsAction(settings, restController),
                // follow APIs
                new RestCreateAndFollowIndexAction(settings, restController),
                new RestFollowIndexAction(settings, restController),
                new RestUnfollowIndexAction(settings, restController),
                // auto-follow APIs
                new RestDeleteAutoFollowPatternAction(settings, restController),
                new RestPutAutoFollowPatternAction(settings, restController));
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
                // Persistent action requests
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, ShardFollowTask.NAME,
                        ShardFollowTask::new),

                // Task statuses
                new NamedWriteableRegistry.Entry(Task.Status.class, ShardFollowNodeTaskStatus.STATUS_PARSER_NAME,
                        ShardFollowNodeTaskStatus::new)
        );
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
                // Persistent action requests
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(ShardFollowTask.NAME),
                        ShardFollowTask::fromXContent),

                // Task statuses
                new NamedXContentRegistry.Entry(
                        ShardFollowNodeTaskStatus.class,
                        new ParseField(ShardFollowNodeTaskStatus.STATUS_PARSER_NAME),
                        ShardFollowNodeTaskStatus::fromXContent));
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

        return Collections.singletonList(new FixedExecutorBuilder(settings, CCR_THREAD_POOL_NAME, 32, 100, "xpack.ccr.ccr_thread_pool"));
    }

    protected XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

}
