/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsUpdater;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;

public class TransportClusterUpdateSettingsAction extends TransportMasterNodeAction<
    ClusterUpdateSettingsRequest,
    ClusterUpdateSettingsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterUpdateSettingsAction.class);

    private final MasterServiceTaskQueue<ClusterUpdateSettingsTask> taskQueue;

    @Inject
    public TransportClusterUpdateSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterSettings clusterSettings
    ) {
        super(
            ClusterUpdateSettingsAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterUpdateSettingsRequest::new,
            indexNameExpressionResolver,
            ClusterUpdateSettingsResponse::new,
            ThreadPool.Names.SAME
        );
        this.taskQueue = clusterService.createTaskQueue("cluster-update-settings", Priority.IMMEDIATE, new Executor());
    }

    /**
     skip check block if:
     * Only at least one of cluster.blocks.read_only or cluster.blocks.read_only_allow_delete is being cleared (set to null or false).
     * Or all of the following are true:
     * 1. At least one of cluster.blocks.read_only or cluster.blocks.read_only_allow_delete is being cleared (set to null or false).
     * 2. Neither cluster.blocks.read_only nor cluster.blocks.read_only_allow_delete is being set to true.
     * 3. The only other settings in this update are archived ones being set to null.
     */
    @Override
    protected ClusterBlockException checkBlock(ClusterUpdateSettingsRequest request, ClusterState state) {
        Set<String> clearedBlockAndArchivedSettings = new HashSet<>();
        if (checkClearedBlockAndArchivedSettings(request.transientSettings(), clearedBlockAndArchivedSettings)
            && checkClearedBlockAndArchivedSettings(request.persistentSettings(), clearedBlockAndArchivedSettings)) {
            if (clearedBlockAndArchivedSettings.contains(Metadata.SETTING_READ_ONLY_SETTING.getKey())
                || clearedBlockAndArchivedSettings.contains(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey())) {
                return null;
            }
        }

        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    /**
     * Check settings that only contains block and archived settings.
     * @param settings target settings to be checked.
     * @param clearedBlockAndArchivedSettings block settings that have been set to null or false,
     *                                        archived settings that have been set to null.
     * @return true if all settings are clear blocks or archived settings.
     */
    private static boolean checkClearedBlockAndArchivedSettings(
        final Settings settings,
        final Set<String> clearedBlockAndArchivedSettings
    ) {
        for (String key : settings.keySet()) {
            if (Metadata.SETTING_READ_ONLY_SETTING.getKey().equals(key)) {
                if (Metadata.SETTING_READ_ONLY_SETTING.get(settings)) {
                    // set block as true
                    return false;
                }
            } else if (Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey().equals(key)) {
                if (Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.get(settings)) {
                    // set block as true
                    return false;
                }
            } else if (key.startsWith(ARCHIVED_SETTINGS_PREFIX)) {
                if (settings.get(key) != null) {
                    // archived setting value is not null
                    return false;
                }
            } else {
                // other settings
                return false;
            }
            clearedBlockAndArchivedSettings.add(key);
        }
        return true;
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ReservedClusterSettingsAction.NAME);
    }

    @Override
    public Set<String> modifiedKeys(ClusterUpdateSettingsRequest request) {
        Settings allSettings = Settings.builder().put(request.persistentSettings()).put(request.transientSettings()).build();
        return allSettings.keySet();
    }

    private static final String UPDATE_TASK_SOURCE = "cluster_update_settings";
    private static final String REROUTE_TASK_SOURCE = "reroute_after_cluster_update_settings";

    @Override
    protected void masterOperation(
        Task task,
        final ClusterUpdateSettingsRequest request,
        final ClusterState state,
        final ActionListener<ClusterUpdateSettingsResponse> listener
    ) {
        taskQueue.submitTask(UPDATE_TASK_SOURCE, new ClusterUpdateSettingsTask(request, listener), request.masterNodeTimeout());
    }

    private class Executor implements ClusterStateTaskExecutor<ClusterUpdateSettingsTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<ClusterUpdateSettingsTask> batchExecutionContext) throws Exception {
            var clusterState = batchExecutionContext.initialState();

            final var rerouteListener = new SubscribableListener<Void>();
            boolean reroutePending = false;
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                final var request = taskContext.getTask().request();
                final var updater = new SettingsUpdater(clusterService.getClusterSettings());
                final ClusterState updatedState;

                try (var ignored = taskContext.captureResponseHeaders()) {
                    updatedState = updater.updateSettings(clusterState, request.transientSettings(), request.persistentSettings(), logger);
                } catch (Exception e) {
                    taskContext.onFailure(e);
                    continue;
                }

                final Runnable publicationSuccessHandler;
                if (updatedState != clusterState && reroutePending == false) {
                    // this is the first task in the batch which changed the cluster state, so if the publication succeeds then we must
                    // submit a followup reroute which will complete the rerouteListener when it finishes
                    reroutePending = true;
                    publicationSuccessHandler = () -> clusterService.getRerouteService()
                        .reroute(REROUTE_TASK_SOURCE, Priority.URGENT, rerouteListener.delegateResponse((l, e) -> {
                            logger.debug(() -> "failed to perform [" + REROUTE_TASK_SOURCE + "]", e);
                            if (MasterService.isPublishFailureException(e)) {
                                l.onResponse(null);
                            } else {
                                l.onFailure(new ElasticsearchException("reroute after update settings failed", e));
                            }
                        }));
                } else {
                    // either this task did not change the cluster state, or we're already submitting a reroute
                    publicationSuccessHandler = () -> {};
                }

                taskContext.success(
                    publicationSuccessHandler,
                    new PublicationSuccessHandler(
                        request.ackTimeout(),
                        updater.getTransientUpdates(),
                        updater.getPersistentUpdate(),
                        // when acking finishes, subscribe to the reroute listener
                        response -> rerouteListener.addListener(
                            taskContext.getTask().listener().map(ignored -> response),
                            EsExecutors.DIRECT_EXECUTOR_SERVICE,
                            threadPool.getThreadContext()
                        )
                    )
                );

                clusterState = updatedState;
            }

            assert (reroutePending == false) == (clusterState == batchExecutionContext.initialState());
            if (reroutePending == false) {
                // none of the tasks changed the cluster state so there's no reroute to do, but we must still complete the reroute listener
                rerouteListener.onResponse(null);
            }

            return clusterState;
        }
    }

    private record PublicationSuccessHandler(
        TimeValue ackTimeout,
        Settings transientUpdate,
        Settings persistentUpdate,
        Consumer<ClusterUpdateSettingsResponse> responseConsumer
    ) implements ClusterStateAckListener {

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return true;
        }

        @Override
        public TimeValue ackTimeout() {
            return ackTimeout;
        }

        @Override
        public void onAckFailure(Exception e) {
            onCompletion(false);
        }

        @Override
        public void onAckTimeout() {
            onCompletion(false);
        }

        @Override
        public void onAllNodesAcked() {
            onCompletion(true);
        }

        private void onCompletion(boolean updateSettingsAcked) {
            responseConsumer.accept(new ClusterUpdateSettingsResponse(updateSettingsAcked, transientUpdate, persistentUpdate));
        }
    }

    private record ClusterUpdateSettingsTask(ClusterUpdateSettingsRequest request, ActionListener<ClusterUpdateSettingsResponse> listener)
        implements
            ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }
}
