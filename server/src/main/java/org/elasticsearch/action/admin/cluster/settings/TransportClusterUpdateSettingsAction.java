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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsUpdater;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.reservedstate.action.ReservedClusterSettingsAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;

public class TransportClusterUpdateSettingsAction extends TransportMasterNodeAction<
    ClusterUpdateSettingsRequest,
    ClusterUpdateSettingsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClusterUpdateSettingsAction.class);

    private final RerouteService rerouteService;
    private final ClusterSettings clusterSettings;

    @Inject
    public TransportClusterUpdateSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        RerouteService rerouteService,
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
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.rerouteService = rerouteService;
        this.clusterSettings = clusterSettings;
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
        submitUnbatchedTask(UPDATE_TASK_SOURCE, new ClusterUpdateSettingsTask(clusterSettings, Priority.IMMEDIATE, request, listener) {
            @Override
            protected ClusterUpdateSettingsResponse newResponse(boolean acknowledged) {
                return new ClusterUpdateSettingsResponse(acknowledged, updater.getTransientUpdates(), updater.getPersistentUpdate());
            }

            @Override
            public void onAllNodesAcked() {
                if (reroute) {
                    reroute(true);
                } else {
                    super.onAllNodesAcked();
                }
            }

            @Override
            public void onAckFailure(Exception e) {
                if (reroute) {
                    reroute(false);
                } else {
                    super.onAckFailure(e);
                }
            }

            @Override
            public void onAckTimeout() {
                if (reroute) {
                    reroute(false);
                } else {
                    super.onAckTimeout();
                }
            }

            private void reroute(final boolean updateSettingsAcked) {
                // The reason the reroute needs to be sent as separate update task, is that all the *cluster* settings are encapsulated in
                // the components (e.g. FilterAllocationDecider), so the changes made by the first call aren't visible to the components
                // until the ClusterStateListener instances have been invoked, but are visible after the first update task has been
                // completed.
                rerouteService.reroute(REROUTE_TASK_SOURCE, Priority.URGENT, new ActionListener<>() {
                    @Override
                    public void onResponse(Void ignored) {
                        listener.onResponse(
                            new ClusterUpdateSettingsResponse(
                                updateSettingsAcked,
                                updater.getTransientUpdates(),
                                updater.getPersistentUpdate()
                            )
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(() -> "failed to perform [" + REROUTE_TASK_SOURCE + "]", e);
                        if (MasterService.isPublishFailureException(e)) {
                            listener.onResponse(
                                new ClusterUpdateSettingsResponse(
                                    updateSettingsAcked,
                                    updater.getTransientUpdates(),
                                    updater.getPersistentUpdate()
                                )
                            );
                        } else {
                            listener.onFailure(new ElasticsearchException("reroute after update settings failed", e));
                        }
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(() -> "failed to perform [" + UPDATE_TASK_SOURCE + "]", e);
                super.onFailure(e);
            }
        });
    }

    private static class ClusterUpdateSettingsTask extends AckedClusterStateUpdateTask {
        protected volatile boolean reroute = false;
        protected final SettingsUpdater updater;
        protected final ClusterUpdateSettingsRequest request;

        ClusterUpdateSettingsTask(
            final ClusterSettings clusterSettings,
            Priority priority,
            ClusterUpdateSettingsRequest request,
            ActionListener<? extends AcknowledgedResponse> listener
        ) {
            super(priority, request, listener);
            this.updater = new SettingsUpdater(clusterSettings);
            this.request = request;
        }

        @Override
        public ClusterState execute(final ClusterState currentState) {
            final ClusterState clusterState = updater.updateSettings(
                currentState,
                request.transientSettings(),
                request.persistentSettings(),
                logger
            );
            reroute = clusterState != currentState;
            return clusterState;
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }
}
