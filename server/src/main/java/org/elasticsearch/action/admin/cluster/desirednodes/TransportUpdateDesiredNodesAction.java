/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.desirednodes.DesiredNodesSettingsValidator;
import org.elasticsearch.cluster.desirednodes.VersionConflictException;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.DesiredNodesMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.function.BiConsumer;

import static java.lang.String.format;

public class TransportUpdateDesiredNodesAction extends TransportMasterNodeAction<UpdateDesiredNodesRequest, UpdateDesiredNodesResponse> {

    private final DesiredNodesSettingsValidator settingsValidator;
    private final ClusterStateTaskExecutor<UpdateDesiredNodesTask> taskExecutor;

    @Inject
    public TransportUpdateDesiredNodesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DesiredNodesSettingsValidator settingsValidator
    ) {
        super(
            UpdateDesiredNodesAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateDesiredNodesRequest::new,
            indexNameExpressionResolver,
            UpdateDesiredNodesResponse::new,
            ThreadPool.Names.SAME
        );
        this.settingsValidator = settingsValidator;
        this.taskExecutor = new UpdateDesiredNodesExecutor(clusterService.getRerouteService());
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateDesiredNodesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(
        Task task,
        UpdateDesiredNodesRequest request,
        ClusterState state,
        ActionListener<UpdateDesiredNodesResponse> listener
    ) throws Exception {
        try {
            settingsValidator.validate(request.getNodes());
            clusterService.submitStateUpdateTask(
                "update-desired-nodes",
                new UpdateDesiredNodesTask(request, listener),
                ClusterStateTaskConfig.build(Priority.URGENT, request.masterNodeTimeout()),
                taskExecutor
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected void doExecute(Task task, UpdateDesiredNodesRequest request, ActionListener<UpdateDesiredNodesResponse> listener) {
        final var minNodeVersion = clusterService.state().nodes().getMinNodeVersion();
        if (request.isCompatibleWithVersion(minNodeVersion) == false) {
            listener.onFailure(
                new IllegalArgumentException(
                    "Unable to use processor ranges or floating-point processors in mixed-clusters with nodes in version: " + minNodeVersion
                )
            );
            return;
        }

        super.doExecute(task, request, listener);
    }

    static ClusterState replaceDesiredNodes(ClusterState clusterState, DesiredNodes newDesiredNodes) {
        return clusterState.copyAndUpdateMetadata(
            metadata -> metadata.putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(newDesiredNodes))
        );
    }

    static DesiredNodes updateDesiredNodes(DesiredNodes latestDesiredNodes, UpdateDesiredNodesRequest request) {
        final DesiredNodes proposedDesiredNodes = new DesiredNodes(request.getHistoryID(), request.getVersion(), request.getNodes());

        if (latestDesiredNodes != null) {
            if (latestDesiredNodes.equals(proposedDesiredNodes)) {
                return latestDesiredNodes;
            }

            if (latestDesiredNodes.hasSameVersion(proposedDesiredNodes)) {
                throw new IllegalArgumentException(
                    format(
                        Locale.ROOT,
                        "Desired nodes with history [%s] and version [%d] already exists with a different definition",
                        latestDesiredNodes.historyID(),
                        latestDesiredNodes.version()
                    )
                );
            }

            if (latestDesiredNodes.isSupersededBy(proposedDesiredNodes) == false) {
                throw new VersionConflictException(
                    "version [{}] has been superseded by version [{}] for history [{}]",
                    proposedDesiredNodes.version(),
                    latestDesiredNodes.version(),
                    latestDesiredNodes.historyID()
                );
            }
        }

        return proposedDesiredNodes.withMembershipInfoFrom(latestDesiredNodes);
    }

    private record UpdateDesiredNodesTask(UpdateDesiredNodesRequest request, ActionListener<UpdateDesiredNodesResponse> listener)
        implements
            ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    private static class UpdateDesiredNodesExecutor implements ClusterStateTaskExecutor<UpdateDesiredNodesTask> {
        private final RerouteService rerouteService;

        UpdateDesiredNodesExecutor(RerouteService rerouteService) {
            this.rerouteService = rerouteService;
        }

        private static final BiConsumer<ActionListener<UpdateDesiredNodesResponse>, ClusterState> SUCCESS_SAME_HISTORY_ID = (l, s) -> l
            .onResponse(new UpdateDesiredNodesResponse(false));
        private static final BiConsumer<ActionListener<UpdateDesiredNodesResponse>, ClusterState> SUCCESS_NEW_HISTORY_ID = (l, s) -> l
            .onResponse(new UpdateDesiredNodesResponse(true));

        @Override
        public ClusterState execute(ClusterState currentState, List<TaskContext<UpdateDesiredNodesTask>> taskContexts) throws Exception {
            var desiredNodes = DesiredNodesMetadata.fromClusterState(currentState).getLatestDesiredNodes();
            final List<PendingTask> pendingTaskListeners = new ArrayList<>();
            for (final var taskContext : taskContexts) {

                final var previousDesiredNodes = desiredNodes;
                try {
                    desiredNodes = updateDesiredNodes(desiredNodes, taskContext.getTask().request());
                } catch (Exception e) {
                    taskContext.onFailure(e);
                    continue;
                }
                final var replacedExistingHistoryId = previousDesiredNodes != null
                    && previousDesiredNodes.hasSameHistoryId(desiredNodes) == false;
                pendingTaskListeners.add(new PendingTask(taskContext, replacedExistingHistoryId));
            }

            final var updatedClusterState = DesiredNodes.withMembershipInformationUpgraded(currentState, desiredNodes);
            if (updatedClusterState == currentState) {
                for (final var pendingTaskListener : pendingTaskListeners) {
                    pendingTaskListener.setClusterStateUpdateTaskListener(pendingTaskListener.getUpdateRequestListener());
                }
                return currentState;
            } else {
                final var pendingUpdateRequestListeners = pendingTaskListeners.stream().map(PendingTask::getUpdateRequestListener).toList();
                GroupedActionListener<ClusterState> clusterStateGroupedActionListener = new GroupedActionListener<>(new ActionListener<>() {
                    @Override
                    public void onResponse(Collection<ClusterState> clusterStates) {
                        rerouteService.reroute("upgraded desired nodes", Priority.URGENT, new ActionListener<>() {
                            @Override
                            public void onResponse(ClusterState clusterState) {
                                ActionListener.onResponse(pendingUpdateRequestListeners, clusterState);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (MasterService.isPublishFailureException(e)) {
                                    // The request listener ignores the cluster state argument,
                                    // therefore it's safe to pass the EMPTY_state here
                                    ActionListener.onResponse(pendingUpdateRequestListeners, ClusterState.EMPTY_STATE);
                                } else {
                                    ActionListener.onFailure(
                                        pendingUpdateRequestListeners,
                                        new ElasticsearchException("reroute after update desired nodes failed", e)
                                    );
                                }
                            }
                        });
                    }

                    @Override
                    public void onFailure(Exception e) {
                        ActionListener.onFailure(pendingUpdateRequestListeners, e);
                    }
                }, pendingTaskListeners.size());

                for (final var pendingTaskListener : pendingTaskListeners) {
                    pendingTaskListener.setClusterStateUpdateTaskListener(clusterStateGroupedActionListener);
                }
                return updatedClusterState;
            }
        }

        record PendingTask(ClusterStateTaskExecutor.TaskContext<UpdateDesiredNodesTask> taskContext, boolean replacedExistingHistoryId) {
            void setClusterStateUpdateTaskListener(ActionListener<ClusterState> taskListener) {
                taskContext.success(taskListener);
            }

            ActionListener<ClusterState> getUpdateRequestListener() {
                return taskContext.getTask()
                    .listener()
                    .delegateFailure(replacedExistingHistoryId ? SUCCESS_NEW_HISTORY_ID : SUCCESS_SAME_HISTORY_ID);
            }
        }
    }
}
