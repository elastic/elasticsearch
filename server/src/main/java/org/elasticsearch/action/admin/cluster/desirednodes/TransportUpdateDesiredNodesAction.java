/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
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
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Locale;

import static java.lang.String.format;

public class TransportUpdateDesiredNodesAction extends TransportMasterNodeAction<UpdateDesiredNodesRequest, UpdateDesiredNodesResponse> {
    private static final Logger logger = LogManager.getLogger(TransportUpdateDesiredNodesAction.class);

    private final DesiredNodesSettingsValidator settingsValidator;
    private final ClusterStateTaskExecutor<UpdateDesiredNodesTask> taskExecutor;

    @Inject
    public TransportUpdateDesiredNodesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DesiredNodesSettingsValidator settingsValidator,
        AllocationService allocationService
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
        this.taskExecutor = new UpdateDesiredNodesExecutor(clusterService.getRerouteService(), allocationService);
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
                    "Unable to use processor ranges, floating-point (with greater precision) processors "
                        + "in mixed-clusters with nodes in version: "
                        + minNodeVersion
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
        final DesiredNodes proposedDesiredNodes = DesiredNodes.createIncludingStatusFromPreviousVersion(
            request.getHistoryID(),
            request.getVersion(),
            request.getNodes(),
            latestDesiredNodes
        );

        if (latestDesiredNodes != null) {
            if (latestDesiredNodes.equalsWithProcessorsCloseTo(proposedDesiredNodes)) {
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

        return proposedDesiredNodes;
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
        private static final ActionListener<ClusterState> REROUTE_LISTENER = ActionListener.wrap(
            r -> logger.trace("reroute after desired nodes update completed"),
            e -> logger.debug("reroute after desired nodes update failed", e)
        );

        private final RerouteService rerouteService;
        private final AllocationService allocationService;

        UpdateDesiredNodesExecutor(RerouteService rerouteService, AllocationService allocationService) {
            this.rerouteService = rerouteService;
            this.allocationService = allocationService;
        }

        @Override
        public ClusterState execute(BatchExecutionContext<UpdateDesiredNodesTask> batchExecutionContext) throws Exception {
            final var initialState = batchExecutionContext.initialState();
            final var initialDesiredNodes = DesiredNodesMetadata.fromClusterState(initialState).getLatestDesiredNodes();
            var desiredNodes = initialDesiredNodes;
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                final UpdateDesiredNodesRequest request = taskContext.getTask().request();
                if (request.isDryRun()) {
                    try {
                        updateDesiredNodes(desiredNodes, request);
                        taskContext.success(() -> taskContext.getTask().listener().onResponse(new UpdateDesiredNodesResponse(false, true)));
                    } catch (Exception e) {
                        taskContext.onFailure(e);
                    }
                    continue;
                }
                final var previousDesiredNodes = desiredNodes;
                try (var ignored = taskContext.captureResponseHeaders()) {
                    desiredNodes = updateDesiredNodes(desiredNodes, request);
                } catch (Exception e) {
                    taskContext.onFailure(e);
                    continue;
                }
                final var replacedExistingHistoryId = previousDesiredNodes != null
                    && previousDesiredNodes.hasSameHistoryId(desiredNodes) == false;
                taskContext.success(
                    () -> taskContext.getTask().listener().onResponse(new UpdateDesiredNodesResponse(replacedExistingHistoryId, false))
                );
            }

            desiredNodes = DesiredNodes.updateDesiredNodesStatusIfNeeded(initialState.nodes(), desiredNodes);

            if (desiredNodes == initialDesiredNodes) {
                return initialState;
            } else {
                final ClusterState withUpdatedDesiredNodes = replaceDesiredNodes(initialState, desiredNodes);
                return allocationService.adaptAutoExpandReplicas(withUpdatedDesiredNodes);
            }
        }

        @Override
        public void clusterStatePublished(ClusterState newClusterState) {
            rerouteService.reroute("desired nodes updated", Priority.HIGH, REROUTE_LISTENER);
        }
    }
}
