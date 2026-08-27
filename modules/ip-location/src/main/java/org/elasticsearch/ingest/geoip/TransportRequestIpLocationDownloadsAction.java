/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.geoip.RequestIpLocationDownloadsAction.Request.Operation;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Handles {@link RequestIpLocationDownloadsAction} by submitting a cluster state update
 * that registers or unregisters an {@link IpLocationConsumer} on the per-project
 * {@link IpLocationDownloadConsumers} metadata.
 */
public class TransportRequestIpLocationDownloadsAction extends TransportMasterNodeAction<
    RequestIpLocationDownloadsAction.Request,
    AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRequestIpLocationDownloadsAction.class);

    private static final SimpleBatchedExecutor<UpdateConsumersTask, Void> TASK_EXECUTOR = new SimpleBatchedExecutor<>() {
        @Override
        public Tuple<ClusterState, Void> executeTask(UpdateConsumersTask task, ClusterState clusterState) {
            return Tuple.tuple(task.execute(clusterState), null);
        }

        /**
         * Note: this "success" path also covers no-op task executions &mdash; e.g. when the project is
         * missing from cluster state, or when the register/unregister is idempotent. Callers that need to
         * distinguish those cases from a true state change should not rely on this response.
         */
        @Override
        public void taskSucceeded(UpdateConsumersTask task, Void unused) {
            logger.trace(
                "Updated ip-location download consumers for project [{}]: {} consumer [{}]",
                task.projectId,
                task.operation.label(),
                task.consumer
            );
            task.listener.onResponse(AcknowledgedResponse.TRUE);
        }
    };

    private final MasterServiceTaskQueue<UpdateConsumersTask> taskQueue;

    @Inject
    public TransportRequestIpLocationDownloadsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            RequestIpLocationDownloadsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RequestIpLocationDownloadsAction.Request::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.taskQueue = clusterService.createTaskQueue("ip-location-download-consumers-update", Priority.NORMAL, TASK_EXECUTOR);
    }

    @Override
    protected void masterOperation(
        Task task,
        RequestIpLocationDownloadsAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        taskQueue.submitTask(
            "ip-location-download-consumers-" + request.operation().label() + "-" + request.getConsumer(),
            new UpdateConsumersTask(request.getProjectId(), listener, request.getConsumer(), request.operation()),
            null
        );
    }

    record UpdateConsumersTask(
        ProjectId projectId,
        ActionListener<AcknowledgedResponse> listener,
        IpLocationConsumer consumer,
        Operation operation
    ) implements ClusterStateTaskListener {

        ClusterState execute(ClusterState currentState) {
            if (currentState.metadata().hasProject(projectId) == false) {
                return currentState;
            }
            ProjectMetadata project = currentState.metadata().getProject(projectId);
            IpLocationDownloadConsumers current = project.custom(IpLocationDownloadConsumers.TYPE, IpLocationDownloadConsumers.EMPTY);
            IpLocationDownloadConsumers updated = switch (operation) {
                case REGISTER -> current.withConsumer(consumer);
                case UNREGISTER -> current.withoutConsumer(consumer);
            };
            if (current == updated) {
                return currentState;
            }
            return ClusterState.builder(currentState)
                .putProjectMetadata(ProjectMetadata.builder(project).putCustom(IpLocationDownloadConsumers.TYPE, updated))
                .build();
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(
                () -> Strings.format(
                    "failed to apply ip-location download consumer update for project [%s]: %s consumer [%s]",
                    projectId,
                    operation.label(),
                    consumer
                ),
                e
            );
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(RequestIpLocationDownloadsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
