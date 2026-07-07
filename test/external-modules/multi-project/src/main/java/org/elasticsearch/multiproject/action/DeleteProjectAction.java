/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class DeleteProjectAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteProjectAction INSTANCE = new DeleteProjectAction();
    public static final String NAME = "cluster:admin/projects/delete";

    private static final Logger logger = LogManager.getLogger(DeleteProjectAction.class);

    public DeleteProjectAction() {
        super(NAME);
    }

    @Override
    public String name() {
        return NAME;
    }

    public static class TransportDeleteProjectAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

        private final MasterServiceTaskQueue<AddDeletionBlockTask> addDeletionBlockQueue;
        private final MasterServiceTaskQueue<RemoveProjectTask> removeProjectQueue;

        @Inject
        public TransportDeleteProjectAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters
        ) {
            super(
                INSTANCE.name(),
                false,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                AcknowledgedResponse::readFrom,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            this.addDeletionBlockQueue = clusterService.createTaskQueue(
                "mark-project-for-deletion",
                Priority.NORMAL,
                new AddDeletionBlockExecutor()
            );
            this.removeProjectQueue = clusterService.createTaskQueue("delete-project", Priority.NORMAL, new RemoveProjectExecutor());
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        /**
         * Project deletion is two-phase, mirroring the real project lifecycle: {@link ProjectMetadata#PROJECT_UNDER_DELETION_BLOCK}
         * is added to the existing project first, then the project is actually removed from metadata and the routing table.
         * Each phase is its own cluster state update, so the resulting {@link org.elasticsearch.cluster.ClusterChangedEvent
         * ClusterChangedEvent}'s project delta correctly reports the project as {@code deleting} and then {@code deleted}.
         */
        @Override
        protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
            throws Exception {
            SubscribableListener.<Void>newForked(
                l -> addDeletionBlockQueue.submitTask(
                    "mark-project-for-deletion " + request.projectId,
                    new AddDeletionBlockTask(request.projectId, l),
                    request.masterNodeTimeout()
                )
            )
                .<Void>andThen(
                    (l, ignored) -> removeProjectQueue.submitTask(
                        "delete-project " + request.projectId,
                        new RemoveProjectTask(request.projectId, l),
                        request.masterNodeTimeout()
                    )
                )
                .addListener(listener.map(ignored -> AcknowledgedResponse.TRUE));
        }
    }

    record AddDeletionBlockTask(ProjectId projectId, ActionListener<Void> listener) implements ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    static class AddDeletionBlockExecutor extends SimpleBatchedExecutor<AddDeletionBlockTask, Void> {

        @Override
        public Tuple<ClusterState, Void> executeTask(AddDeletionBlockTask task, ClusterState clusterState) {
            if (clusterState.metadata().hasProject(task.projectId()) == false) {
                throw new IllegalArgumentException("project [" + task.projectId() + "] does not exist");
            }
            final var nextState = ClusterState.builder(clusterState)
                .blocks(
                    ClusterBlocks.builder(clusterState.blocks())
                        .addProjectGlobalBlock(task.projectId(), ProjectMetadata.PROJECT_UNDER_DELETION_BLOCK)
                        .build()
                )
                .build();
            return Tuple.tuple(nextState, null);
        }

        @Override
        public void taskSucceeded(AddDeletionBlockTask task, Void unused) {
            task.listener().onResponse(null);
        }
    }

    record RemoveProjectTask(ProjectId projectId, ActionListener<Void> listener) implements ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    static class RemoveProjectExecutor implements ClusterStateTaskExecutor<RemoveProjectTask> {

        @Override
        public ClusterState execute(BatchExecutionContext<RemoveProjectTask> batchExecutionContext) throws Exception {
            var metadataBuilder = Metadata.builder(batchExecutionContext.initialState().metadata());
            var routingTableBuilder = GlobalRoutingTable.builder(batchExecutionContext.initialState().globalRoutingTable());
            var clusterBlocksBuilder = ClusterBlocks.builder(batchExecutionContext.initialState().blocks());
            for (TaskContext<RemoveProjectTask> taskContext : batchExecutionContext.taskContexts()) {
                try {
                    ProjectId projectId = taskContext.getTask().projectId();
                    if (metadataBuilder.getProject(projectId) == null) {
                        taskContext.onFailure(new IllegalArgumentException("project [" + projectId + "] does not exist"));
                        continue;
                    }
                    metadataBuilder.removeProject(projectId);
                    routingTableBuilder.removeProject(projectId);
                    clusterBlocksBuilder.removeProject(projectId);
                    logger.info(
                        "Deleted project [{}] from cluster state version [{}]",
                        projectId,
                        batchExecutionContext.initialState().version()
                    );
                    taskContext.success(() -> taskContext.getTask().listener().onResponse(null));
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }
            return ClusterState.builder(batchExecutionContext.initialState())
                .metadata(metadataBuilder.build())
                .routingTable(routingTableBuilder.build())
                .blocks(clusterBlocksBuilder.build())
                .build();
        }
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final ProjectId projectId;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, ProjectId projectId) {
            super(masterNodeTimeout, ackTimeout);
            this.projectId = projectId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.projectId = ProjectId.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            projectId.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (projectId == null || projectId.id() == null || projectId.id().isEmpty()) {
                validationException = ValidateActions.addValidationError("project id is missing", validationException);
            }
            return validationException;
        }
    }
}
