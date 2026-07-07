/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject.action;

import org.elasticsearch.ResourceAlreadyExistsException;
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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class PutProjectAction extends ActionType<AcknowledgedResponse> {

    public static final PutProjectAction INSTANCE = new PutProjectAction();
    public static final String NAME = "cluster:admin/projects/put";

    public PutProjectAction() {
        super(NAME);
    }

    @Override
    public String name() {
        return NAME;
    }

    public static class TransportPutProjectAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {
        private final MasterServiceTaskQueue<CreateProjectTask> createProjectQueue;
        private final MasterServiceTaskQueue<RemoveCreationBlockTask> removeCreationBlockQueue;

        @Inject
        public TransportPutProjectAction(
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
            this.createProjectQueue = clusterService.createTaskQueue("create-project", Priority.NORMAL, new CreateProjectExecutor());
            this.removeCreationBlockQueue = clusterService.createTaskQueue(
                "remove-project-creation-block",
                Priority.NORMAL,
                new RemoveCreationBlockExecutor()
            );
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        /**
         * Project creation is two-phase, mirroring the real project lifecycle: the project is created together with
         * {@link ProjectMetadata#PROJECT_UNDER_CREATION_BLOCK}, then that block is removed once creation is complete.
         * Each phase is its own cluster state update, so the resulting {@link org.elasticsearch.cluster.ClusterChangedEvent
         * ClusterChangedEvent}'s project delta correctly reports the project as {@code initializing} and then
         * {@code initialized}, rather than skipping straight to ready in one update.
         */
        @Override
        protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
            throws Exception {
            SubscribableListener.<Void>newForked(
                l -> createProjectQueue.submitTask(
                    "create-project " + request.projectId,
                    new CreateProjectTask(request.projectId, l),
                    request.masterNodeTimeout()
                )
            )
                .<Void>andThen(
                    (l, ignored) -> removeCreationBlockQueue.submitTask(
                        "remove-project-creation-block " + request.projectId,
                        new RemoveCreationBlockTask(request.projectId, l),
                        request.masterNodeTimeout()
                    )
                )
                .addListener(listener.map(ignored -> AcknowledgedResponse.TRUE));
        }
    }

    record CreateProjectTask(ProjectId projectId, ActionListener<Void> listener) implements ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    static class CreateProjectExecutor implements ClusterStateTaskExecutor<CreateProjectTask> {

        @Override
        public ClusterState execute(BatchExecutionContext<CreateProjectTask> batchExecutionContext) throws Exception {
            final ClusterState initialState = batchExecutionContext.initialState();
            final Set<ProjectId> knownProjectIds = new HashSet<>(initialState.metadata().projects().keySet());
            var stateBuilder = ClusterState.builder(initialState);
            var blocksBuilder = ClusterBlocks.builder(initialState.blocks());
            for (TaskContext<CreateProjectTask> taskContext : batchExecutionContext.taskContexts()) {
                try {
                    ProjectId projectId = taskContext.getTask().projectId();
                    if (knownProjectIds.contains(projectId)) {
                        throw new ResourceAlreadyExistsException("project [{}] already exists", projectId);
                    }
                    stateBuilder.putProjectMetadata(ProjectMetadata.builder(projectId));
                    blocksBuilder.addProjectGlobalBlock(projectId, ProjectMetadata.PROJECT_UNDER_CREATION_BLOCK);
                    knownProjectIds.add(projectId);
                    taskContext.success(() -> taskContext.getTask().listener().onResponse(null));
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }
            return stateBuilder.blocks(blocksBuilder).build();
        }
    }

    record RemoveCreationBlockTask(ProjectId projectId, ActionListener<Void> listener) implements ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    static class RemoveCreationBlockExecutor extends SimpleBatchedExecutor<RemoveCreationBlockTask, Void> {

        @Override
        public Tuple<ClusterState, Void> executeTask(RemoveCreationBlockTask task, ClusterState clusterState) {
            final var nextState = ClusterState.builder(clusterState)
                .blocks(
                    ClusterBlocks.builder(clusterState.blocks())
                        .removeProjectGlobalBlock(task.projectId(), ProjectMetadata.PROJECT_UNDER_CREATION_BLOCK)
                        .build()
                )
                .build();
            return Tuple.tuple(nextState, null);
        }

        @Override
        public void taskSucceeded(RemoveCreationBlockTask task, Void unused) {
            task.listener().onResponse(null);
        }
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private static final Pattern VALID_PROJECT_ID_PATTERN = Pattern.compile("[-_a-zA-Z0-9]+");

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
            } else if (VALID_PROJECT_ID_PATTERN.matcher(projectId.id()).matches() == false) {
                validationException = ValidateActions.addValidationError(
                    "project id may only contain alpha numeric characters (received [" + projectId + "])",
                    validationException
                );
            }
            return validationException;
        }
    }
}
