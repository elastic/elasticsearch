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
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
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
        private final MasterServiceTaskQueue<PutProjectTask> putProjectTaskQueue;

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
            this.putProjectTaskQueue = clusterService.createTaskQueue("put-project", Priority.NORMAL, new PutProjectExecutor());
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
            throws Exception {
            putProjectTaskQueue.submitTask(
                "put-project " + request.projectId,
                new PutProjectTask(request, listener),
                request.masterNodeTimeout()
            );
        }
    }

    record PutProjectTask(Request request, ActionListener<AcknowledgedResponse> listener) implements ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    static class PutProjectExecutor implements ClusterStateTaskExecutor<PutProjectTask> {

        @Override
        public ClusterState execute(BatchExecutionContext<PutProjectTask> batchExecutionContext) throws Exception {
            final ClusterState initialState = batchExecutionContext.initialState();
            final Set<ProjectId> knownProjectIds = new HashSet<>(initialState.metadata().projects().keySet());
            var stateBuilder = ClusterState.builder(initialState);
            for (TaskContext<PutProjectTask> taskContext : batchExecutionContext.taskContexts()) {
                try {
                    Request request = taskContext.getTask().request();
                    if (knownProjectIds.contains(request.projectId)) {
                        throw new ResourceAlreadyExistsException("project [{}] already exists", request.projectId);
                    }
                    stateBuilder.putProjectMetadata(ProjectMetadata.builder(request.projectId));
                    knownProjectIds.add(request.projectId);
                    taskContext.success(() -> taskContext.getTask().listener.onResponse(AcknowledgedResponse.TRUE));
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }
            return stateBuilder.build();
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
