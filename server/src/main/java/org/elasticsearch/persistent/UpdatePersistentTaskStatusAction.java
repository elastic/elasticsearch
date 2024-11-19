/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class UpdatePersistentTaskStatusAction {

    public static final ActionType<PersistentTaskResponse> INSTANCE = new ActionType<>("cluster:admin/persistent/update_status");

    private UpdatePersistentTaskStatusAction() {/* no instances */}

    public static class Request extends MasterNodeRequest<Request> {
        private final String taskId;
        private final long allocationId;
        private final PersistentTaskState state;

        public Request(StreamInput in) throws IOException {
            super(in);
            taskId = in.readString();
            allocationId = in.readLong();
            state = in.readOptionalNamedWriteable(PersistentTaskState.class);
        }

        public Request(String taskId, long allocationId, PersistentTaskState state) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            this.taskId = taskId;
            this.allocationId = allocationId;
            this.state = state;
        }

        public String getTaskId() {
            return taskId;
        }

        public long getAllocationId() {
            return allocationId;
        }

        public PersistentTaskState getState() {
            return state;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(taskId);
            out.writeLong(allocationId);
            out.writeOptionalNamedWriteable(state);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (this.taskId == null) {
                validationException = addValidationError("task id must be specified", validationException);
            }
            if (this.allocationId == -1L) {
                validationException = addValidationError("allocationId must be specified", validationException);
            }
            // We cannot really check if status has the same type as task because we don't have access
            // to the task here. We will check it when we try to update the task
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(taskId, request.taskId) && allocationId == request.allocationId && Objects.equals(state, request.state);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId, allocationId, state);
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, PersistentTaskResponse> {

        private final PersistentTasksClusterService persistentTasksClusterService;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            PersistentTasksClusterService persistentTasksClusterService,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                INSTANCE.name(),
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                indexNameExpressionResolver,
                PersistentTaskResponse::new,
                threadPool.executor(ThreadPool.Names.MANAGEMENT)
            );
            this.persistentTasksClusterService = persistentTasksClusterService;
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // Cluster is not affected but we look up repositories in metadata
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected final void masterOperation(
            Task ignoredTask,
            final Request request,
            final ClusterState state,
            final ActionListener<PersistentTaskResponse> listener
        ) {
            persistentTasksClusterService.updatePersistentTaskState(
                request.taskId,
                request.allocationId,
                request.state,
                listener.map(PersistentTaskResponse::new)
            );
        }
    }
}
