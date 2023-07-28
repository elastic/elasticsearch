/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * ActionType that is used by executor node to indicate that the persistent action finished or failed on the node and needs to be
 * removed from the cluster state in case of successful completion or restarted on some other node in case of failure.
 */
public class CompletionPersistentTaskAction extends ActionType<PersistentTaskResponse> {

    public static final CompletionPersistentTaskAction INSTANCE = new CompletionPersistentTaskAction();
    public static final String NAME = "cluster:admin/persistent/completion";

    private CompletionPersistentTaskAction() {
        super(NAME, PersistentTaskResponse::new);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private String taskId;

        private Exception exception;

        private long allocationId = -1;

        private String localAbortReason;

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            taskId = in.readString();
            allocationId = in.readLong();
            exception = in.readException();
            localAbortReason = in.readOptionalString();
        }

        public Request(String taskId, long allocationId, Exception exception, String localAbortReason) {
            this.taskId = taskId;
            this.exception = exception;
            this.allocationId = allocationId;
            this.localAbortReason = localAbortReason;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(taskId);
            out.writeLong(allocationId);
            out.writeException(exception);
            out.writeOptionalString(localAbortReason);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (taskId == null) {
                validationException = addValidationError("task id is missing", validationException);
            }
            if (allocationId < 0) {
                validationException = addValidationError("allocation id is negative or missing", validationException);
            }
            if (exception != null && localAbortReason != null) {
                validationException = addValidationError("task cannot be both locally aborted and failed", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(taskId, request.taskId)
                && allocationId == request.allocationId
                && Objects.equals(exception, request.exception)
                && Objects.equals(localAbortReason, request.localAbortReason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId, allocationId, exception, localAbortReason);
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
                CompletionPersistentTaskAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                indexNameExpressionResolver,
                PersistentTaskResponse::new,
                ThreadPool.Names.GENERIC
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
            ClusterState state,
            final ActionListener<PersistentTaskResponse> listener
        ) {
            if (request.localAbortReason != null) {
                assert request.exception == null
                    : "request has both exception " + request.exception + " and local abort reason " + request.localAbortReason;
                persistentTasksClusterService.unassignPersistentTask(
                    request.taskId,
                    request.allocationId,
                    request.localAbortReason,
                    listener.map(PersistentTaskResponse::new)
                );
            } else {
                persistentTasksClusterService.completePersistentTask(
                    request.taskId,
                    request.allocationId,
                    request.exception,
                    listener.map(PersistentTaskResponse::new)
                );
            }
        }
    }
}
