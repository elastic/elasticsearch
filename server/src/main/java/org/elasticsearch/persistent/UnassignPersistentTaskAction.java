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
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
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

public class UnassignPersistentTaskAction extends ActionType<PersistentTaskResponse> {

    public static final UnassignPersistentTaskAction INSTANCE = new UnassignPersistentTaskAction();
    public static final String NAME = "cluster:internal/persistent/unassign";

    private UnassignPersistentTaskAction() {
        super(NAME, PersistentTaskResponse::new);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private String taskId;
        private long allocationId = -1L;
        private String reason;

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            taskId = in.readString();
            allocationId = in.readLong();
            reason = in.readString();
        }

        public Request(String taskId, long allocationId, String reason) {
            this.taskId = taskId;
            this.allocationId = allocationId;
            this.reason = reason;
        }

        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }

        public void setAllocationId(long allocationId) {
            this.allocationId = allocationId;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(taskId);
            out.writeLong(allocationId);
            out.writeString(reason);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (taskId == null) {
                validationException = addValidationError("task id must be specified", validationException);
            }
            if (allocationId == -1L) {
                validationException = addValidationError("allocationId must be specified", validationException);
            }
            if (reason == null) {
                validationException = addValidationError("reason must be specified", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(taskId, request.taskId) && allocationId == request.allocationId && Objects.equals(reason, request.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId, allocationId, reason);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, PersistentTaskResponse, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, UnassignPersistentTaskAction action) {
            super(client, action, new Request());
        }

        public final RequestBuilder setTaskId(String taskId) {
            request.setTaskId(taskId);
            return this;
        }

        public final RequestBuilder setAllocationId(long allocationId) {
            request.setAllocationId(allocationId);
            return this;
        }

        public final RequestBuilder setReason(String reason) {
            request.setReason(reason);
            return this;
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, PersistentTaskResponse> {

        private final PersistentTasksClusterService persistentTasksClusterService;

        @Inject
        public TransportAction(TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               PersistentTasksClusterService persistentTasksClusterService,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(UnassignPersistentTaskAction.NAME, transportService, clusterService, threadPool, actionFilters,
                Request::new, indexNameExpressionResolver, PersistentTaskResponse::new, ThreadPool.Names.MANAGEMENT);
            this.persistentTasksClusterService = persistentTasksClusterService;
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // Cluster is not affected but we look up repositories in metadata
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected final void masterOperation(Task ignoredTask, final Request request, ClusterState state,
                                             final ActionListener<PersistentTaskResponse> listener) {
            persistentTasksClusterService.unassignPersistentTask(request.taskId, request.allocationId, request.reason,
                listener.map(PersistentTaskResponse::new));
        }
    }
}
