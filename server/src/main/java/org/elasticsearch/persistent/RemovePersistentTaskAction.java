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

public class RemovePersistentTaskAction extends ActionType<PersistentTaskResponse> {

    public static final RemovePersistentTaskAction INSTANCE = new RemovePersistentTaskAction();
    public static final String NAME = "cluster:admin/persistent/remove";

    private RemovePersistentTaskAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private String taskId;

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            taskId = in.readString();
        }

        public Request(String taskId) {
            this.taskId = taskId;
        }

        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(taskId);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(taskId, request.taskId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId);
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
                RemovePersistentTaskAction.NAME,
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
            ClusterState state,
            final ActionListener<PersistentTaskResponse> listener
        ) {
            persistentTasksClusterService.removePersistentTask(request.taskId, listener.map(PersistentTaskResponse::new));
        }
    }
}
