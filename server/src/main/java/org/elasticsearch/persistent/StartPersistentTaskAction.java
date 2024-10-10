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

/**
 *  This action can be used to add the record for the persistent action to the cluster state.
 */
public class StartPersistentTaskAction {

    public static final ActionType<PersistentTaskResponse> INSTANCE = new ActionType<>("cluster:admin/persistent/start");

    private StartPersistentTaskAction() {/* no instances */}

    public static class Request extends MasterNodeRequest<Request> {
        private final String taskId;
        private final String taskName;
        private final PersistentTaskParams params;

        public Request(StreamInput in) throws IOException {
            super(in);
            taskId = in.readString();
            taskName = in.readString();
            params = in.readNamedWriteable(PersistentTaskParams.class);
        }

        public Request(String taskId, String taskName, PersistentTaskParams params) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            this.taskId = taskId;
            this.taskName = taskName;
            this.params = params;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(taskId);
            out.writeString(taskName);
            out.writeNamedWriteable(params);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (this.taskId == null) {
                validationException = addValidationError("task id must be specified", validationException);
            }
            if (this.taskName == null) {
                validationException = addValidationError("action must be specified", validationException);
            }
            if (params != null) {
                if (params.getWriteableName().equals(taskName) == false) {
                    validationException = addValidationError(
                        "params have to have the same writeable name as task. params: " + params.getWriteableName() + " task: " + taskName,
                        validationException
                    );
                }
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request1 = (Request) o;
            return Objects.equals(taskId, request1.taskId)
                && Objects.equals(taskName, request1.taskName)
                && Objects.equals(params, request1.params);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId, taskName, params);
        }

        public String getTaskName() {
            return taskName;
        }

        public String getTaskId() {
            return taskId;
        }

        public PersistentTaskParams getParams() {
            return params;
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
            PersistentTasksExecutorRegistry persistentTasksExecutorRegistry,
            PersistentTasksService persistentTasksService,
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
                threadPool.executor(ThreadPool.Names.GENERIC)
            );
            this.persistentTasksClusterService = persistentTasksClusterService;
            NodePersistentTasksExecutor executor = new NodePersistentTasksExecutor();
            clusterService.addListener(
                new PersistentTasksNodeService(
                    threadPool,
                    persistentTasksService,
                    persistentTasksExecutorRegistry,
                    transportService.getTaskManager(),
                    executor
                )
            );
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
            persistentTasksClusterService.createPersistentTask(
                request.taskId,
                request.taskName,
                request.params,
                listener.safeMap(PersistentTaskResponse::new)
            );
        }
    }
}
