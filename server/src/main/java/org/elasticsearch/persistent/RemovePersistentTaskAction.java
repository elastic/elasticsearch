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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.persistent.PersistentTasksClusterService.resolveProjectIdHint;

public class RemovePersistentTaskAction {

    public static final ActionType<PersistentTaskResponse> INSTANCE = new ActionType<>("cluster:admin/persistent/remove");

    private RemovePersistentTaskAction() {/* no instances */}

    public static class Request extends MasterNodeRequest<Request> {

        private final String taskId;

        public Request(StreamInput in) throws IOException {
            super(in);
            taskId = in.readString();
        }

        public Request(TimeValue masterNodeTimeout, String taskId) {
            super(masterNodeTimeout);
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
        private final ProjectResolver projectResolver;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            PersistentTasksClusterService persistentTasksClusterService,
            ProjectResolver projectResolver
        ) {
            super(
                INSTANCE.name(),
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                PersistentTaskResponse::new,
                threadPool.executor(ThreadPool.Names.MANAGEMENT)
            );
            this.persistentTasksClusterService = persistentTasksClusterService;
            this.projectResolver = projectResolver;
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
            // Try resolve the project-id which may be null if the request is for a cluster-scope task.
            // A non-null project-id does not guarantee the task is project-scope. This will be determined
            // later by checking the taskName associated with the task-id.
            final ProjectId projectIdHint = resolveProjectIdHint(projectResolver);

            persistentTasksClusterService.removePersistentTask(projectIdHint, request.taskId, listener.map(PersistentTaskResponse::new));
        }
    }
}
