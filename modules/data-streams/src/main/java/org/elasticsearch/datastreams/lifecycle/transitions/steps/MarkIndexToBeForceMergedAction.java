/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;

/**
 * Action to mark an index to be force merged by updating its custom metadata.
 */
public class MarkIndexToBeForceMergedAction {

    public static final ActionType<AcknowledgedResponse> INSTANCE = new ActionType<>("indices:admin/dlm/mark_to_force_merge");
    private static final String DLM_INDEX_TO_BE_MERGED_KEY = "dlm_index_to_be_force_merged";

    /**
     * Request to mark an index to be force merged.
     */
    public static class Request extends MasterNodeRequest<Request> {
        private final ProjectId projectId;
        private final String sourceIndex;
        private final String indexToBeForceMerged;

        public Request(ProjectId projectId, String sourceIndex, String indexToBeForceMerged, TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
            this.projectId = projectId;
            this.sourceIndex = sourceIndex;
            this.indexToBeForceMerged = indexToBeForceMerged;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.projectId = ProjectId.readFrom(in);
            this.sourceIndex = in.readString();
            this.indexToBeForceMerged = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            projectId.writeTo(out);
            out.writeString(sourceIndex);
            out.writeString(indexToBeForceMerged);
        }

        public ProjectId getProjectId() {
            return projectId;
        }

        public String getSourceIndex() {
            return sourceIndex;
        }

        public String getIndexToBeForceMerged() {
            return indexToBeForceMerged;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /**
     * Task to update the cluster state with the force merge marker.
     */
    static class UpdateTask implements ClusterStateTaskListener {
        private final ActionListener<AcknowledgedResponse> listener;
        private final ProjectId projectId;
        private final String sourceIndex;
        private final String indexToBeForceMerged;

        UpdateTask(ActionListener<AcknowledgedResponse> listener, ProjectId projectId, String sourceIndex, String indexToBeForceMerged) {
            this.listener = listener;
            this.projectId = projectId;
            this.sourceIndex = sourceIndex;
            this.indexToBeForceMerged = indexToBeForceMerged;
        }

        ClusterState execute(ClusterState currentState) {
            final ProjectMetadata currentProject = currentState.metadata().getProject(projectId);
            if (currentProject == null) {
                return currentState;
            }

            IndexMetadata sourceIndexMetadata = currentProject.index(sourceIndex);
            if (sourceIndexMetadata == null) {
                return currentState;
            }

            Map<String, String> existingCustomMetadata = sourceIndexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
            Map<String, String> customMetadata = new HashMap<>();
            if (existingCustomMetadata != null) {
                customMetadata.putAll(existingCustomMetadata);
            }
            customMetadata.put(DLM_INDEX_TO_BE_MERGED_KEY, indexToBeForceMerged);

            IndexMetadata updatedSourceIndexMetadata = new IndexMetadata.Builder(sourceIndexMetadata).putCustom(
                LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                customMetadata
            ).build();

            final ProjectMetadata.Builder updatedProject = ProjectMetadata.builder(currentProject).put(updatedSourceIndexMetadata, true);
            return ClusterState.builder(currentState).putProjectMetadata(updatedProject).build();
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Executor for the UpdateTask.
     */
    static class Executor implements ClusterStateTaskExecutor<UpdateTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<UpdateTask> batchExecutionContext) {
            var state = batchExecutionContext.initialState();
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                try {
                    state = taskContext.getTask().execute(state);
                    taskContext.success(() -> taskContext.getTask().listener.onResponse(AcknowledgedResponse.TRUE));
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }
            return state;
        }
    }

    /**
     * Helper class to manage the task queue for marking indices to be force merged.
     */
    public static class TaskQueueManager {
        private final MasterServiceTaskQueue<UpdateTask> taskQueue;

        public TaskQueueManager(ClusterService clusterService) {
            this.taskQueue = clusterService.createTaskQueue("dlm-mark-index-for-force-merge", Priority.LOW, new Executor());
        }

        public void submitTask(
            String sourceIndex,
            String indexToBeForceMerged,
            Request request,
            ActionListener<AcknowledgedResponse> listener
        ) {
            taskQueue.submitTask(
                Strings.format("marking index [%s] to be force merged for DLM", indexToBeForceMerged),
                new UpdateTask(listener, request.getProjectId(), sourceIndex, indexToBeForceMerged),
                null
            );
        }
    }
}
