/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.transitions.steps.MarkIndexForDLMForceMergeAction;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;
import static org.elasticsearch.datastreams.lifecycle.transitions.steps.MarkIndexForDLMForceMergeAction.DLM_INDEX_FOR_FORCE_MERGE_KEY;

/**
 * Task to update the cluster state with the force merge marker for DLM Tiers.
 * Public for testing.
 */
public class MarkIndexForDlmForceMergeTask extends AckedBatchedClusterStateUpdateTask implements ClusterStateTaskListener {
    private final ActionListener<AcknowledgedResponse> listener;
    private final ProjectId projectId;
    private final String originalIndex;
    private final String indexToBeForceMerged;
    private final Logger logger = LogManager.getLogger(MarkIndexForDlmForceMergeTask.class);

    MarkIndexForDlmForceMergeTask(
        ActionListener<AcknowledgedResponse> listener,
        ProjectId projectId,
        MarkIndexForDLMForceMergeAction.Request request
    ) {
        super(TimeValue.THIRTY_SECONDS, listener);
        this.listener = listener;
        this.projectId = projectId;
        this.originalIndex = request.getOriginalIndex();
        this.indexToBeForceMerged = request.getIndexToBeForceMerged();
    }

    ClusterState execute(ClusterState currentState) {
        ProjectMetadata projectMetadata = currentState.metadata().getProject(this.projectId);
        if (projectMetadata == null) {
            return currentState;
        }
        IndexMetadata originalIndexMetadata = projectMetadata.index(originalIndex);
        IndexMetadata cloneIndexMetadata = projectMetadata.index(indexToBeForceMerged);
        if (originalIndexMetadata == null) {
            // If the source index doesn't exist, we can't mark it for force merge. Return the unchanged cluster state.
            String errorMessage = Strings.format(
                "DLM attempted to mark clone index [%s] for force merge for original index [%s] but index no longer exists",
                indexToBeForceMerged,
                originalIndex
            );
            logger.warn(errorMessage);
            return currentState;
        }

        if (cloneIndexMetadata == null) {
            // If the clone index doesn't exist, this is an unexpected error - the clone should have been created before this task
            String errorMessage = Strings.format(
                "clone index [%s] doesn't exist but was expected to exist when marking index [%s] for DLM force merge",
                indexToBeForceMerged,
                originalIndex
            );
            logger.warn(errorMessage);
            return currentState;
        }

        Map<String, String> existingCustomMetadata = originalIndexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
        Map<String, String> customMetadata = new HashMap<>();
        if (existingCustomMetadata != null) {
            if (indexToBeForceMerged.equals(existingCustomMetadata.get(DLM_INDEX_FOR_FORCE_MERGE_KEY))) {
                // Index is already marked for force merge, no update needed
                return currentState;
            }
            customMetadata.putAll(existingCustomMetadata);
        }
        customMetadata.put(DLM_INDEX_FOR_FORCE_MERGE_KEY, indexToBeForceMerged);

        IndexMetadata updatedOriginalIndexMetadata = new IndexMetadata.Builder(originalIndexMetadata).putCustom(
            LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
            customMetadata
        ).build();

        final ProjectMetadata.Builder updatedProject = ProjectMetadata.builder(currentState.metadata().getProject(projectId))
            .put(updatedOriginalIndexMetadata, true);
        return ClusterState.builder(currentState).putProjectMetadata(updatedProject).build();
    }

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }
}
