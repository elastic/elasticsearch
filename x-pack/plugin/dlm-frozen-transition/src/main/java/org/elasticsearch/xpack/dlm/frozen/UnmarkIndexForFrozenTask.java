/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.datastreams.DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY;

/**
 * Task to update the cluster state to remove the frozen conversion flag from an index
 */
public class UnmarkIndexForFrozenTask extends AckedBatchedClusterStateUpdateTask implements ClusterStateTaskListener {
    private final ActionListener<AcknowledgedResponse> listener;
    private final ProjectId projectId;
    private final String indexName;
    private final Logger logger = LogManager.getLogger(UnmarkIndexForFrozenTask.class);

    UnmarkIndexForFrozenTask(ProjectId projectId, String indexName, ActionListener<AcknowledgedResponse> listener) {
        super(TimeValue.THIRTY_SECONDS, listener);
        this.listener = listener;
        this.projectId = projectId;
        this.indexName = indexName;
    }

    ClusterState execute(ClusterState currentState) {
        ProjectMetadata projectMetadata = currentState.metadata().getProject(this.projectId);
        if (projectMetadata == null) {
            return currentState;
        }
        IndexMetadata originalIndexMetadata = projectMetadata.index(indexName);
        if (originalIndexMetadata == null) {
            logger.debug("DLM attempted to unmark index [{}] for frozen conversion, but index no longer exists", indexName);
            return currentState;
        }

        Map<String, String> existingCustomMetadata = originalIndexMetadata.getCustomData(LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
        Map<String, String> customMetadata = new HashMap<>();
        if (existingCustomMetadata != null) {
            if (existingCustomMetadata.containsKey(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY) == false) {
                // Index is already unmarked, no update needed
                return currentState;
            }
            customMetadata.putAll(existingCustomMetadata);
        }
        customMetadata.remove(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY);

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
