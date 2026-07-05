/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoriesService;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This task takes a set of indices, and adds custom metadata to the {@link DataStreamsPlugin#LIFECYCLE_CUSTOM_INDEX_METADATA_KEY} map
 * that indicates the index is ready to be converted into a frozen index. The metadata is a key-value pair of
 * {@link DataStreamLifecycleService#FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY} and a String which is the currently configured
 * "repositories.default_repository" setting, defined in {@link RepositoriesService#DEFAULT_REPOSITORY_SETTING}.
 *
 * If the default repository is not configured, the cluster state update is a no-op.
 */
public class MarkIndicesForFrozenTask extends AckedBatchedClusterStateUpdateTask {
    private static final Logger logger = LogManager.getLogger(MarkIndicesForFrozenTask.class);
    private final Set<Index> indicesToMarkForFrozen;
    private final ProjectId projectId;

    public MarkIndicesForFrozenTask(ProjectId projectId, Set<Index> indicesToMarkForFrozen, ActionListener<AcknowledgedResponse> listener) {
        super(TimeValue.THIRTY_SECONDS, listener);
        this.indicesToMarkForFrozen = indicesToMarkForFrozen;
        this.projectId = projectId;
    }

    ClusterState execute(ClusterState currentState) {
        final ProjectMetadata projectMetadata = currentState.metadata().getProject(this.projectId);
        if (projectMetadata == null) {
            return currentState;
        }
        final String defaultRepository = RepositoriesService.DEFAULT_REPOSITORY_SETTING.get(currentState.getMetadata().settings());
        if (Strings.hasText(defaultRepository) == false) {
            logger.debug("DLM skipping marking indices as ready for frozen conversion, because no default repository has been configured");
            return currentState;
        }

        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectMetadata);
        boolean changed = false;
        for (Index index : indicesToMarkForFrozen) {
            final IndexMetadata indexMetadata = projectMetadata.index(index);
            if (indexMetadata == null) {
                logger.trace(
                    "DLM tried to mark [{}] as ready to be converted to a frozen index, but it does not exist, ignoring",
                    index.getName()
                );
                continue;
            }

            Map<String, String> existingMetadata = indexMetadata.getCustomData(DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY);
            Map<String, String> newMetadata = new HashMap<>();
            if (existingMetadata != null) {
                if (defaultRepository.equals(existingMetadata.get(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY))) {
                    // This index has already been marked as ready for frozen conversion, and with the same repository, so skip it
                    continue;
                }
                // Make sure we don't lose the existing metadata
                newMetadata.putAll(existingMetadata);
            }

            // Update the custom metadata with the key (dlm_freeze_with) and value of the currently configured default repository
            newMetadata.put(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, defaultRepository);
            final IndexMetadata updatedMetadata = IndexMetadata.builder(indexMetadata)
                .putCustom(DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY, newMetadata)
                .build();

            projectBuilder.put(updatedMetadata, true);
            changed = true;
        }

        if (changed) {
            return ClusterState.builder(currentState).putProjectMetadata(projectBuilder.build()).build();
        } else {
            logger.debug("DLM marking {} indices as ready for frozen ended in a no-op", indicesToMarkForFrozen.size());
            return currentState;
        }
    }
}
