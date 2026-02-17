/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.MinimalServiceSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Container for cluster state update tasks used by {@link ModelRegistry} to update
 * {@link ModelRegistryClusterStateMetadata}. Contains the abstract {@link MetadataTask} and its
 * concrete implementations.
 */
public final class ModelRegistryMetadataTask {

    private ModelRegistryMetadataTask() {}

    public record ModelAndSettings(String inferenceEntityId, MinimalServiceSettings settings) {}

    public abstract static class MetadataTask extends AckedBatchedClusterStateUpdateTask {
        private final ProjectId projectId;

        MetadataTask(ProjectId projectId, ActionListener<AcknowledgedResponse> listener) {
            super(TimeValue.THIRTY_SECONDS, listener);
            this.projectId = projectId;
        }

        abstract ModelRegistryClusterStateMetadata executeTask(ModelRegistryClusterStateMetadata current);

        public ProjectId getProjectId() {
            return projectId;
        }
    }

    public static class UpgradeModelsMetadataTask extends MetadataTask {
        private final Map<String, MinimalServiceSettings> fromIndex;

        public UpgradeModelsMetadataTask(
            ProjectId projectId,
            Map<String, MinimalServiceSettings> fromIndex,
            ActionListener<AcknowledgedResponse> listener
        ) {
            super(projectId, listener);
            this.fromIndex = fromIndex;
        }

        @Override
        ModelRegistryClusterStateMetadata executeTask(ModelRegistryClusterStateMetadata current) {
            return current.withUpgradedModels(fromIndex);
        }
    }

    public static class AddModelMetadataTask extends MetadataTask {
        private final List<ModelAndSettings> models = new ArrayList<>();

        public AddModelMetadataTask(ProjectId projectId, List<ModelAndSettings> models, ActionListener<AcknowledgedResponse> listener) {
            super(projectId, listener);
            this.models.addAll(models);
        }

        @Override
        ModelRegistryClusterStateMetadata executeTask(ModelRegistryClusterStateMetadata current) {
            return current.withAddedModels(models);
        }
    }

    public static class DeleteModelMetadataTask extends MetadataTask {
        private final Set<String> inferenceEntityIds;

        public DeleteModelMetadataTask(ProjectId projectId, Set<String> inferenceEntityIds, ActionListener<AcknowledgedResponse> listener) {
            super(projectId, listener);
            this.inferenceEntityIds = inferenceEntityIds;
        }

        @Override
        ModelRegistryClusterStateMetadata executeTask(ModelRegistryClusterStateMetadata current) {
            return current.withRemovedModel(inferenceEntityIds);
        }
    }
}
