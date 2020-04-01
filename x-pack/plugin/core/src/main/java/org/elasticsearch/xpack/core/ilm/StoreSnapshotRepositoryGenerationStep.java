/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.index.Index;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.fromIndexMetadata;

/**
 * Stores the current snapshot repository safe generation as stored in the {@link RepositoryMetaData#generation()} into the ILM execution
 * state.
 * Actions that need to wait for a snapshot status to change could make use of this step to store the target repository generation before
 * they start creating the snapshot and wait for the generation to change before moving forward.
 */
public class StoreSnapshotRepositoryGenerationStep extends ClusterStateActionStep {

    public static final String NAME = "store-repository-generation";

    private static final Logger logger = LogManager.getLogger(StoreSnapshotRepositoryGenerationStep.class);

    private final String snapshotRepository;

    public StoreSnapshotRepositoryGenerationStep(StepKey key, StepKey nextStepKey, String snapshotRepository) {
        super(key, nextStepKey);
        this.snapshotRepository = snapshotRepository;
    }

    public String getSnapshotRepository() {
        return snapshotRepository;
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index);
        if (indexMetaData == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
            return clusterState;
        }

        RepositoryMetaData repositoryMetadata =
            clusterState.getMetaData().<RepositoriesMetaData>custom(RepositoriesMetaData.TYPE).repository(snapshotRepository);
        if (repositoryMetadata == null) {
            String policyName = indexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
            String errorMessage = "repository [" + snapshotRepository + "] is missing. [" + policyName +
                "] policy for index [" + indexMetaData.getIndex().getName() + "] cannot continue until the repository is created";
            logger.warn(errorMessage);
            throw new IllegalStateException(errorMessage);
        }

        ClusterState.Builder newClusterStateBuilder = ClusterState.builder(clusterState);

        LifecycleExecutionState lifecycleState = fromIndexMetadata(indexMetaData);
        LifecycleExecutionState.Builder newCustomData = LifecycleExecutionState.builder(lifecycleState);
        newCustomData.setRepositoryGeneration(repositoryMetadata.generation());
        IndexMetaData.Builder indexMetadataBuilder = IndexMetaData.builder(indexMetaData);
        indexMetadataBuilder.putCustom(ILM_CUSTOM_METADATA_KEY, newCustomData.build().asMap());
        newClusterStateBuilder.metaData(MetaData.builder(clusterState.getMetaData()).put(indexMetadataBuilder));
        return newClusterStateBuilder.build();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), snapshotRepository);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        StoreSnapshotRepositoryGenerationStep other = (StoreSnapshotRepositoryGenerationStep) obj;
        return super.equals(obj) &&
            Objects.equals(snapshotRepository, other.snapshotRepository);
    }
}
