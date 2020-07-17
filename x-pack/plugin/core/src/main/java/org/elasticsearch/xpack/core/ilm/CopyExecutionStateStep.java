/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;

/**
 * Copies the execution state data from one index to another, typically after a
 * new index has been created. As part of the execution state copy it will set the target index
 * "current step" to the provided step name (part of the same phase and action as the current step's, unless
 * the "complete" step is configured in which case the action will be changed to "complete" as well)
 *
 * Useful for actions such as shrink.
 */
public class CopyExecutionStateStep extends ClusterStateActionStep {
    public static final String NAME = "copy-execution-state";

    private static final Logger logger = LogManager.getLogger(CopyExecutionStateStep.class);

    private final String targetIndexPrefix;
    private final String targetNextStepName;

    public CopyExecutionStateStep(StepKey key, StepKey nextStepKey, String targetIndexPrefix, String targetNextStepName) {
        super(key, nextStepKey);
        this.targetIndexPrefix = targetIndexPrefix;
        this.targetNextStepName = targetNextStepName;
    }

    String getTargetIndexPrefix() {
        return targetIndexPrefix;
    }

    String getTargetNextStepName() {
        return targetNextStepName;
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        IndexMetadata indexMetadata = clusterState.metadata().index(index);
        if (indexMetadata == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
            return clusterState;
        }
        // get source index
        String indexName = indexMetadata.getIndex().getName();
        // get target index
        String targetIndexName = targetIndexPrefix + indexName;
        IndexMetadata targetIndexMetadata = clusterState.metadata().index(targetIndexName);

        if (targetIndexMetadata == null) {
            logger.warn("[{}] index [{}] unable to copy execution state to target index [{}] as target index does not exist",
                getKey().getAction(), index.getName(), targetIndexName);
            throw new IllegalStateException("unable to copy execution state from [" + index.getName() +
                "] to [" + targetIndexName + "] as target index does not exist");
        }

        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);
        String phase = lifecycleState.getPhase();
        String action = lifecycleState.getAction();
        long lifecycleDate = lifecycleState.getLifecycleDate();

        LifecycleExecutionState.Builder relevantTargetCustomData = LifecycleExecutionState.builder();
        relevantTargetCustomData.setIndexCreationDate(lifecycleDate);
        relevantTargetCustomData.setPhase(phase);
        relevantTargetCustomData.setStep(targetNextStepName);
        if (targetNextStepName.equals(PhaseCompleteStep.NAME)) {
            relevantTargetCustomData.setAction(PhaseCompleteStep.NAME);
        } else {
            relevantTargetCustomData.setAction(action);
        }
        relevantTargetCustomData.setSnapshotRepository(lifecycleState.getSnapshotRepository());
        relevantTargetCustomData.setSnapshotName(lifecycleState.getSnapshotName());

        Metadata.Builder newMetadata = Metadata.builder(clusterState.getMetadata())
            .put(IndexMetadata.builder(targetIndexMetadata)
                .putCustom(ILM_CUSTOM_METADATA_KEY, relevantTargetCustomData.build().asMap()));

        return ClusterState.builder(clusterState).metadata(newMetadata).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        CopyExecutionStateStep that = (CopyExecutionStateStep) o;
        return Objects.equals(targetIndexPrefix, that.targetIndexPrefix) &&
            Objects.equals(targetNextStepName, that.targetNextStepName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), targetIndexPrefix, targetNextStepName);
    }
}
