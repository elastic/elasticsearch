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
 * new index has been created. Useful for actions such as shrink.
 */
public class CopyExecutionStateStep extends ClusterStateActionStep {
    public static final String NAME = "copy-execution-state";

    private static final Logger logger = LogManager.getLogger(CopyExecutionStateStep.class);

    private String shrunkIndexPrefix;


    public CopyExecutionStateStep(StepKey key, StepKey nextStepKey, String shrunkIndexPrefix) {
        super(key, nextStepKey);
        this.shrunkIndexPrefix = shrunkIndexPrefix;
    }

    String getShrunkIndexPrefix() {
        return shrunkIndexPrefix;
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
        // get target shrink index
        String targetIndexName = shrunkIndexPrefix + indexName;
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
        relevantTargetCustomData.setAction(action);
        relevantTargetCustomData.setStep(ShrunkenIndexCheckStep.NAME);

        Metadata.Builder newMetadata = Metadata.builder(clusterState.getMetadata())
            .put(IndexMetadata.builder(targetIndexMetadata)
                .putCustom(ILM_CUSTOM_METADATA_KEY, relevantTargetCustomData.build().asMap()));

        return ClusterState.builder(clusterState).metadata(newMetadata).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CopyExecutionStateStep that = (CopyExecutionStateStep) o;
        return Objects.equals(shrunkIndexPrefix, that.shrunkIndexPrefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), shrunkIndexPrefix);
    }
}
