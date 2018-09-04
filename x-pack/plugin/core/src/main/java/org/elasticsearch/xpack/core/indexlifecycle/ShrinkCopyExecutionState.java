/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.index.Index;

import java.util.Objects;

import static org.elasticsearch.xpack.core.indexlifecycle.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;

public class ShrinkCopyExecutionState extends ClusterStateActionStep {
    public static final String NAME = "copy_metadata";
    private String shrunkIndexPrefix;

    public ShrinkCopyExecutionState(StepKey key, StepKey nextStepKey, String shrunkIndexPrefix) {
        super(key, nextStepKey);
        this.shrunkIndexPrefix = shrunkIndexPrefix;
    }

    String getShrunkIndexPrefix() {
        return shrunkIndexPrefix;
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index);
        // get source index
        String indexName = indexMetaData.getIndex().getName();
        // get target shrink index
        String targetIndexName = shrunkIndexPrefix + indexName;
        IndexMetaData targetIndexMetaData = clusterState.metaData().index(targetIndexName);

        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        String phase = lifecycleState.getPhase();
        String action = lifecycleState.getAction();
        long lifecycleDate = lifecycleState.getIndexCreationDate();

        LifecycleExecutionState.Builder relevantTargetCustomData = LifecycleExecutionState.builder();
        relevantTargetCustomData.setIndexCreationDate(lifecycleDate);
        relevantTargetCustomData.setPhase(phase);
        relevantTargetCustomData.setAction(action);
        relevantTargetCustomData.setStep(ShrunkenIndexCheckStep.NAME);

        MetaData.Builder newMetaData = MetaData.builder(clusterState.getMetaData())
            .put(IndexMetaData.builder(targetIndexMetaData)
                .putCustom(ILM_CUSTOM_METADATA_KEY, relevantTargetCustomData.build().asMap()));

        return ClusterState.builder(clusterState).metaData(newMetaData).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ShrinkCopyExecutionState that = (ShrinkCopyExecutionState) o;
        return Objects.equals(shrunkIndexPrefix, that.shrunkIndexPrefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), shrunkIndexPrefix);
    }
}
