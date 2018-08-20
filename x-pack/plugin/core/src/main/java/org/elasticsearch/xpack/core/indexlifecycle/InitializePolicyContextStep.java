/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

public final class InitializePolicyContextStep extends ClusterStateActionStep {
    public static final StepKey KEY = new StepKey("new", "init", "init");

    public InitializePolicyContextStep(Step.StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        Settings settings = clusterState.metaData().index(index).getSettings();
        if (settings.hasValue(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE)) {
            return clusterState;
        }

        ClusterState.Builder newClusterStateBuilder = ClusterState.builder(clusterState);
        IndexMetaData idxMeta = clusterState.getMetaData().index(index);
        Settings.Builder indexSettings = Settings.builder().put(idxMeta.getSettings())
            .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, idxMeta.getCreationDate());
        newClusterStateBuilder.metaData(MetaData.builder(clusterState.getMetaData()).put(IndexMetaData
            .builder(clusterState.getMetaData().index(index))
            .settings(indexSettings)));
        return newClusterStateBuilder.build();
    }
}
