/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

public final class InitializePolicyContextStep extends ClusterStateActionStep {
    public static final String INITIALIZATION_PHASE = "new";
    public static final StepKey KEY = new StepKey(INITIALIZATION_PHASE, "init", "init");
    private static final Logger logger = LogManager.getLogger(InitializePolicyContextStep.class);

    public InitializePolicyContextStep(Step.StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        IndexMetaData indexMetaData = clusterState.getMetaData().index(index);
        if (indexMetaData == null) {
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
            // Index must have been since deleted, ignore it
            return clusterState;
        }
        Settings settings = indexMetaData.getSettings();
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
