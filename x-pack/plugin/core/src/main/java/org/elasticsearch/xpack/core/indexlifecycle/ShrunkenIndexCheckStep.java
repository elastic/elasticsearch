/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.Index;

public class ShrunkenIndexCheckStep extends ClusterStateWaitStep {
    public static final String NAME = "is-shrunken-index";

    public ShrunkenIndexCheckStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isConditionMet(Index index, ClusterState clusterState) {
        String shrunkenIndexSource = IndexMetaData.INDEX_SHRINK_SOURCE_NAME.get(
            clusterState.metaData().index(index).getSettings());
        if (Strings.isNullOrEmpty(shrunkenIndexSource)) {
            throw new IllegalStateException("step[" + NAME + "] is checking an un-shrunken index[" + index.getName() + "]");
        }
        return index.getName().equals(ShrinkStep.SHRUNKEN_INDEX_PREFIX + shrunkenIndexSource);
    }
}
