/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;

import java.util.function.LongSupplier;

public class PhaseAfterStep extends ClusterStateWaitStep {
    private final TimeValue after;
    private final LongSupplier nowSupplier;

    PhaseAfterStep(LongSupplier nowSupplier, TimeValue after, StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
        this.nowSupplier = nowSupplier;
        this.after = after;
    }

    @Override
    public boolean isConditionMet(Index index, ClusterState clusterState) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index);
        long lifecycleDate = indexMetaData.getSettings()
            .getAsLong(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, -1L);
        return nowSupplier.getAsLong() >= lifecycleDate + after.getMillis();
    }
}
