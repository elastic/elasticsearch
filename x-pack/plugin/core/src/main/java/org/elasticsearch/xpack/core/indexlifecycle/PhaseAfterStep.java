/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;

import java.util.function.LongSupplier;

public class PhaseAfterStep extends ClusterStateWaitStep {
    private static final Logger logger = ESLoggerFactory.getLogger(PhaseAfterStep.class);
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
        logger.warn("checking phase[" + indexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_PHASE) + "]"
           + " after[" + after + "]");
        long lifecycleDate = indexMetaData.getSettings()
            .getAsLong(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, -1L);
        if (lifecycleDate < 0) {
            // TODO(talevy): make sure this setting is set before we find ourselves here
            logger.warn("index-lifecycle-setting for index" + index.getName() + "] not set");
            lifecycleDate = indexMetaData.getCreationDate();
        }
        return nowSupplier.getAsLong() >= lifecycleDate + after.getMillis();
    }
}
