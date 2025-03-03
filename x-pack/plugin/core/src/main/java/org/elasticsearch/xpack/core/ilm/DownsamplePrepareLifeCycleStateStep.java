/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import static org.elasticsearch.action.downsample.DownsampleConfig.generateDownsampleIndexName;
import static org.elasticsearch.xpack.core.ilm.DownsampleAction.DOWNSAMPLED_INDEX_PREFIX;

/**
 * An ILM step that sets the target index to use in the {@link DownsampleStep}.
 * The reason why this is done in a separate step and stored in {@link LifecycleExecutionState},
 * is because other steps after downsampling also depend on the target index generated here.
 */
public class DownsamplePrepareLifeCycleStateStep extends ClusterStateActionStep {

    private static final Logger LOGGER = LogManager.getLogger(DownsamplePrepareLifeCycleStateStep.class);
    public static final String NAME = "generate-downsampled-index-name";
    private final DateHistogramInterval fixedInterval;

    public DownsamplePrepareLifeCycleStateStep(StepKey key, StepKey nextStepKey, DateHistogramInterval fixedInterval) {
        super(key, nextStepKey);
        this.fixedInterval = fixedInterval;
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        IndexMetadata indexMetadata = clusterState.metadata().getProject().index(index);
        if (indexMetadata == null) {
            // Index must have been since deleted, ignore it
            LOGGER.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().action(), index.getName());
            return clusterState;
        }

        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();

        LifecycleExecutionState.Builder newLifecycleState = LifecycleExecutionState.builder(lifecycleState);
        final String downsampleIndexName = generateDownsampleIndexName(DOWNSAMPLED_INDEX_PREFIX, indexMetadata, fixedInterval);
        newLifecycleState.setDownsampleIndexName(downsampleIndexName);

        return LifecycleExecutionStateUtils.newClusterStateWithLifecycleState(
            clusterState,
            indexMetadata.getIndex(),
            newLifecycleState.build()
        );
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

}
