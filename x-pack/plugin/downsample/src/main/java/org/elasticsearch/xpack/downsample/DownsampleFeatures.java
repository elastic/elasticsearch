/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xpack.downsample.orchestration.DownsampleStateIndex;

import java.util.Set;

public class DownsampleFeatures implements FeatureSpecification {

    /**
     * Marks the fix for a bug where two consecutive structural resets within the same downsample bucket
     * caused the same (field, timestamp) pair to be emitted twice in the reset boundary document,
     * producing an {@code XContentParseException: Duplicate field} and permanently failing the shard
     * downsample task.
     */
    public static final NodeFeature CUMULATIVE_HISTOGRAM_CONSECUTIVE_RESETS_FIX = new NodeFeature(
        "downsample.cumulative_histogram_consecutive_resets_fix"
    );

    /**
     * The node runs downsample operations through the system index (master orchestration, shard tasks polled by the nodes). Only
     * advertised with the feature flag, so that the downsample API switches to the new path only once every node can run it.
     */
    public static final NodeFeature SYSTEM_INDEX_ORCHESTRATION = new NodeFeature("downsample.system_index_orchestration");

    @Override
    public Set<NodeFeature> getFeatures() {
        return DownsampleStateIndex.FEATURE_FLAG.isEnabled()
            ? Set.of(CUMULATIVE_HISTOGRAM_CONSECUTIVE_RESETS_FIX, SYSTEM_INDEX_ORCHESTRATION)
            : Set.of(CUMULATIVE_HISTOGRAM_CONSECUTIVE_RESETS_FIX);
    }
}
