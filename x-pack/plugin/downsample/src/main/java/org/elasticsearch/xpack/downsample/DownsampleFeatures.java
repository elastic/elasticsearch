/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

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

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(CUMULATIVE_HISTOGRAM_CONSECUTIVE_RESETS_FIX);
    }
}
