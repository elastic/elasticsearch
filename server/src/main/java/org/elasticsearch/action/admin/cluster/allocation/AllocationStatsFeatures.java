/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class AllocationStatsFeatures implements FeatureSpecification {
    public static final NodeFeature INCLUDE_DISK_THRESHOLD_SETTINGS = new NodeFeature("stats.include_disk_thresholds");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(INCLUDE_DISK_THRESHOLD_SETTINGS);
    }
}
