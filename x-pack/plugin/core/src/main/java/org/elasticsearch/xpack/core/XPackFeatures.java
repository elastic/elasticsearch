/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.core.datatiers.NodesDataTiersUsageTransportAction;

import java.util.Set;

/**
 * Provides the XPack features that this version of the code supports
 */
public class XPackFeatures implements FeatureSpecification {
    public static final NodeFeature LOGSDB_TELEMETRY = new NodeFeature("logsdb_telemetry");
    public static final NodeFeature LOGSDB_TELMETRY_STATS = new NodeFeature("logsdb_telemetry_stats");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(
            NodesDataTiersUsageTransportAction.LOCALLY_PRECALCULATED_STATS_FEATURE, // Added in 8.12
            License.INDEPENDENT_TRIAL_VERSION_FEATURE, // 8.14.0
            LOGSDB_TELEMETRY,
            LOGSDB_TELMETRY_STATS
        );
    }
}
