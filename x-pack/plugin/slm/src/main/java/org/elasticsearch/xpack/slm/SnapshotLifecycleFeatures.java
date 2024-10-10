/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xpack.slm.history.SnapshotLifecycleTemplateRegistry;

import java.util.Map;
import java.util.Set;

public class SnapshotLifecycleFeatures implements FeatureSpecification {
    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(SnapshotLifecycleService.INTERVAL_SCHEDULE);
    }

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.of(SnapshotLifecycleTemplateRegistry.MANAGED_BY_DATA_STREAM_LIFECYCLE, Version.V_8_12_0);
    }
}
