/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/**
 * Exposes the {@code prometheus_plugin} cluster feature so that yaml tests can depend on it.
 * Required for BWC: older nodes in a mixed-version cluster advertise this feature, so new nodes
 * must also advertise it or they will be rejected by the feature join barrier.
 */
public class PrometheusFeatures implements FeatureSpecification {

    public static final NodeFeature PROMETHEUS_PLUGIN = new NodeFeature("prometheus_plugin");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(PROMETHEUS_PLUGIN);
    }
}
