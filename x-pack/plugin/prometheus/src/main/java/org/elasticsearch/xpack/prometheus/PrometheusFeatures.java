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
 * This is so that we can use {@code cluster_features: prometheus_plugin} in the yaml tests depending on whether the
 * feature flag is enabled (in snapshot builds and regular PR tests) or disabled (in release builds and release tests).
 */
public class PrometheusFeatures implements FeatureSpecification {

    public static final NodeFeature PROMETHEUS_PLUGIN = new NodeFeature("prometheus_plugin");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        if (PrometheusPlugin.PROMETHEUS_FEATURE_FLAG.isEnabled()) {
            return Set.of(PROMETHEUS_PLUGIN);
        } else {
            return Set.of();
        }
    }
}
