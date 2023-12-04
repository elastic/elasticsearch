/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthInfoPublisher;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/**
 * Provides the features for data streams that this version of the code supports
 */
public class DataStreamFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(
            DataStreamLifecycleHealthInfoPublisher.DSL_HEALTH_INFO_FEATURE // Added in 8.12
        );
    }
}
