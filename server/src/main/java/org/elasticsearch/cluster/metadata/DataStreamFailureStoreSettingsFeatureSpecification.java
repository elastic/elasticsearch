/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/**
 * Registers the ability to specify data stream failure store cluster settings as a node feature.
 */
public class DataStreamFailureStoreSettingsFeatureSpecification implements FeatureSpecification {

    public Set<NodeFeature> getFeatures() {
        return DataStream.isFailureStoreFeatureFlagEnabled() ? Set.of(DataStreamFailureStoreSettings.NODE_FEATURE) : Set.of();
    }
}
