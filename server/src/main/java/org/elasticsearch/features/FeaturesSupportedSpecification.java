/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import java.util.Set;

/**
 * This adds a feature {@code features_supported} indicating that a node supports node features.
 * Nodes that do not support features won't have this feature in its feature set,
 * so this can be checked without needing to look at the node version.
 */
public class FeaturesSupportedSpecification implements FeatureSpecification {

    /**
     * A feature indicating that node features are supported.
     */
    public static final NodeFeature FEATURES_SUPPORTED = new NodeFeature("features_supported");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(FEATURES_SUPPORTED);
    }
}
