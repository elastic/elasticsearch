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
 * This class specifies features for the features functionality itself.
 * <p>
 * This adds a feature {@code features_supported} indicating that a node supports node features.
 * Nodes that do not support features won't have this feature in its feature set,
 * so this can be checked without needing to look at the node version.
 */
public class FeatureInfrastructureFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(FeatureService.FEATURES_SUPPORTED);
    }
}
