/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class MachineLearningFeatures implements FeatureSpecification {

    public static final NodeFeature COMPONENTS_RESET_ACTION = new NodeFeature("ml.components.reset");

    public Set<NodeFeature> getFeatures() {
        return Set.of(COMPONENTS_RESET_ACTION);
    }
}
