/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import org.elasticsearch.Version;

import java.util.Map;
import java.util.Set;

/**
 * Specifies one or more features that should be published by this node
 */
public interface FeatureSpecification {
    default Set<NodeFeature> getFeatures() {
        return Set.of();
    }

    default Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.of();
    }
}
