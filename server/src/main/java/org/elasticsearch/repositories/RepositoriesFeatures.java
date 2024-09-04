/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class RepositoriesFeatures implements FeatureSpecification {
    public static final NodeFeature SUPPORTS_REPOSITORIES_USAGE_STATS = new NodeFeature("repositories.supports_usage_stats");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(SUPPORTS_REPOSITORIES_USAGE_STATS);
    }
}
