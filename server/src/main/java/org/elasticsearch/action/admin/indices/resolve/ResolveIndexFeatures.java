/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class ResolveIndexFeatures implements FeatureSpecification {

    // Feature published by nodes that return "mode" in indices.resolve_index responses.
    public static final NodeFeature RESOLVE_INDEX_RETURNS_MODE = new NodeFeature("resolve_index_returns_mode");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(RESOLVE_INDEX_RETURNS_MODE);
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(RESOLVE_INDEX_RETURNS_MODE);
    }

}
