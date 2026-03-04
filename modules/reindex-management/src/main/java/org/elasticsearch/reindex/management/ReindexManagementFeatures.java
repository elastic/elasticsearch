/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.reindex.ReindexPlugin;

import java.util.HashSet;
import java.util.Set;

public class ReindexManagementFeatures implements FeatureSpecification {

    public static final NodeFeature NEW_ENDPOINTS = new NodeFeature("reindex_management_endpoints");

    @Override
    public Set<NodeFeature> getFeatures() {
        final Set<NodeFeature> features = new HashSet<>();
        // TODO: Before we release any functionality behind FeatureFlags, we should see whether we can
        // consolidate the node features. These are a constrained resource, so we should combine the features for anything that is being
        // released at the same time while we still can.
        if (ReindexPlugin.REINDEX_RESILIENCE_ENABLED) {
            features.add(NEW_ENDPOINTS);
            features.add(ReindexPlugin.RELOCATE_ON_SHUTDOWN_NODE_FEATURE);
        }
        if (ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED) {
            features.add(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE);
        }
        return Set.copyOf(features);
    }
}
