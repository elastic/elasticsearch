/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

class ReindexFeatures implements FeatureSpecification {

    static final NodeFeature RELOCATE_TASK_ON_SHUTDOWN = new NodeFeature("reindex_relocate_task_on_shutdown");

    @Override
    public Set<NodeFeature> getFeatures() {
        // TODO: Consider adding a cluster feature to control this when we remove the feature flag
        return ReindexPlugin.REINDEX_RESILIENCE_ENABLED ? Set.of(RELOCATE_TASK_ON_SHUTDOWN) : Set.of();
    }
}
