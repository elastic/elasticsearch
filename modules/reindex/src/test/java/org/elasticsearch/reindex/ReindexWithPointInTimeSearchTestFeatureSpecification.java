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

/**
 * Test-only {@link FeatureSpecification} that conditionally registers {@link ReindexPlugin#REINDEX_PIT_SEARCH_FEATURE}
 * based on whether the test is running with scroll or point-in-time searching.
 */
public class ReindexWithPointInTimeSearchTestFeatureSpecification implements FeatureSpecification {

    /**
     * Set by the running test before node creation. When {@code true}, this spec
     * registers {@link ReindexPlugin#REINDEX_PIT_SEARCH_FEATURE} so the cluster reports the feature.
     * Volatile, so it is visible across threads (test thread sets it, node thread reads it).
     */
    public static volatile boolean REINDEX_PIT_SEARCH_FOR_TEST = false;

    @Override
    public Set<NodeFeature> getFeatures() {
        if (REINDEX_PIT_SEARCH_FOR_TEST) {
            return Set.of(ReindexPlugin.REINDEX_PIT_SEARCH_FEATURE);
        }
        return Set.of();
    }
}
