/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

public interface TestFeatureService {
    TestFeatureService ALL_FEATURES = (feature, any) -> true;

    /**
     * Returns {@code true} if all nodes in the cluster have feature {@code featureId}
     */
    default boolean clusterHasFeature(String featureId) {
        return clusterHasFeature(featureId, false);
    }

    /**
     * @param featureId The feature to check
     * @param any
     *         {@code true} if it should check if any node has the feature,
     *         {@code false} if it should check if all nodes have the feature
     */
    boolean clusterHasFeature(String featureId, boolean any);
}
