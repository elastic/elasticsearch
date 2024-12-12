/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.features;

import java.util.Set;

/**
 * Specifies one or more features that are supported by this node.
 * <p>
 * Features are published as part of node information in cluster state.
 * Code can check if all nodes in a cluster support a feature using {@link FeatureService#clusterHasFeature}.
 * Once all nodes in a cluster support a feature, other nodes are blocked from joining that cluster
 * unless they also support that feature (this is known as the 'feature ratchet').
 * So once a feature is supported by a cluster, it will always be supported by that cluster in the future.
 * <p>
 * The feature information in cluster state should not normally be directly accessed.
 * All feature checks should be done through {@code FeatureService} to ensure that Elasticsearch's
 * guarantees on the introduction of new functionality are followed;
 * that is, new functionality is not enabled until all nodes in the cluster support it.
 * <p>
 * <b>Note:</b> {@link FeatureSpecification}s are loaded as service providers, however tests are not fully modularized yet.
 * Make sure to also register new specifications in {@code META-INF/services/org.elasticsearch.features.FeatureSpecification},
 * so they are available in tests as well.
 */
public interface FeatureSpecification {
    /**
     * Returns a set of regular features that this node supports.
     */
    default Set<NodeFeature> getFeatures() {
        return Set.of();
    }

    /**
     * Returns a set of test features that this node supports.
     * <p>
     * These features will only be exposed if the {@code tests.testfeatures.enabled} system property is set.
     * This should only be used when deploying test clusters.
     */
    default Set<NodeFeature> getTestFeatures() {
        return Set.of();
    }
}
