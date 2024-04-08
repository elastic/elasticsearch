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
 */
public interface FeatureSpecification {
    /**
     * Returns a set of regular features that this node supports.
     */
    default Set<NodeFeature> getFeatures() {
        return Set.of();
    }

    /**
     * Returns information on historical features that should be deemed to be present on all nodes
     * on or above the {@link Version} specified.
     */
    default Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.of();
    }
}
