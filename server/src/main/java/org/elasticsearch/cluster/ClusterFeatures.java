/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.features.NodeFeature;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Stores information on what features are present throughout the cluster
 */
public class ClusterFeatures {

    /**
     * The features on each individual node
     */
    private final Map<String, Set<String>> nodeFeatures;
    /**
     * The features present on all nodes
     */
    private final Set<String> allNodeFeatures;

    public ClusterFeatures(Map<String, Set<String>> nodeFeatures) {
        this.nodeFeatures = nodeFeatures.entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> Set.copyOf(e.getValue())));
        this.allNodeFeatures = calculateAllNodeFeatures(nodeFeatures.values());
    }

    private static Set<String> calculateAllNodeFeatures(Collection<Set<String>> nodeFeatures) {
        if (nodeFeatures.isEmpty()) {
            return Set.of();
        }

        Set<String> allNodeFeatures = null;
        for (Set<String> featureSet : nodeFeatures) {
            if (allNodeFeatures == null) {
                allNodeFeatures = new HashSet<>(featureSet);
            } else {
                allNodeFeatures.retainAll(featureSet);
            }
        }
        return Set.copyOf(allNodeFeatures);
    }

    Map<String, Set<String>> nodeFeatures() {
        return nodeFeatures;
    }

    /**
     * {@code true} if {@code feature} is present on all nodes in the cluster.
     * <p>
     * NOTE: This should not be used directly, as it does not read historical features.
     * Please use {@link org.elasticsearch.features.FeatureService#clusterHasFeature} instead.
     */
    public boolean clusterHasFeature(NodeFeature feature) {
        return allNodeFeatures.contains(feature.id());
    }
}
