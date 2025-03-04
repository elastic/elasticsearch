/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.features;

import org.elasticsearch.cluster.ClusterFeatures;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.List;
import java.util.Map;

/**
 * Manages information on the features supported by nodes in the cluster.
 * For more information, see {@link FeatureSpecification}.
 */
public class FeatureService {

    public static final NodeFeature TEST_FEATURES_ENABLED = new NodeFeature("test_features_enabled");

    private static final Logger logger = LogManager.getLogger(FeatureService.class);

    private final Map<String, NodeFeature> nodeFeatures;

    /**
     * Creates a new {@code FeatureService}, reporting all the features declared in {@code specs}
     * as the local node's supported feature set
     */
    public FeatureService(List<? extends FeatureSpecification> specs) {
        this.nodeFeatures = FeatureData.createFromSpecifications(specs).getNodeFeatures();

        logger.info("Registered local node features {}", nodeFeatures.keySet().stream().sorted().toList());
    }

    /**
     * The features supported by this node.
     * @return Map of {@code feature-id} to its declaring {@code NodeFeature} object.
     */
    public Map<String, NodeFeature> getNodeFeatures() {
        return nodeFeatures;
    }

    /**
     * Returns {@code true} if {@code node} can have assumed features.
     */
    public static boolean featuresCanBeAssumedForNode(DiscoveryNode node) {
        return ClusterFeatures.featuresCanBeAssumedForNode(node);
    }

    /**
    * Returns {@code true} if one or more nodes in {@code nodes} can have assumed features.
    */
    public static boolean featuresCanBeAssumedForNodes(DiscoveryNodes nodes) {
        return ClusterFeatures.featuresCanBeAssumedForNodes(nodes);
    }

    /**
     * Returns {@code true} if all nodes in {@code state} support feature {@code feature}.
     */
    @SuppressForbidden(reason = "We need basic feature information from cluster state")
    public boolean clusterHasFeature(ClusterState state, NodeFeature feature) {
        return state.clusterFeatures().clusterHasFeature(state.nodes(), feature);
    }
}
