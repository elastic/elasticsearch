/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/**
 * Manages information on the features supported by nodes in the cluster.
 * For more information, see {@link FeatureSpecification}.
 */
public class FeatureService {

    /**
     * A feature indicating that node features are supported.
     */
    public static final NodeFeature FEATURES_SUPPORTED = new NodeFeature("features_supported");

    private static final Logger logger = LogManager.getLogger(FeatureService.class);

    public static final Version CLUSTER_FEATURES_ADDED_VERSION = Version.V_8_12_0;

    private final NavigableMap<Version, Set<String>> historicalFeatures;
    private final Map<String, NodeFeature> nodeFeatures;

    /**
     * Creates a new {@code FeatureService}, reporting all the features declared in {@code specs}
     * as the local node's supported feature set
     */
    public FeatureService(List<? extends FeatureSpecification> specs) {

        var featureData = FeatureData.createFromSpecifications(specs);
        nodeFeatures = featureData.getNodeFeatures();
        historicalFeatures = featureData.getHistoricalFeatures();

        logger.info("Registered local node features {}", nodeFeatures.keySet().stream().sorted().toList());
    }

    /**
     * The non-historical features supported by this node.
     * @return Map of {@code feature-id} to its declaring {@code NodeFeature} object.
     */
    public Map<String, NodeFeature> getNodeFeatures() {
        return nodeFeatures;
    }

    /**
     * Returns {@code true} if all nodes in {@code state} support feature {@code feature}.
     */
    @SuppressForbidden(reason = "We need basic feature information from cluster state")
    public boolean clusterHasFeature(ClusterState state, NodeFeature feature) {
        if (state.clusterFeatures().clusterHasFeature(feature)) {
            return true;
        }

        var features = historicalFeatures.floorEntry(state.getNodes().getMinNodeVersion());
        return features != null && features.getValue().contains(feature.id());
    }
}
