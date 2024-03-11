/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maintains a list of the nodes in each fake availability zone, where each availability zone
 * corresponds to a single node.
 */
public class NodeFakeAvailabilityZoneMapper extends AbstractNodeAvailabilityZoneMapper {

    public NodeFakeAvailabilityZoneMapper(Settings settings, ClusterSettings clusterSettings) {
        this(settings, clusterSettings, null);
    }

    @SuppressWarnings("this-escape")
    public NodeFakeAvailabilityZoneMapper(Settings settings, ClusterSettings clusterSettings, DiscoveryNodes discoveryNodes) {
        super(settings, clusterSettings, discoveryNodes);
        updateNodesByAvailabilityZone();
    }

    /**
     * @param discoveryNodes The current set of all nodes in the cluster.
     * @return A map whose keys are single item lists of node id values, and whose values are single item collections
     *         of nodes corresponding to the node ids. An empty map will be returned if there are no ML nodes in the
     *         cluster.
     */
    public NodesByAvailabilityZone buildNodesByAvailabilityZone(DiscoveryNodes discoveryNodes) {
        Collection<DiscoveryNode> nodes = discoveryNodes.getNodes().values();

        Map<List<String>, Collection<DiscoveryNode>> allNodesByAvailabilityZone = new HashMap<>();
        Map<List<String>, Collection<DiscoveryNode>> mlNodesByAvailabilityZone = new HashMap<>();
        for (DiscoveryNode node : nodes) {
            List<String> nodeIdValues = List.of(node.getId());
            List<DiscoveryNode> nodeList = List.of(node);
            allNodesByAvailabilityZone.put(nodeIdValues, nodeList);
            if (node.getRoles().contains(DiscoveryNodeRole.ML_ROLE)) {
                mlNodesByAvailabilityZone.put(nodeIdValues, nodeList);
            }
        }
        return new NodesByAvailabilityZone(Map.copyOf(allNodesByAvailabilityZone), Map.copyOf(mlNodesByAvailabilityZone));
    }

    /**
     * This is different to {@link #getMlNodesByAvailabilityZone()} in that the latter returns the ML nodes by (fake) availability zone
     * of the latest cluster state, while this method does the same for a specific cluster state.
     *
     * @param clusterState The cluster state whose nodes will be used to detect ML nodes by fake availability zone.
     * @return A map whose keys are single item lists of node id values, and whose values are single item collections
     *         of nodes corresponding to the node ids. An empty map will be returned if there are no ML nodes in the
     *         cluster.
     */
    @Override
    public Map<List<String>, Collection<DiscoveryNode>> buildMlNodesByAvailabilityZone(ClusterState clusterState) {
        return buildNodesByAvailabilityZone(clusterState.nodes()).mlNodes();
    }
}
