/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maintains a list of the nodes in each availability zone, where an availability zone
 * means a unique combination of values of the attributes listed in the cluster setting
 * cluster.routing.allocation.awareness.attributes.
 */
public class NodeRealAvailabilityZoneMapper extends AbstractNodeAvailabilityZoneMapper {

    private static final Logger logger = LogManager.getLogger(NodeRealAvailabilityZoneMapper.class);

    private volatile List<String> awarenessAttributes;

    public NodeRealAvailabilityZoneMapper(Settings settings, ClusterSettings clusterSettings) {
        this(settings, clusterSettings, null);
    }

    @SuppressWarnings("this-escape")
    public NodeRealAvailabilityZoneMapper(Settings settings, ClusterSettings clusterSettings, DiscoveryNodes discoveryNodes) {
        super(settings, clusterSettings, discoveryNodes);
        awarenessAttributes = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        updateNodesByAvailabilityZone();
        clusterSettings.addSettingsUpdateConsumer(
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
            this::setAwarenessAttributes
        );
    }

    private synchronized void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = List.copyOf(awarenessAttributes);
        updateNodesByAvailabilityZone();
    }

    public List<String> getAwarenessAttributes() {
        return awarenessAttributes;
    }

    public NodesByAvailabilityZone buildNodesByAvailabilityZone(DiscoveryNodes discoveryNodes) {
        return buildNodesByAvailabilityZone(discoveryNodes, awarenessAttributes);
    }

    private static NodesByAvailabilityZone buildNodesByAvailabilityZone(DiscoveryNodes discoveryNodes, List<String> awarenessAttributes) {
        Collection<DiscoveryNode> nodes = discoveryNodes.getNodes().values();

        if (awarenessAttributes.isEmpty()) {
            return new NodesByAvailabilityZone(
                Map.of(List.of(), nodes),
                Map.of(List.of(), nodes.stream().filter(n -> n.getRoles().contains(DiscoveryNodeRole.ML_ROLE)).toList())
            );
        }

        Map<List<String>, Collection<DiscoveryNode>> allNodesByAvailabilityZone = new HashMap<>();
        Map<List<String>, Collection<DiscoveryNode>> mlNodesByAvailabilityZone = new HashMap<>();
        for (DiscoveryNode node : nodes) {
            List<String> orderedNodeAttributeValues = awarenessAttributes.stream().map(a -> {
                String v = node.getAttributes().get(a);
                if (v == null) {
                    // This will never happen for a Cloud cluster, but for self-managed it's possible.
                    // We reuse the same allocation attributes that are used for shards, but self-managed
                    // users might not have bothered to set them on their ML nodes if they are not spread
                    // across availability zones.
                    logger.debug(
                        "Node [{}] does not have all configured awareness attributes {} - missing [{}]",
                        node,
                        awarenessAttributes,
                        a
                    );
                    return "";
                }
                return v;
            }).toList();
            allNodesByAvailabilityZone.computeIfAbsent(orderedNodeAttributeValues, k -> new ArrayList<>()).add(node);
            if (node.getRoles().contains(DiscoveryNodeRole.ML_ROLE)) {
                mlNodesByAvailabilityZone.compute(orderedNodeAttributeValues, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(node);
                    return v;
                });
            }
        }
        return new NodesByAvailabilityZone(Map.copyOf(allNodesByAvailabilityZone), Map.copyOf(mlNodesByAvailabilityZone));
    }

    /**
     * This is different to {@link #getMlNodesByAvailabilityZone()} in that the latter returns the ML nodes by availability zone
     * of the latest cluster state, while this method does the same for a specific cluster state.
     *
     * @param clusterState The cluster state whose nodes will be used to detect ML nodes by availability zone.
     * @return A map whose keys are lists of awareness attribute values in the same order as the configured awareness attribute
     *         names, and whose values are collections of nodes that have that combination of attributes. If availability zones
     *         are not configured then the map will contain one entry mapping an empty list to a collection of all nodes. If
     *         it is too early in the lifecycle of the node to know the answer then an empty map will be returned. An empty
     *         map will also be returned if there are no ML nodes in the cluster. (These two empty map return scenarios can be
     *         distinguished by calling one of the other methods.)
     */
    @Override
    public Map<List<String>, Collection<DiscoveryNode>> buildMlNodesByAvailabilityZone(ClusterState clusterState) {
        return buildNodesByAvailabilityZone(clusterState.nodes(), awarenessAttributes).mlNodes();
    }
}
