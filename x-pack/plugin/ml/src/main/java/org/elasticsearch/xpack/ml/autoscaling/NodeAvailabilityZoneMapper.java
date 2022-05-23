/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
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
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Maintains a list of the nodes in each availability zone, where an availability zone
 * means a unique combination of values of the attributes listed in the cluster setting
 * cluster.routing.allocation.awareness.attributes.
 */
public class NodeAvailabilityZoneMapper implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(NodeAvailabilityZoneMapper.class);

    private volatile List<String> awarenessAttributes;
    private volatile DiscoveryNodes lastDiscoveryNodes;
    private volatile Map<List<String>, Collection<DiscoveryNode>> allNodesByAvailabilityZone;
    private volatile Map<List<String>, Collection<DiscoveryNode>> mlNodesByAvailabilityZone;

    public NodeAvailabilityZoneMapper(Settings settings, ClusterSettings clusterSettings) {
        this(settings, clusterSettings, null);
    }

    public NodeAvailabilityZoneMapper(Settings settings, ClusterSettings clusterSettings, DiscoveryNodes discoveryNodes) {
        awarenessAttributes = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        lastDiscoveryNodes = discoveryNodes;
        buildNodesByAvailabilityZone();
        clusterSettings.addSettingsUpdateConsumer(
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
            this::setAwarenessAttributes
        );
    }

    private synchronized void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = List.copyOf(awarenessAttributes);
        buildNodesByAvailabilityZone();
    }

    public List<String> getAwarenessAttributes() {
        return awarenessAttributes;
    }

    /**
     * @return A map whose keys are lists of awareness attribute values in the same order as the configured awareness attribute
     *         names, and whose values are collections of nodes that have that combination of attributes. If availability zones
     *         are not configured then the map will contain one entry mapping an empty list to a collection of all nodes. If
     *         it is too early in the lifecycle of the node to know the answer then an empty map will be returned.
     */
    public Map<List<String>, Collection<DiscoveryNode>> getAllNodesByAvailabilityZone() {
        return allNodesByAvailabilityZone;
    }

    /**
     * @return The number of availability zones in the cluster. If availability zones are not configured this will be 1.
     *         If it is too early in the lifecycle of the node to know the answer then {@link OptionalInt#empty()} will be returned.
     */
    public OptionalInt getNumAvailabilityZones() {
        return (lastDiscoveryNodes == null) ? OptionalInt.empty() : OptionalInt.of(allNodesByAvailabilityZone.size());
    }

    /**
     * @return A map whose keys are lists of awareness attribute values in the same order as the configured awareness attribute
     *         names, and whose values are collections of nodes that have that combination of attributes. If availability zones
     *         are not configured then the map will contain one entry mapping an empty list to a collection of all nodes. If
     *         it is too early in the lifecycle of the node to know the answer then an empty map will be returned. An empty
     *         map will also be returned if there are no ML nodes in the cluster. (These two empty map return scenarios can be
     *         distinguished by calling one of the other methods.)
     */
    public Map<List<String>, Collection<DiscoveryNode>> getMlNodesByAvailabilityZone() {
        return mlNodesByAvailabilityZone;
    }

    /**
     * @return The number of availability zones in the cluster. If availability zones are not configured this will be 1.
     *         If it is too early in the lifecycle of the node to know the answer then {@link OptionalInt#empty()} will be returned.
     *         Unlike {@link #getNumAvailabilityZones()}, it is possible this method will return 0, as it is possible
     *         for a cluster to have no ML nodes.
     */
    public OptionalInt getNumMlAvailabilityZones() {
        return (lastDiscoveryNodes == null) ? OptionalInt.empty() : OptionalInt.of(mlNodesByAvailabilityZone.size());
    }

    @Override
    public synchronized void clusterChanged(ClusterChangedEvent event) {
        if (lastDiscoveryNodes == null || event.nodesChanged()) {
            lastDiscoveryNodes = event.state().nodes();
            buildNodesByAvailabilityZone();
        }
    }

    private synchronized void buildNodesByAvailabilityZone() {
        if (lastDiscoveryNodes == null) {
            allNodesByAvailabilityZone = Map.of();
            mlNodesByAvailabilityZone = allNodesByAvailabilityZone;
            return;
        }

        Collection<DiscoveryNode> nodes = lastDiscoveryNodes.getNodes().values();

        if (awarenessAttributes.isEmpty()) {
            allNodesByAvailabilityZone = Map.of(List.of(), nodes);
            mlNodesByAvailabilityZone = Map.of(
                List.of(),
                nodes.stream().filter(n -> n.getRoles().contains(DiscoveryNodeRole.ML_ROLE)).toList()
            );
            return;
        }

        Map<List<String>, Collection<DiscoveryNode>> updatedNodesByAvailabilityZone = new HashMap<>();
        Map<List<String>, Collection<DiscoveryNode>> updatedMlNodesByAvailabilityZone = new HashMap<>();
        for (DiscoveryNode node : nodes) {
            List<String> orderedNodeAttributeValues = awarenessAttributes.stream()
                .map(a -> node.getAttributes().get(a))
                .filter(Objects::nonNull)
                .toList();
            if (orderedNodeAttributeValues.size() != awarenessAttributes.size()) {
                // This indicates bad configuration - there shouldn't be nodes that don't have all the awareness attributes
                logger.debug(
                    "Node [{}] does not have all configured awareness attributes {} - will be ignored in availability zone mapper",
                    node,
                    awarenessAttributes
                );
                continue;
            }
            updatedNodesByAvailabilityZone.computeIfAbsent(orderedNodeAttributeValues, k -> new ArrayList<>()).add(node);
            if (node.getRoles().contains(DiscoveryNodeRole.ML_ROLE)) {
                updatedMlNodesByAvailabilityZone.compute(orderedNodeAttributeValues, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(node);
                    return v;
                });
            }
        }
        allNodesByAvailabilityZone = Map.copyOf(updatedNodesByAvailabilityZone);
        mlNodesByAvailabilityZone = Map.copyOf(updatedMlNodesByAvailabilityZone);
    }
}
