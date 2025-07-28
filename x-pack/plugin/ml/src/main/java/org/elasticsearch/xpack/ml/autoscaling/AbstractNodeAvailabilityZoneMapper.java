/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

public abstract class AbstractNodeAvailabilityZoneMapper implements ClusterStateListener, NodeAvailabilityZoneMapper {
    private volatile DiscoveryNodes lastDiscoveryNodes;
    private volatile Map<List<String>, Collection<DiscoveryNode>> allNodesByAvailabilityZone;
    private volatile Map<List<String>, Collection<DiscoveryNode>> mlNodesByAvailabilityZone;

    public AbstractNodeAvailabilityZoneMapper(Settings settings, ClusterSettings clusterSettings) {
        this(settings, clusterSettings, null);
    }

    public AbstractNodeAvailabilityZoneMapper(Settings settings, ClusterSettings clusterSettings, DiscoveryNodes discoveryNodes) {
        lastDiscoveryNodes = discoveryNodes;
    }

    /**
     * @return A map whose keys are lists of attributes that together define an availability zone, and whose values are
     *         collections of nodes that have that combination of attributes. If availability zones
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
     * @return A map whose keys are lists of attributes that together define an availability zone, and whose values are
     *         collections of nodes that have that combination of attributes. If availability zones
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
            updateNodesByAvailabilityZone();
        }
    }

    synchronized void updateNodesByAvailabilityZone() {
        if (lastDiscoveryNodes == null) {
            allNodesByAvailabilityZone = Map.of();
            mlNodesByAvailabilityZone = allNodesByAvailabilityZone;
            return;
        }
        NodesByAvailabilityZone nodesByAvailabilityZone = buildNodesByAvailabilityZone(lastDiscoveryNodes);
        this.allNodesByAvailabilityZone = nodesByAvailabilityZone.allNodes();
        this.mlNodesByAvailabilityZone = nodesByAvailabilityZone.mlNodes();
    }
}
