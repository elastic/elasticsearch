/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

public interface NodeAvailabilityZoneMapper {
    /**
     * @param clusterState The specific cluster state whose nodes will be used to detect ML nodes by availability zone.
     * @return A map whose keys are conceptually lists of availability zone attributes, and whose values are collections
     *         of nodes corresponding to the availability zone attributes.
     *         An empty map will be returned if there are no ML nodes in the cluster.
     */
    Map<List<String>, Collection<DiscoveryNode>> buildMlNodesByAvailabilityZone(ClusterState clusterState);

    OptionalInt getNumMlAvailabilityZones();

    NodesByAvailabilityZone buildNodesByAvailabilityZone(DiscoveryNodes discoveryNodes);

    record NodesByAvailabilityZone(
        Map<List<String>, Collection<DiscoveryNode>> allNodes,
        Map<List<String>, Collection<DiscoveryNode>> mlNodes
    ) {}
}
