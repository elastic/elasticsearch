/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

public interface NodeAvailabilityZoneMapper {
    /**
     * @param mlNodes The nodes which will be used to detect ML nodes by availability zone.
     * @return A map whose keys are conceptually lists of availability zone attributes, and whose values are collections
     *         of nodes corresponding to the availability zone attributes.
     *         An empty map will be returned if there are no ML nodes in the cluster.
     */
    Map<List<String>, Collection<DiscoveryNode>> buildMlNodesByAvailabilityZone(List<DiscoveryNode> mlNodes);

    OptionalInt getNumMlAvailabilityZones();

    record NodesByAvailabilityZone(
        Map<List<String>, Collection<DiscoveryNode>> allNodes,
        Map<List<String>, Collection<DiscoveryNode>> mlNodes
    ) {}
}
