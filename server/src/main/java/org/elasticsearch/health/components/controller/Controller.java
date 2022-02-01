/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.components.controller;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthStatus;

import java.util.Collections;

public final class Controller {

    private Controller() {}

    public static GetHealthAction.Component createControllerComponent(final DiscoveryNode node, final ClusterState clusterState) {
        final DiscoveryNodes nodes = clusterState.nodes();
        final DiscoveryNode masterNode = nodes.getMasterNode();
        InstanceHasMaster instanceHasMaster = new InstanceHasMaster(node, masterNode);
        // Only a single indicator currently so it determines the status
        final HealthStatus status = instanceHasMaster.getStatus();
        return new GetHealthAction.Component("controller", status, Collections.singletonList(instanceHasMaster));
    }
}
