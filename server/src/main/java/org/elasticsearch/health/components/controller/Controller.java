/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.components.controller;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.health.GetHealthAction;

import java.util.ArrayList;
import java.util.List;

public class Controller extends GetHealthAction.Component {

    private final ClusterHealthStatus status;
    private final List<GetHealthAction.Indicator> indicators = new ArrayList<>(2);

    public Controller(final DiscoveryNode node, final ClusterState clusterState) {
        final DiscoveryNodes nodes = clusterState.nodes();
        final DiscoveryNode masterNode = nodes.getMasterNode();
        NodeDoesNotHaveMaster nodeDoesNotHaveMaster = new NodeDoesNotHaveMaster(node, masterNode);
        indicators.add(nodeDoesNotHaveMaster);
        // Only a single indicator currently so it determines the status
        status = nodeDoesNotHaveMaster.getStatus();

    }

    @Override
    public String getName() {
        return "controller";
    }

    @Override
    public ClusterHealthStatus getStatus() {
        return status;
    }

    @Override
    public List<GetHealthAction.Indicator> getIndicators() {
        return indicators;
    }
}
