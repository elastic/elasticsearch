/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.components.controller;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class NodeDoesNotHaveMaster extends GetHealthAction.Indicator {

    public static final String GREEN_EXPLAIN = "health coordinating instance has a master node";
    public static final String RED_EXPLAIN = "health coordinating instance does not have a master node";
    private final DiscoveryNode coordinatingNode;
    private final DiscoveryNode masterNode;

    public NodeDoesNotHaveMaster(DiscoveryNode coordinatingNode, DiscoveryNode masterNode) {
        this.coordinatingNode = coordinatingNode;
        this.masterNode = masterNode;
    }

    @Override
    public void writeMeta(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("node-id", coordinatingNode.getId());
        builder.field("name-name", coordinatingNode.getName());
        if (masterNode != null) {
            builder.field("master-node-id", masterNode.getId());
            builder.field("master-node-name", masterNode.getName());
        } else {
            builder.nullField("master-node-id");
            builder.nullField("master-node-name");
        }
    }

    @Override
    public String getExplain() {
        if (masterNode == null) {
            return RED_EXPLAIN;
        } else {
            return GREEN_EXPLAIN;
        }
    }

    @Override
    public String getName() {
        return "instance has master";
    }

    @Override
    public ClusterHealthStatus getStatus() {
        if (masterNode == null) {
            return ClusterHealthStatus.RED;
        } else {
            return ClusterHealthStatus.GREEN;
        }
    }
}
