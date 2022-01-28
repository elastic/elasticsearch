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

    public static final String NAME = "instance_has_master";
    public static final String GREEN_EXPLAIN = "Health coordinating instance has a master node.";
    public static final String RED_EXPLAIN = "Health coordinating instance does not have a master node.";

    private final DiscoveryNode coordinatingNode;
    private final DiscoveryNode masterNode;

    public NodeDoesNotHaveMaster(DiscoveryNode coordinatingNode, DiscoveryNode masterNode) {
        super(
            NAME,
            masterNode == null ? ClusterHealthStatus.RED : ClusterHealthStatus.GREEN,
            masterNode == null ? RED_EXPLAIN : GREEN_EXPLAIN
        );
        this.coordinatingNode = coordinatingNode;
        this.masterNode = masterNode;
    }

    @Override
    public void writeMeta(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("node_id", coordinatingNode.getId());
        builder.field("name_name", coordinatingNode.getName());
        if (masterNode != null) {
            builder.field("master_node_id", masterNode.getId());
            builder.field("master_node_name", masterNode.getName());
        } else {
            builder.nullField("master_node_id");
            builder.nullField("master_node_name");
        }
    }
}
