/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.components.controller;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class InstanceHasMaster extends GetHealthAction.Indicator {

    public static final String NAME = "instance_has_master";
    public static final String GREEN_SUMMARY = "Health coordinating instance has a master node.";
    public static final String RED_SUMMARY = "Health coordinating instance does not have a master node.";

    private final DiscoveryNode coordinatingNode;
    private final DiscoveryNode masterNode;

    public InstanceHasMaster(DiscoveryNode coordinatingNode, DiscoveryNode masterNode) {
        super(NAME, masterNode == null ? HealthStatus.RED : HealthStatus.GREEN, masterNode == null ? RED_SUMMARY : GREEN_SUMMARY);
        this.coordinatingNode = coordinatingNode;
        this.masterNode = masterNode;
    }

    @Override
    public void writeDetails(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.object("coordinating_node", xContentBuilder -> {
            builder.field("node_id", coordinatingNode.getId());
            builder.field("name", coordinatingNode.getName());
        });
        builder.object("master_node", xContentBuilder -> {
            if (masterNode != null) {
                builder.field("node_id", masterNode.getId());
                builder.field("name", masterNode.getName());
            } else {
                builder.nullField("node_id");
                builder.nullField("name");
            }
        });
    }
}
