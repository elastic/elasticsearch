/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

/**
 * Monitoring document collected by {@link ClusterStateCollector} that contains information
 * about every node of the cluster.
 */
public class DiscoveryNodeMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "node";

    private final DiscoveryNode node;

    public DiscoveryNodeMonitoringDoc(String monitoringId, String monitoringVersion,
                                      String clusterUUID, long timestamp, DiscoveryNode node) {
        super(monitoringId, monitoringVersion, TYPE, node.getId(), clusterUUID, timestamp, node);
        this.node = node;
    }

    public DiscoveryNode getNode() {
        return node;
    }
}