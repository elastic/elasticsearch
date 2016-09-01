/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

public class DiscoveryNodeMonitoringDoc extends MonitoringDoc {

    private DiscoveryNode node;

    public DiscoveryNodeMonitoringDoc(String monitoringId, String monitoringVersion) {
        super(monitoringId, monitoringVersion);
    }

    public DiscoveryNode getNode() {
        return node;
    }

    public void setNode(DiscoveryNode node) {
        this.node = node;
    }
}
