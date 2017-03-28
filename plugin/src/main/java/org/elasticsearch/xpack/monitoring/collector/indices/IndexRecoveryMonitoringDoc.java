/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

/**
 * Monitoring document collected by {@link IndexRecoveryCollector}
 */
public class IndexRecoveryMonitoringDoc extends MonitoringDoc {

    public static final String TYPE = "index_recovery";

    private final RecoveryResponse recoveryResponse;

    public IndexRecoveryMonitoringDoc(String monitoringId, String monitoringVersion,
                                      String clusterUUID, long timestamp, DiscoveryNode node,
                                      RecoveryResponse recoveryResponse) {
        super(monitoringId, monitoringVersion, TYPE, null, clusterUUID, timestamp, node);
        this.recoveryResponse = recoveryResponse;
    }

    public RecoveryResponse getRecoveryResponse() {
        return recoveryResponse;
    }
}
