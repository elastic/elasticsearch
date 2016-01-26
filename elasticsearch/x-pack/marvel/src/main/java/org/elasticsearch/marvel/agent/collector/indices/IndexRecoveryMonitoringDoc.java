/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;

public class IndexRecoveryMonitoringDoc extends MonitoringDoc {

    private RecoveryResponse recoveryResponse;

    public IndexRecoveryMonitoringDoc(String monitoringId, String monitoringVersion) {
        super(monitoringId, monitoringVersion);
    }

    public RecoveryResponse getRecoveryResponse() {
        return recoveryResponse;
    }

    public void setRecoveryResponse(RecoveryResponse recoveryResponse) {
        this.recoveryResponse = recoveryResponse;
    }
}
