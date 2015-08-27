/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class IndexRecoveryMarvelDoc extends MarvelDoc {

    private final RecoveryResponse recoveryResponse;

    public IndexRecoveryMarvelDoc(String clusterUUID, String type, long timestamp, RecoveryResponse recoveryResponse) {
        super(clusterUUID, type, timestamp);
        this.recoveryResponse = recoveryResponse;
    }

    public RecoveryResponse getRecoveryResponse() {
        return recoveryResponse;
    }
}
