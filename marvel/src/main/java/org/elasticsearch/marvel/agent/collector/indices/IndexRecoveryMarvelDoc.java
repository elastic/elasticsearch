/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.indices;

import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class IndexRecoveryMarvelDoc extends MarvelDoc<IndexRecoveryMarvelDoc.Payload> {

    private final Payload payload;

    public IndexRecoveryMarvelDoc(String clusterName, String type, long timestamp, Payload payload) {
        super(clusterName, type, timestamp);
        this.payload = payload;
    }

    @Override
    public IndexRecoveryMarvelDoc.Payload payload() {
        return payload;
    }

    public static IndexRecoveryMarvelDoc createMarvelDoc(String clusterName, String type, long timestamp,
                                                         RecoveryResponse recoveryResponse) {
        return new IndexRecoveryMarvelDoc(clusterName, type, timestamp, new Payload(recoveryResponse));
    }

    public static class Payload {

        RecoveryResponse recoveryResponse;

        public Payload(RecoveryResponse recoveryResponse) {
            this.recoveryResponse = recoveryResponse;
        }

        public RecoveryResponse getRecoveryResponse() {
            return recoveryResponse;
        }

    }
}
