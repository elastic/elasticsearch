/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class ClusterStateMarvelDoc extends MarvelDoc<ClusterStateMarvelDoc.Payload> {

    private final Payload payload;

    public ClusterStateMarvelDoc(String clusterName, String type, long timestamp, Payload payload) {
        super(clusterName, type, timestamp);
        this.payload = payload;
    }

    @Override
    public ClusterStateMarvelDoc.Payload payload() {
        return payload;
    }

    public static ClusterStateMarvelDoc createMarvelDoc(String clusterName, String type, long timestamp, ClusterState clusterState, ClusterHealthStatus status) {
        return new ClusterStateMarvelDoc(clusterName, type, timestamp, new Payload(clusterState, status));
    }

    public static class Payload {

        private final ClusterState clusterState;
        private final ClusterHealthStatus status;

        Payload(ClusterState clusterState, ClusterHealthStatus status) {
            this.clusterState = clusterState;
            this.status = status;
        }

        public ClusterState getClusterState() {
            return clusterState;
        }

        public ClusterHealthStatus getStatus() {
            return status;
        }
    }
}
