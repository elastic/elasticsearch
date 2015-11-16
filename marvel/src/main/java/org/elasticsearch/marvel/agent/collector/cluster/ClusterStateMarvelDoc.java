/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class ClusterStateMarvelDoc extends MarvelDoc {

    private final ClusterState clusterState;
    private final ClusterHealthStatus status;

    public ClusterStateMarvelDoc(String clusterUUID, String type, long timestamp, ClusterState clusterState, ClusterHealthStatus status) {
        super(clusterUUID, type, timestamp);
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
