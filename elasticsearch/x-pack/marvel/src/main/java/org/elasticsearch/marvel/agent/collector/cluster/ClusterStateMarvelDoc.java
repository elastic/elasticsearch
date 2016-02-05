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

    private ClusterState clusterState;
    private ClusterHealthStatus status;

    public ClusterState getClusterState() {
        return clusterState;
    }

    public void setClusterState(ClusterState clusterState) {
        this.clusterState = clusterState;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public void setStatus(ClusterHealthStatus status) {
        this.status = status;
    }

}
