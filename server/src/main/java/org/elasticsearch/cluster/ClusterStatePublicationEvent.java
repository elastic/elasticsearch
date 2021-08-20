/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

/**
 * Represents a cluster state update computed by the {@link org.elasticsearch.cluster.service.MasterService} for publication to the cluster.
 * If publication is successful then this creates a {@link ClusterChangedEvent} which is applied on every node.
 */
public class ClusterStatePublicationEvent {

    private final String summary;
    private final ClusterState oldState;
    private final ClusterState newState;

    public ClusterStatePublicationEvent(String summary, ClusterState oldState, ClusterState newState) {
        this.summary = summary;
        this.oldState = oldState;
        this.newState = newState;
    }

    public String getSummary() {
        return summary;
    }

    public ClusterState getOldState() {
        return oldState;
    }

    public ClusterState getNewState() {
        return newState;
    }
}
