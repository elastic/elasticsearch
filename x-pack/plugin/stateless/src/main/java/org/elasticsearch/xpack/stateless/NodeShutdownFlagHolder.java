/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;

/**
 * Tracks whether this node has been marked for shutdown.
 */
class NodeShutdownFlagHolder implements ClusterStateListener {

    private volatile boolean isNodeShuttingDown;

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (event.state().metadata().nodeShutdowns().contains(event.state().nodes().getLocalNodeId())) {
            isNodeShuttingDown = true;
        }
    }

    public boolean isNodeShuttingDown() {
        return isNodeShuttingDown;
    }
}
