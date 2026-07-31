/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;

/**
 * Flushes everything buffered as soon as this node is marked for shutdown.
 *
 * <p>This is the last point at which a flush can actually land. Plugins are closed near the end of {@code Node.close()}, after the
 * cluster service, the indices service and the transport service have already been shut down, so a bulk sent from {@code close()} has
 * nowhere to go. The shutdown API marks the node in cluster state well before any of that, which is early enough to still write.
 *
 * <p>It does not help a node that is killed outright — nothing short of persisting the buffer would — but an orderly shutdown, which is
 * how nodes usually go away, no longer discards the interval in progress.
 */
public class DerivedMetricsShutdownListener implements ClusterStateListener {

    private final ClusterService clusterService;
    private final DerivedMetricsService service;

    private volatile boolean flushed;

    public DerivedMetricsShutdownListener(ClusterService clusterService, DerivedMetricsService service) {
        this.clusterService = clusterService;
        this.service = service;
    }

    public void init() {
        clusterService.addListener(this);
    }

    public void close() {
        clusterService.removeListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        String localNodeId = event.state().nodes().getLocalNodeId();
        boolean shuttingDown = event.state()
            .metadata()
            .custom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY)
            .getAll()
            .containsKey(localNodeId);
        if (shuttingDown == false) {
            // the shutdown was cancelled, so arm again for next time
            flushed = false;
            return;
        }
        if (flushed) {
            return;
        }
        flushed = true;
        service.flushEverything("node [" + localNodeId + "] is shutting down");
    }
}
