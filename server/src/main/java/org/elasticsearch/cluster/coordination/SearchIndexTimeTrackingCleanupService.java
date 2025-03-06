/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.search.stats.ShardSearchPerIndexTimeTrackingMetrics;

/**
 * Service responsible for cleaning up task execution time tracking for deleted indices.
 * Implements the ClusterStateListener interface to listen for cluster state changes.
 */
public class SearchIndexTimeTrackingCleanupService implements ClusterStateListener {

    private ShardSearchPerIndexTimeTrackingMetrics listener;

    /**
     * Constructor.
     *
     * @param listener the listener for shard search time tracking metrics
     */
    public SearchIndexTimeTrackingCleanupService(ShardSearchPerIndexTimeTrackingMetrics listener) {
        this.listener = listener;
    }

    /**
     * Called when the cluster state changes. Stops tracking execution time for deleted indices.
     *
     * @param event the cluster changed event
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        for (Index index : event.indicesDeleted()) {
            listener.stopTrackingIndex(index.getName());
        }
    }
}
