/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.stats;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 * The Request to get the watcher stats
 */
public class WatcherStatsRequest extends BaseNodesRequest<WatcherStatsRequest> {

    private boolean includeCurrentWatches;
    private boolean includeQueuedWatches;
    private boolean includeStats;

    public WatcherStatsRequest() {
        super((String[]) null);
    }

    public WatcherStatsRequest(StreamInput in) throws IOException {
        super(in);
        includeCurrentWatches = in.readBoolean();
        includeQueuedWatches = in.readBoolean();
        includeStats = in.readBoolean();
    }

    public boolean includeCurrentWatches() {
        return includeCurrentWatches;
    }

    public void includeCurrentWatches(boolean currentWatches) {
        this.includeCurrentWatches = currentWatches;
    }

    public boolean includeQueuedWatches() {
        return includeQueuedWatches;
    }

    public void includeQueuedWatches(boolean includeQueuedWatches) {
        this.includeQueuedWatches = includeQueuedWatches;
    }

    public boolean includeStats() {
        return includeStats;
    }

    public void includeStats(boolean includeStats) {
        this.includeStats = includeStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(includeCurrentWatches);
        out.writeBoolean(includeQueuedWatches);
        out.writeBoolean(includeStats);
    }

    @Override
    public String toString() {
        return "watcher_stats";
    }

    public static class Node extends TransportRequest {

        private boolean includeCurrentWatches;
        private boolean includeQueuedWatches;
        private boolean includeStats;

        public Node(StreamInput in) throws IOException {
            super(in);
            includeCurrentWatches = in.readBoolean();
            includeQueuedWatches = in.readBoolean();
            includeStats = in.readBoolean();
        }

        public Node(WatcherStatsRequest request) {
            includeCurrentWatches = request.includeCurrentWatches();
            includeQueuedWatches = request.includeQueuedWatches();
            includeStats = request.includeStats();
        }

        public boolean includeCurrentWatches() {
            return includeCurrentWatches;
        }

        public boolean includeQueuedWatches() {
            return includeQueuedWatches;
        }

        public boolean includeStats() {
            return includeStats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(includeCurrentWatches);
            out.writeBoolean(includeQueuedWatches);
            out.writeBoolean(includeStats);
        }
    }
}
