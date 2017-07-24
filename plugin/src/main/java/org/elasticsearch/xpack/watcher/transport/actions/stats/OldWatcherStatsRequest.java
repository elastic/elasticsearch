/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.stats;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * The Request to get the watcher stats
 */
public class OldWatcherStatsRequest extends MasterNodeReadRequest<OldWatcherStatsRequest> {

    private boolean includeCurrentWatches;
    private boolean includeQueuedWatches;

    public OldWatcherStatsRequest() {
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

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        includeCurrentWatches = in.readBoolean();
        includeQueuedWatches = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(includeCurrentWatches);
        out.writeBoolean(includeQueuedWatches);
    }

    @Override
    public String toString() {
        return "watcher_stats";
    }
}
