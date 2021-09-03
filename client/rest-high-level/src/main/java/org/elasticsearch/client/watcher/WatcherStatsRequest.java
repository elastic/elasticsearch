/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.client.Validatable;

/**
 * A request to explicitly acknowledge a watch.
 */
public class WatcherStatsRequest implements Validatable {

    private final boolean includeCurrentWatches;
    private final boolean includeQueuedWatches;

    public WatcherStatsRequest( ) {
        this(true, true);
    }

    public WatcherStatsRequest(boolean includeCurrentWatches, boolean includeQueuedWatches) {
        this.includeCurrentWatches = includeCurrentWatches;
        this.includeQueuedWatches = includeQueuedWatches;
    }

    public boolean includeCurrentWatches() {
        return includeCurrentWatches;
    }

    public boolean includeQueuedWatches() {
        return includeQueuedWatches;
    }

    @Override
    public String toString() {
        return "stats [current=" + includeCurrentWatches + ", " + "queued=" + includeQueuedWatches + "]";
    }
}
