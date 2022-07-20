/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.search.ReferenceManager;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.bulk.stats.BulkLoadTracker;

import java.io.IOException;
import java.util.function.LongSupplier;

public class WriteLoadTracker implements ReferenceManager.RefreshListener {
    public static final WriteLoadTracker NO_OP = new WriteLoadTracker(BulkLoadTracker.NO_OP, BulkLoadTracker.NO_OP, BulkLoadTracker.NO_OP);
    private final BulkLoadTracker bulkLoadTracker;
    private final BulkLoadTracker mergeLoadTracker;
    private final BulkLoadTracker refreshLoadTracker;

    private volatile Releasable onGoingRefresh;

    public WriteLoadTracker(LongSupplier relativeTimeSupplier) {
        this(
            new BulkLoadTracker(relativeTimeSupplier),
            new BulkLoadTracker(relativeTimeSupplier),
            new BulkLoadTracker(relativeTimeSupplier)
        );
    }

    private WriteLoadTracker(BulkLoadTracker bulkLoadTracker, BulkLoadTracker mergeLoadTracker, BulkLoadTracker refreshLoadTracker) {
        this.bulkLoadTracker = bulkLoadTracker;
        this.mergeLoadTracker = mergeLoadTracker;
        this.refreshLoadTracker = refreshLoadTracker;
    }

    public Releasable startTrackingBulkLoad() {
        return bulkLoadTracker.trackWriteLoad();
    }

    public long totalBulkTimeInNanos() {
        return bulkLoadTracker.totalIndexingTimeInNanos();
    }

    Releasable startTrackingMergeLoad() {
        return mergeLoadTracker.trackWriteLoad();
    }

    public long totalMergeTimeInNanos() {
        return mergeLoadTracker.totalIndexingTimeInNanos();
    }

    @Override
    public void beforeRefresh() throws IOException {
        onGoingRefresh = refreshLoadTracker.trackWriteLoad();
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        onGoingRefresh.close();
    }

    public long totalRefreshTimeInNanos() {
        return refreshLoadTracker.totalIndexingTimeInNanos();
    }
}
