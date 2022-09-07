/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.search.ReferenceManager;
import org.elasticsearch.common.RunningTimeRecorder;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.function.LongSupplier;

public class ShardIndexingTimeStats implements ReferenceManager.RefreshListener {
    public static final ShardIndexingTimeStats NO_OP = new ShardIndexingTimeStats(
        RunningTimeRecorder.NO_OP,
        RunningTimeRecorder.NO_OP,
        RunningTimeRecorder.NO_OP
    );
    private final RunningTimeRecorder bulkTimeRecorder;
    private final RunningTimeRecorder mergeTimeRecorder;
    private final RunningTimeRecorder refreshTimeRecorder;

    private volatile Releasable onGoingRefresh;

    public ShardIndexingTimeStats(LongSupplier relativeTimeSupplier) {
        this(
            new RunningTimeRecorder(relativeTimeSupplier),
            new RunningTimeRecorder(relativeTimeSupplier),
            new RunningTimeRecorder(relativeTimeSupplier)
        );
    }

    private ShardIndexingTimeStats(
        RunningTimeRecorder bulkTimeRecorder,
        RunningTimeRecorder mergeTimeRecorder,
        RunningTimeRecorder refreshTimeRecorder
    ) {
        this.bulkTimeRecorder = bulkTimeRecorder;
        this.mergeTimeRecorder = mergeTimeRecorder;
        this.refreshTimeRecorder = refreshTimeRecorder;
    }

    public Releasable startTrackingBulkOperationTime() {
        return bulkTimeRecorder.trackRunningTime();
    }

    public long totalBulkTimeInNanos() {
        return bulkTimeRecorder.totalRunningTimeInNanos();
    }

    Releasable startTrackingMergeTime() {
        return mergeTimeRecorder.trackRunningTime();
    }

    public long totalMergeTimeInNanos() {
        return mergeTimeRecorder.totalRunningTimeInNanos();
    }

    @Override
    public void beforeRefresh() throws IOException {
        onGoingRefresh = refreshTimeRecorder.trackRunningTime();
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        Releasable refreshReleasable = onGoingRefresh;
        onGoingRefresh = null;
        if (refreshReleasable != null) {
            refreshReleasable.close();
        }
    }

    public long totalRefreshTimeInNanos() {
        return refreshTimeRecorder.totalRunningTimeInNanos();
    }
}
