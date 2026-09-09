/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.common.settings.ClusterSettings;

/**
 * Holds dynamic settings for {@link IndexEngine}.
 * <p>
 * A single instance is created per node and shared across all {@link IndexEngine} instances.  Keeping the
 * listener registration here rather than inside each engine avoids holding a reference from the long-lived
 * {@link ClusterSettings} to every engine instance, which would otherwise prevent garbage collection of
 * closed engines (memory leak).
 * <p>
 * Values are stored in {@code volatile} fields so that updates made by the settings framework are immediately
 * visible to engine threads without locking.
 * <p>
 * Note: an already-throttled shard will not re-evaluate the backlog threshold until the next merge enqueue
 * or completion event; this is accepted behavior for a dynamic setting.
 */
public class IndexEngineDynamicSettings {

    private volatile long mergeForceRefreshSizeBytes;
    private volatile int mergeBacklogThrottleFactor;

    public IndexEngineDynamicSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(
            IndexEngine.MERGE_FORCE_REFRESH_SIZE,
            value -> this.mergeForceRefreshSizeBytes = value.getBytes()
        );
        clusterSettings.initializeAndWatch(IndexEngine.MERGE_BACKLOG_THROTTLE_FACTOR, value -> this.mergeBacklogThrottleFactor = value);
    }

    /**
     * Returns the current threshold (in bytes) above which a completed merge triggers an immediate refresh.
     */
    public long mergeForceRefreshSizeBytes() {
        return mergeForceRefreshSizeBytes;
    }

    /**
     * Returns the current multiplier applied to the merge thread count to derive the active-merge throttle threshold.
     */
    public int mergeBacklogThrottleFactor() {
        return mergeBacklogThrottleFactor;
    }
}
