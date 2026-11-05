/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.xpack.core.downsample.DownsampleShardIndexerStatus;
import org.elasticsearch.xpack.core.downsample.DownsampleShardProgress;

/**
 * What {@link DownsampleShardIndexer} needs from whatever is running it, so that the indexer does not depend on how a
 * shard run is scheduled and tracked (today a persistent task).
 */
public interface ShardTaskReporter {

    /**
     * The counters the indexer updates while it scans the shard.
     */
    DownsampleShardProgress progress();

    /**
     * Whether the run was cancelled. Polled by the indexer while it scans, so it must be cheap.
     */
    boolean isCancelled();

    /**
     * An identifier of the run, used in log messages only.
     */
    String id();

    /**
     * Records a status change of the indexer ({@code STARTED}, {@code COMPLETED}, {@code FAILED} or {@code CANCELLED}):
     * it updates the status in {@link #progress()} and publishes the change to wherever the run is tracked. Publishing is
     * best effort and never blocks or fails the indexer.
     */
    void publishStatus(DownsampleShardIndexerStatus status);
}
