/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo.TransformCheckpointingInfoBuilder;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;

/**
 * Interface for checkpoint creation, checking for changes and getting statistics about checkpoints
 */
public interface CheckpointProvider {

    /**
     * Fail-fast starting bound for coordinator → node GetCheckpoint. A silent node must not pin the
     * indexer in {@code INDEXING} for the full 12h walk timeout.
     */
    TimeValue MIN_GET_INDEX_CHECKPOINTS_TIMEOUT = TimeValue.timeValueSeconds(30);

    /**
     * Upper bound so a healthy node can still checkpoint a huge searchable-snapshot source. Same
     * 12h ceiling as the original internal GetCheckpoint timeout.
     */
    TimeValue MAX_GET_INDEX_CHECKPOINTS_TIMEOUT = TimeValue.timeValueHours(12);

    /**
     * GetCheckpoint timeout for {@link #createNextCheckpoint}. Starts at
     * {@link #MIN_GET_INDEX_CHECKPOINTS_TIMEOUT} and doubles with each indexer failure, capped at
     * {@link #MAX_GET_INDEX_CHECKPOINTS_TIMEOUT}.
     * <p>
     * A silent node fails the first attempt in 30s so unattended transforms can retry. A checkpoint
     * that legitimately takes longer (huge searchable-snapshot source) succeeds on a later attempt
     * once the timeout has grown.
     *
     * @param failureCount current {@code TransformContext} failure count
     */
    static TimeValue getIndexCheckpointsTimeout(int failureCount) {
        // Math.min(failureCount, 32) avoids overflow of the left-shift, matching
        // TransformSchedulingUtils.calculateNextScheduledTime.
        long timeoutMillis = Math.min(
            MIN_GET_INDEX_CHECKPOINTS_TIMEOUT.millis() << Math.min(Math.max(failureCount, 0), 32),
            MAX_GET_INDEX_CHECKPOINTS_TIMEOUT.millis()
        );
        return TimeValue.timeValueMillis(timeoutMillis);
    }

    /**
     * Create a new checkpoint
     *
     * @param lastCheckpoint the last checkpoint
     * @param listener listener to call after inner request returned
     */
    void createNextCheckpoint(TransformCheckpoint lastCheckpoint, ActionListener<TransformCheckpoint> listener);

    /**
     * Create a new checkpoint using {@code timeout} for GetCheckpoint (sender and receiver).
     * <p>
     * Default ignores {@code timeout} and delegates to {@link #createNextCheckpoint(TransformCheckpoint, ActionListener)}
     * so test doubles that only implement the two-argument form keep working.
     */
    default void createNextCheckpoint(TransformCheckpoint lastCheckpoint, TimeValue timeout, ActionListener<TransformCheckpoint> listener) {
        createNextCheckpoint(lastCheckpoint, listener);
    }

    /**
     * Determines whether the transform needs updating
     *
     * @param lastCheckpoint the last checkpoint
     * @param listener listener to send the result to
     */
    void sourceHasChanged(TransformCheckpoint lastCheckpoint, ActionListener<Boolean> listener);

    /**
     * Get checkpoint statistics for a running transform
     *
     * For running transforms most information is available in-memory.
     *
     * @param lastCheckpoint the last checkpoint
     * @param nextCheckpoint the next checkpoint
     * @param nextCheckpointPosition position for the next checkpoint
     * @param nextCheckpointProgress progress for the next checkpoint
     * @param listener listener to retrieve the result
     */
    void getCheckpointingInfo(
        TransformCheckpoint lastCheckpoint,
        TransformCheckpoint nextCheckpoint,
        TransformIndexerPosition nextCheckpointPosition,
        TransformProgress nextCheckpointProgress,
        TimeValue timeout,
        ActionListener<TransformCheckpointingInfoBuilder> listener
    );

    /**
     * Get checkpoint statistics for a stopped transform
     *
     * For stopped transforms we need to do lookups in the internal index.
     *
     * @param lastCheckpointNumber the last checkpoint number
     * @param nextCheckpointPosition position for the next checkpoint
     * @param nextCheckpointProgress progress for the next checkpoint
     * @param listener listener to retrieve the result
     */
    void getCheckpointingInfo(
        long lastCheckpointNumber,
        TransformIndexerPosition nextCheckpointPosition,
        TransformProgress nextCheckpointProgress,
        TimeValue timeout,
        ActionListener<TransformCheckpointingInfoBuilder> listener
    );
}
