/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.checkpoint;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerPosition;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfo;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;

/**
 * Interface for checkpoint creation, checking for changes and getting statistics about checkpoints
 */
public interface CheckpointProvider {

    /**
     * Create a new checkpoint
     *
     * @param lastCheckpoint the last checkpoint
     * @param listener listener to call after inner request returned
     */
    void createNextCheckpoint(DataFrameTransformCheckpoint lastCheckpoint, ActionListener<DataFrameTransformCheckpoint> listener);

    /**
     * Determines whether the data frame needs updating
     *
     * @param lastCheckpoint the last checkpoint
     * @param listener listener to send the result to
     */
    void sourceHasChanged(DataFrameTransformCheckpoint lastCheckpoint, ActionListener<Boolean> listener);

    /**
     * Get checkpoint statistics for a running data frame
     *
     * For running data frames most information is available in-memory.
     *
     * @param lastCheckpoint the last checkpoint
     * @param nextCheckpoint the next checkpoint
     * @param nextCheckpointPosition position for the next checkpoint
     * @param nextCheckpointProgress progress for the next checkpoint
     * @param listener listener to retrieve the result
     */
    void getCheckpointingInfo(DataFrameTransformCheckpoint lastCheckpoint,
                              DataFrameTransformCheckpoint nextCheckpoint,
                              DataFrameIndexerPosition nextCheckpointPosition,
                              DataFrameTransformProgress nextCheckpointProgress,
                              ActionListener<DataFrameTransformCheckpointingInfo> listener);

    /**
     * Get checkpoint statistics for a stopped data frame
     *
     * For stopped data frames we need to do lookups in the internal index.
     *
     * @param lastCheckpointNumber the last checkpoint number
     * @param nextCheckpointPosition position for the next checkpoint
     * @param nextCheckpointProgress progress for the next checkpoint
     * @param listener listener to retrieve the result
     */
    void getCheckpointingInfo(long lastCheckpointNumber,
                              DataFrameIndexerPosition nextCheckpointPosition,
                              DataFrameTransformProgress nextCheckpointProgress,
                              ActionListener<DataFrameTransformCheckpointingInfo> listener);
}
