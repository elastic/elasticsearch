/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo.TransformCheckpointingInfoBuilder;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.util.Collections;

/**
 * mock of a time based checkpoint provider for testing
 */
public class MockTimebasedCheckpointProvider implements CheckpointProvider {

    private final TransformConfig transformConfig;
    private final TimeSyncConfig timeSyncConfig;

    public MockTimebasedCheckpointProvider(final TransformConfig transformConfig) {
        this.transformConfig = ExceptionsHelper.requireNonNull(transformConfig, "transformConfig");
        timeSyncConfig = ExceptionsHelper.requireNonNull((TimeSyncConfig) transformConfig.getSyncConfig(), "timeSyncConfig");
    }

    @Override
    public void createNextCheckpoint(TransformCheckpoint lastCheckpoint, ActionListener<TransformCheckpoint> listener) {

        final long timestamp = System.currentTimeMillis();
        long timeUpperBound = timestamp - timeSyncConfig.getDelay().millis();

        if (TransformCheckpoint.isNullOrEmpty(lastCheckpoint)) {
            listener.onResponse(new TransformCheckpoint(transformConfig.getId(), timestamp, 1, Collections.emptyMap(), timeUpperBound));
            return;
        }

        listener.onResponse(
            new TransformCheckpoint(
                transformConfig.getId(),
                timestamp,
                lastCheckpoint.getCheckpoint() + 1,
                Collections.emptyMap(),
                timeUpperBound
            )
        );
    }

    @Override
    public void sourceHasChanged(TransformCheckpoint lastCheckpoint, ActionListener<Boolean> listener) {
        // lets pretend there are always changes
        listener.onResponse(true);
    }

    @Override
    public void getCheckpointingInfo(
        TransformCheckpoint lastCheckpoint,
        TransformCheckpoint nextCheckpoint,
        TransformIndexerPosition nextCheckpointPosition,
        TransformProgress nextCheckpointProgress,
        ActionListener<TransformCheckpointingInfoBuilder> listener
    ) {
        TransformCheckpointingInfoBuilder checkpointingInfoBuilder = new TransformCheckpointingInfoBuilder();
        checkpointingInfoBuilder.setLastCheckpoint(lastCheckpoint)
            .setNextCheckpoint(nextCheckpoint)
            .setNextCheckpointPosition(nextCheckpointPosition)
            .setNextCheckpointProgress(nextCheckpointProgress);

        long timestamp = System.currentTimeMillis();
        TransformCheckpoint sourceCheckpoint = new TransformCheckpoint(transformConfig.getId(), timestamp, -1L, Collections.emptyMap(), 0L);
        checkpointingInfoBuilder.setSourceCheckpoint(sourceCheckpoint);
        checkpointingInfoBuilder.setOperationsBehind(TransformCheckpoint.getBehind(lastCheckpoint, sourceCheckpoint));
        listener.onResponse(checkpointingInfoBuilder);
    }

    @Override
    public void getCheckpointingInfo(
        long lastCheckpointNumber,
        TransformIndexerPosition nextCheckpointPosition,
        TransformProgress nextCheckpointProgress,
        ActionListener<TransformCheckpointingInfoBuilder> listener
    ) {
        long timestamp = System.currentTimeMillis();

        // emulate the 2 last checkpoints as if last run 1 second ago and next right now
        TransformCheckpoint lastCheckpoint = new TransformCheckpoint(
            transformConfig.getId(),
            timestamp - 1000,
            lastCheckpointNumber,
            Collections.emptyMap(),
            timestamp - 1000 - timeSyncConfig.getDelay().millis()
        );

        TransformCheckpoint nextCheckpoint = new TransformCheckpoint(
            transformConfig.getId(),
            timestamp,
            lastCheckpointNumber + 1,
            Collections.emptyMap(),
            timestamp - timeSyncConfig.getDelay().millis()
        );

        getCheckpointingInfo(lastCheckpoint, nextCheckpoint, nextCheckpointPosition, nextCheckpointProgress, listener);
    }

}
