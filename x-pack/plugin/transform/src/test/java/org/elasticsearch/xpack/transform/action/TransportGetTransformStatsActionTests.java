/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStatsTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.time.Instant;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetTransformStatsActionTests extends ESTestCase {

    private TransformTask task = mock(TransformTask.class);

    public void testDeriveStatsStopped() {
        String transformId = "transform-with-stats";
        String reason = "";
        TransformIndexerStats stats = TransformIndexerStatsTests.randomStats();
        TransformState stoppedState =
            new TransformState(TransformTaskState.STOPPED, IndexerState.STOPPED, null, 0, reason, null, null, true);
        withIdStateAndStats(transformId, stoppedState, stats);
        TransformCheckpointingInfo info = new TransformCheckpointingInfo(
            new TransformCheckpointStats(1, null, null, 1, 1),
            new TransformCheckpointStats(2, null, null, 2, 5),
            2,
            Instant.now());

        assertThat(TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(new TransformStats(transformId, TransformStats.State.STOPPED, "", null, stats, TransformCheckpointingInfo.EMPTY)));
        assertThat(TransportGetTransformStatsAction.deriveStats(task, info),
            equalTo(new TransformStats(transformId, TransformStats.State.STOPPED, "", null, stats, info)));


        reason = "foo";
        stoppedState = new TransformState(TransformTaskState.STOPPED, IndexerState.STOPPED, null, 0, reason, null, null, true);
        withIdStateAndStats(transformId, stoppedState, stats);

        assertThat(TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(new TransformStats(transformId, TransformStats.State.STOPPED, reason, null, stats, TransformCheckpointingInfo.EMPTY)));
        assertThat(TransportGetTransformStatsAction.deriveStats(task, info),
            equalTo(new TransformStats(transformId, TransformStats.State.STOPPED, reason, null, stats, info)));
    }

    public void testDeriveStatsFailed() {
        String transformId = "transform-with-stats";
        String reason = "";
        TransformIndexerStats stats = TransformIndexerStatsTests.randomStats();
        TransformState failedState =
            new TransformState(TransformTaskState.FAILED, IndexerState.STOPPED, null, 0, reason, null, null, true);
        withIdStateAndStats(transformId, failedState, stats);
        TransformCheckpointingInfo info = new TransformCheckpointingInfo(
            new TransformCheckpointStats(1, null, null, 1, 1),
            new TransformCheckpointStats(2, null, null, 2, 5),
            2,
            Instant.now());

        assertThat(TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(new TransformStats(transformId, TransformStats.State.FAILED, "", null, stats, TransformCheckpointingInfo.EMPTY)));
        assertThat(TransportGetTransformStatsAction.deriveStats(task, info),
            equalTo(new TransformStats(transformId, TransformStats.State.FAILED, "", null, stats, info)));


        reason = "the task is failed";
        failedState = new TransformState(TransformTaskState.FAILED, IndexerState.STOPPED, null, 0, reason, null, null, true);
        withIdStateAndStats(transformId, failedState, stats);

        assertThat(TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(new TransformStats(transformId, TransformStats.State.FAILED, reason, null, stats, TransformCheckpointingInfo.EMPTY)));
        assertThat(TransportGetTransformStatsAction.deriveStats(task, info),
            equalTo(new TransformStats(transformId, TransformStats.State.FAILED, reason, null, stats, info)));
    }


    public void testDeriveStats() {
        String transformId = "transform-with-stats";
        String reason = "";
        TransformIndexerStats stats = TransformIndexerStatsTests.randomStats();
        TransformState runningState =
            new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 0, reason, null, null, true);
        withIdStateAndStats(transformId, runningState, stats);
        TransformCheckpointingInfo info = new TransformCheckpointingInfo(
            new TransformCheckpointStats(1, null, null, 1, 1),
            new TransformCheckpointStats(2, null, null, 2, 5),
            2,
            Instant.now());

        assertThat(TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(new TransformStats(transformId, TransformStats.State.STOPPING,
                "transform is set to stop at the next checkpoint", null, stats, TransformCheckpointingInfo.EMPTY)));
        assertThat(TransportGetTransformStatsAction.deriveStats(task, info),
            equalTo(new TransformStats(transformId, TransformStats.State.STOPPING,
                "transform is set to stop at the next checkpoint", null, stats, info)));


        reason = "foo";
        runningState = new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 0, reason, null, null, true);
        withIdStateAndStats(transformId, runningState, stats);

        assertThat(TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(new TransformStats(transformId, TransformStats.State.STOPPING, reason, null, stats, TransformCheckpointingInfo.EMPTY)));
        assertThat(TransportGetTransformStatsAction.deriveStats(task, info),
            equalTo(new TransformStats(transformId, TransformStats.State.STOPPING, reason, null, stats, info)));

        // Stop at next checkpoint is false.
        runningState = new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 0, reason, null, null, false);
        withIdStateAndStats(transformId, runningState, stats);

        assertThat(TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(new TransformStats(transformId, TransformStats.State.INDEXING, reason, null, stats, TransformCheckpointingInfo.EMPTY)));
        assertThat(TransportGetTransformStatsAction.deriveStats(task, info),
            equalTo(new TransformStats(transformId, TransformStats.State.INDEXING, reason, null, stats, info)));
    }

    private void withIdStateAndStats(String transformId, TransformState state, TransformIndexerStats stats) {
        when(task.getTransformId()).thenReturn(transformId);
        when(task.getState()).thenReturn(state);
        when(task.getStats()).thenReturn(stats);
    }

}
