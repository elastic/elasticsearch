/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.xpack.core.transform.transforms.TransformHealth;
import org.elasticsearch.xpack.core.transform.transforms.TransformHealthIssue;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStatsTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.transforms.TransformContext;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetTransformStatsActionTests extends ESTestCase {

    private static final TransformCheckpointingInfo CHECKPOINTING_INFO = new TransformCheckpointingInfo(
        new TransformCheckpointStats(1, null, null, 1, 1),
        new TransformCheckpointStats(2, null, null, 2, 5),
        2,
        Instant.now(),
        Instant.now()
    );
    private final TransformTask task = mock(TransformTask.class);

    public void testDeriveStatsStopped() {
        String transformId = "transform-with-stats";
        String reason = null;
        TransformIndexerStats stats = TransformIndexerStatsTests.randomStats();
        TransformState stoppedState = new TransformState(
            TransformTaskState.STOPPED,
            IndexerState.STOPPED,
            null,
            0,
            reason,
            null,
            null,
            true,
            null
        );
        withIdStateAndStats(transformId, stoppedState, stats);
        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.STOPPED,
                    reason,
                    null,
                    stats,
                    TransformCheckpointingInfo.EMPTY,
                    TransformHealth.GREEN,
                    null
                )
            )
        );
        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, CHECKPOINTING_INFO),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.STOPPED,
                    reason,
                    null,
                    stats,
                    CHECKPOINTING_INFO,
                    TransformHealth.GREEN,
                    null
                )
            )
        );

        reason = "foo";
        stoppedState = new TransformState(TransformTaskState.STOPPED, IndexerState.STOPPED, null, 0, reason, null, null, true, null);
        withIdStateAndStats(transformId, stoppedState, stats);

        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.STOPPED,
                    reason,
                    null,
                    stats,
                    TransformCheckpointingInfo.EMPTY,
                    TransformHealth.GREEN,
                    null
                )
            )
        );
        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, CHECKPOINTING_INFO),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.STOPPED,
                    reason,
                    null,
                    stats,
                    CHECKPOINTING_INFO,
                    TransformHealth.GREEN,
                    null
                )
            )
        );
    }

    public void testDeriveStatsStoppedWithAuthStateGreen() {
        testDeriveStatsStoppedWithAuthState(null, AuthorizationState.green(), TransformCheckpointingInfo.EMPTY);
        testDeriveStatsStoppedWithAuthState(CHECKPOINTING_INFO, AuthorizationState.green(), CHECKPOINTING_INFO);
    }

    public void testDeriveStatsStoppedWithAuthStateRed() {
        AuthorizationState redAuthState = AuthorizationState.red(new ElasticsearchSecurityException("missing privileges"));
        testDeriveStatsStoppedWithAuthState(null, redAuthState, TransformCheckpointingInfo.EMPTY);
        testDeriveStatsStoppedWithAuthState(CHECKPOINTING_INFO, redAuthState, CHECKPOINTING_INFO);
    }

    private void testDeriveStatsStoppedWithAuthState(
        TransformCheckpointingInfo info,
        AuthorizationState authState,
        TransformCheckpointingInfo expectedInfo
    ) {
        String transformId = "transform-with-auth-state";
        TransformIndexerStats stats = TransformIndexerStatsTests.randomStats();
        TransformState stoppedState = new TransformState(
            TransformTaskState.STOPPED,
            IndexerState.STOPPED,
            null,
            0,
            null,
            null,
            null,
            true,
            authState
        );
        withIdStateAndStats(transformId, stoppedState, stats);

        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, info),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.STOPPED,
                    null,
                    null,
                    stats,
                    expectedInfo,
                    TransformHealth.GREEN,
                    authState
                )
            )
        );
    }

    public void testDeriveStatsFailed() {
        String transformId = "transform-with-stats";
        String reason = null;
        TransformHealth expectedHealth = new TransformHealth(
            HealthStatus.RED,
            List.of(new TransformHealthIssue("Transform task state is [failed]", null, 1, null))
        );

        TransformIndexerStats stats = TransformIndexerStatsTests.randomStats();
        TransformState failedState = new TransformState(
            TransformTaskState.FAILED,
            IndexerState.STOPPED,
            null,
            0,
            reason,
            null,
            null,
            true,
            null
        );
        withIdStateAndStats(transformId, failedState, stats);
        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.FAILED,
                    reason,
                    null,
                    stats,
                    TransformCheckpointingInfo.EMPTY,
                    expectedHealth,
                    null
                )
            )
        );
        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, CHECKPOINTING_INFO),
            equalTo(
                new TransformStats(transformId, TransformStats.State.FAILED, reason, null, stats, CHECKPOINTING_INFO, expectedHealth, null)
            )
        );

        reason = "the task is failed";
        expectedHealth = new TransformHealth(
            HealthStatus.RED,
            List.of(new TransformHealthIssue("Transform task state is [failed]", reason, 1, null))
        );
        failedState = new TransformState(TransformTaskState.FAILED, IndexerState.STOPPED, null, 0, reason, null, null, true, null);
        withIdStateAndStats(transformId, failedState, stats);

        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.FAILED,
                    reason,
                    null,
                    stats,
                    TransformCheckpointingInfo.EMPTY,
                    expectedHealth,
                    null
                )
            )
        );
        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, CHECKPOINTING_INFO),
            equalTo(
                new TransformStats(transformId, TransformStats.State.FAILED, reason, null, stats, CHECKPOINTING_INFO, expectedHealth, null)
            )
        );
    }

    public void testDeriveStats() {
        String transformId = "transform-with-stats";
        String reason = null;
        TransformIndexerStats stats = TransformIndexerStatsTests.randomStats();
        TransformState runningState = new TransformState(
            TransformTaskState.STARTED,
            IndexerState.INDEXING,
            null,
            0,
            reason,
            null,
            null,
            true,
            null
        );
        withIdStateAndStats(transformId, runningState, stats);

        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.STOPPING,
                    "transform is set to stop at the next checkpoint",
                    null,
                    stats,
                    TransformCheckpointingInfo.EMPTY,
                    TransformHealth.GREEN,
                    null
                )
            )
        );
        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, CHECKPOINTING_INFO),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.STOPPING,
                    "transform is set to stop at the next checkpoint",
                    null,
                    stats,
                    CHECKPOINTING_INFO,
                    TransformHealth.GREEN,
                    null
                )
            )
        );

        reason = "foo";
        runningState = new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 0, reason, null, null, true, null);
        withIdStateAndStats(transformId, runningState, stats);

        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.STOPPING,
                    reason,
                    null,
                    stats,
                    TransformCheckpointingInfo.EMPTY,
                    TransformHealth.GREEN,
                    null
                )
            )
        );
        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, CHECKPOINTING_INFO),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.STOPPING,
                    reason,
                    null,
                    stats,
                    CHECKPOINTING_INFO,
                    TransformHealth.GREEN,
                    null
                )
            )
        );

        // Stop at next checkpoint is false.
        runningState = new TransformState(TransformTaskState.STARTED, IndexerState.INDEXING, null, 0, reason, null, null, false, null);
        withIdStateAndStats(transformId, runningState, stats);

        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, null),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.INDEXING,
                    reason,
                    null,
                    stats,
                    TransformCheckpointingInfo.EMPTY,
                    TransformHealth.GREEN,
                    null
                )
            )
        );
        assertThat(
            TransportGetTransformStatsAction.deriveStats(task, CHECKPOINTING_INFO),
            equalTo(
                new TransformStats(
                    transformId,
                    TransformStats.State.INDEXING,
                    reason,
                    null,
                    stats,
                    CHECKPOINTING_INFO,
                    TransformHealth.GREEN,
                    null
                )
            )
        );
    }

    private void withIdStateAndStats(String transformId, TransformState state, TransformIndexerStats stats) {
        when(task.getTransformId()).thenReturn(transformId);
        when(task.getState()).thenReturn(state);
        when(task.getStats()).thenReturn(stats);
        when(task.getContext()).thenReturn(new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class)));
    }
}
