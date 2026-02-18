/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.snapshots;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link IndexShardSnapshotStatus}, in particular that totalTimeMillis is not set
 * incorrectly when moveToFailed or moveToUnsuccessful is called before the snapshot has started.
 */
public class IndexShardSnapshotStatusTests extends ESTestCase {

    public void testFailureOrPauseBeforeStartRecordsZeroTotalTimeMillis() {
        final var status = IndexShardSnapshotStatus.newInitializing(new ShardGeneration("gen"));
        assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.INIT));
        assertThat(status.getTotalTimeMillis(), equalTo(0L));

        final long endTime = randomInstantBetween(Instant.now(), Instant.now().plus(30, ChronoUnit.DAYS)).toEpochMilli();
        final var destinationState = randomFrom(IndexShardSnapshotStatus.Stage.PAUSED, IndexShardSnapshotStatus.Stage.FAILURE);
        switch (destinationState) {
            case IndexShardSnapshotStatus.Stage.PAUSED -> {
                status.pauseIfNotCompleted(listener -> listener.onResponse(null));
                assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.PAUSING));
                status.moveToUnsuccessful(IndexShardSnapshotStatus.Stage.PAUSED, "paused", endTime);
                assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.PAUSED));
            }
            case IndexShardSnapshotStatus.Stage.FAILURE -> {
                // Maybe we were pausing when we failed
                if (randomBoolean()) {
                    status.pauseIfNotCompleted(listener -> listener.onResponse(null));
                    assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.PAUSING));
                }
                status.moveToFailed(endTime, "failed before start");
                assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.FAILURE));
            }
        }
        assertThat(status.getTotalTimeMillis(), equalTo(0L));
    }

    public void testFailurePauseOrCompletionAfterStartRecordsTotalTimeMillis() {
        IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(new ShardGeneration("gen"));
        assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.INIT));
        assertThat(status.getTotalTimeMillis(), equalTo(0L));

        final Instant startTime = randomInstantBetween(Instant.now(), Instant.now().plus(30, ChronoUnit.DAYS));
        status.moveToStarted(
            startTime.toEpochMilli(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );

        final long endTime = randomInstantBetween(startTime, startTime.plus(30, ChronoUnit.DAYS)).toEpochMilli();
        final var destinationState = randomFrom(
            IndexShardSnapshotStatus.Stage.PAUSED,
            IndexShardSnapshotStatus.Stage.FAILURE,
            IndexShardSnapshotStatus.Stage.DONE
        );
        switch (destinationState) {
            case IndexShardSnapshotStatus.Stage.PAUSED -> {
                status.pauseIfNotCompleted(listener -> listener.onResponse(null));
                assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.PAUSING));
                status.moveToUnsuccessful(IndexShardSnapshotStatus.Stage.PAUSED, "paused", endTime);
                assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.PAUSED));
            }
            case IndexShardSnapshotStatus.Stage.FAILURE -> {
                // Maybe we were pausing when we failed
                if (randomBoolean()) {
                    status.pauseIfNotCompleted(listener -> listener.onResponse(null));
                    assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.PAUSING));
                }
                status.moveToFailed(endTime, "failed before start");
                assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.FAILURE));
            }
            case IndexShardSnapshotStatus.Stage.DONE -> {
                status.moveToFinalize();
                status.moveToDone(endTime, new ShardSnapshotResult(new ShardGeneration("gen"), ByteSizeValue.MINUS_ONE, 1));
                assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.DONE));
            }
        }

        assertThat(status.getTotalTimeMillis(), equalTo(endTime - startTime.toEpochMilli()));
    }
}
