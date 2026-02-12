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

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@link IndexShardSnapshotStatus}, in particular that totalTimeMillis is not set
 * incorrectly when moveToFailed or moveToUnsuccessful is called before the snapshot has started.
 */
public class IndexShardSnapshotStatusTests extends ESTestCase {

    public void testMoveToFailedBeforeSnapshotStarted_doesNotSetIncorrectTotalTimeMillis() {
        IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(new ShardGeneration("gen"));
        assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.INIT));
        assertThat(status.getTotalTimeMillis(), equalTo(0L));

        // Simulate failure before moveToStarted was ever called (e.g. failure in asyncCreate).
        // Using a large endTime that would be wrong if stored as totalTimeMillis (e.g. current time in ms).
        long largeEndTime = 1_700_000_000_000L; // arbitrary large value
        status.moveToFailed(largeEndTime, "failed before start");

        // totalTimeMillis must remain 0, not endTime - 0 which would be the wrong value
        assertThat(status.getTotalTimeMillis(), equalTo(0L));
        assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.FAILURE));
    }

    public void testMoveToUnsuccessfulPausedBeforeSnapshotStarted_doesNotSetIncorrectTotalTimeMillis() {
        IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(new ShardGeneration("gen"));
        // Go INIT -> PAUSING (e.g. node removal before snapshot actually started)
        status.pauseIfNotCompleted(listener -> listener.onResponse(null));
        assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.PAUSING));

        long largeEndTime = 1_700_000_000_000L;
        status.moveToUnsuccessful(IndexShardSnapshotStatus.Stage.PAUSED, "paused", largeEndTime);

        // totalTimeMillis must remain 0, not endTime - 0
        assertThat(status.getTotalTimeMillis(), equalTo(0L));
        assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.PAUSED));
    }

    public void testMoveToFailedAfterSnapshotStarted_setsTotalTimeMillis() {
        IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(new ShardGeneration("gen"));
        long startTime = 1000L;
        status.moveToStarted(startTime, 1, 10, 2L, 20L);
        assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.STARTED));

        long endTime = 1500L;
        status.moveToFailed(endTime, "failed during snapshot");

        assertThat(status.getTotalTimeMillis(), equalTo(500L));
        assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.FAILURE));
    }

    public void testMoveToUnsuccessfulPausedAfterSnapshotStarted_setsTotalTimeMillis() {
        IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(new ShardGeneration("gen"));
        long startTime = 1000L;
        status.moveToStarted(startTime, 1, 10, 2L, 20L);
        assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.STARTED));
        // Go STARTED -> PAUSING (e.g. node removal during snapshot)
        status.pauseIfNotCompleted(listener -> listener.onResponse(null));
        assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.PAUSING));

        long endTime = 1500L;
        status.moveToUnsuccessful(IndexShardSnapshotStatus.Stage.PAUSED, "paused for node removal", endTime);

        assertThat(status.getTotalTimeMillis(), equalTo(500L));
        assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.PAUSED));
    }

    public void testMoveToDone_setsTotalTimeMillisAsBefore() {
        ShardGeneration gen = new ShardGeneration("gen");
        IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(gen);
        long startTime = 1000L;
        status.moveToStarted(startTime, 1, 10, 2L, 20L);
        status.moveToFinalize();
        long endTime = 1500L;
        status.moveToDone(endTime, new ShardSnapshotResult(gen, ByteSizeValue.MINUS_ONE, 2));

        assertThat(status.getTotalTimeMillis(), equalTo(500L));
        assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.DONE));
    }
}
