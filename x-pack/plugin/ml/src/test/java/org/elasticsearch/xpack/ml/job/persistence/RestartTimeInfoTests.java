/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;

import java.util.Date;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestartTimeInfoTests extends ESTestCase {

    public void testIsAfter_GivenNullsAndTimestampIsZero() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(null, null, false);
        assertThat(restartTimeInfo.isAfter(0), is(false));
    }

    public void testIsAfter_GivenNullsAndTimestampIsNonZero() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(null, null, false);
        assertThat(restartTimeInfo.isAfter(1L), is(false));
    }

    public void testIsAfter_GivenTimestampIsBeforeFinalBucketButAfterLatestRecord() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(10L, 20L, true);
        assertThat(restartTimeInfo.isAfter(15L), is(true));
    }

    public void testIsAfter_GivenTimestampIsAfterFinalBucketButBeforeLatestRecord() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(20L, 10L, true);
        assertThat(restartTimeInfo.isAfter(15L), is(true));
    }

    public void testIsAfter_GivenTimestampIsAfterFinalBucketAndAfterLatestRecord() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(20L, 10L, true);
        assertThat(restartTimeInfo.isAfter(30L), is(false));
    }

    public void testIsAfter_GivenTimestampIsBeforeFinalBucketAndBeforeLatestRecord() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(20L, 10L, true);
        assertThat(restartTimeInfo.isAfter(5L), is(true));
    }

    public void testIsAfterModelSnapshot_GivenNulls() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(null, null, false);
        ModelSnapshot snapshot = newSnapshot(null, null);
        assertThat(restartTimeInfo.isAfterModelSnapshot(snapshot), is(false));
    }

    public void testIsAfterModelSnapshot_GivenModelSnapshotLatestRecordTimeIsBefore() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(20L, 30L, true);
        ModelSnapshot snapshot = newSnapshot(40L, 25L);
        assertThat(restartTimeInfo.isAfterModelSnapshot(snapshot), is(true));
    }

    public void testIsAfterModelSnapshot_GivenModelSnapshotLatestResultTimeIsBefore() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(20L, 30L, true);
        ModelSnapshot snapshot = newSnapshot(15L, 35L);
        assertThat(restartTimeInfo.isAfterModelSnapshot(snapshot), is(true));
   }

    public void testIsAfterModelSnapshot_GivenModelSnapshotIsAfter() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(20L, 30L, true);
        ModelSnapshot snapshot = newSnapshot(30L, 35L);
        assertThat(restartTimeInfo.isAfterModelSnapshot(snapshot), is(false));
    }

    public void testIsAfterModelSnapshot_GivenModelSnapshotMatches() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(20L, 30L, true);
        ModelSnapshot snapshot = newSnapshot(20L, 30L);
        assertThat(restartTimeInfo.isAfterModelSnapshot(snapshot), is(false));
    }

    private static ModelSnapshot newSnapshot(@Nullable Long latestResultTime, @Nullable Long latestRecordTime) {
        ModelSnapshot snapshot = mock(ModelSnapshot.class);
        when(snapshot.getLatestResultTimeStamp()).thenReturn(latestResultTime == null ? null : new Date(latestResultTime));
        when(snapshot.getLatestRecordTimeStamp()).thenReturn(latestRecordTime == null ? null : new Date(latestRecordTime));
        return snapshot;
    }
}
