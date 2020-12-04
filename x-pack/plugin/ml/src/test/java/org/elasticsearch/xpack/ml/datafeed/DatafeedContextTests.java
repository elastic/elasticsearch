/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.persistence.RestartTimeInfo;

import java.util.Date;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatafeedContextTests extends ESTestCase {

    public void testShouldRecoverFromCurrentSnapshot_GivenDatafeedStartTimeIsAfterJobCheckpoint() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(20L, 20L, true);
        ModelSnapshot modelSnapshot = newSnapshot(10L, 10L);

        DatafeedContext context = createContext(20L, restartTimeInfo, modelSnapshot);

        assertThat(context.shouldRecoverFromCurrentSnapshot(), is(false));
    }

    public void testShouldRecoverFromCurrentSnapshot_GivenRestartTimeInfoIsAfterNonNullSnapshot() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(20L, 20L, true);
        ModelSnapshot modelSnapshot = newSnapshot(10L, 10L);

        DatafeedContext context = createContext(10L, restartTimeInfo, modelSnapshot);

        assertThat(context.shouldRecoverFromCurrentSnapshot(), is(true));
    }

    public void testShouldRecoverFromCurrentSnapshot_GivenHaveSeenDataBeforeAndNullSnapshot() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(20L, 20L, true);

        DatafeedContext context = createContext(10L, restartTimeInfo, null);

        assertThat(context.shouldRecoverFromCurrentSnapshot(), is(true));
    }

    public void testShouldRecoverFromCurrentSnapshot_GivenHaveNotSeenDataBeforeAndNullSnapshot() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(null, null, false);

        DatafeedContext context = createContext(10L, restartTimeInfo, null);

        assertThat(context.shouldRecoverFromCurrentSnapshot(), is(false));
    }

    public void testShouldRecoverFromCurrentSnapshot_GivenRestartTimeInfoMatchesSnapshot() {
        RestartTimeInfo restartTimeInfo = new RestartTimeInfo(20L, 20L, true);
        ModelSnapshot modelSnapshot = newSnapshot(20L, 20L);

        DatafeedContext context = createContext(10L, restartTimeInfo, modelSnapshot);

        assertThat(context.shouldRecoverFromCurrentSnapshot(), is(false));
    }

    private static DatafeedContext createContext(long datafeedStartTime, RestartTimeInfo restartTimeInfo,
                                                 @Nullable ModelSnapshot modelSnapshot) {
        Job job = JobTests.createRandomizedJob();
        return DatafeedContext.builder(datafeedStartTime)
            .setJob(job)
            .setDatafeedConfig(DatafeedConfigTests.createRandomizedDatafeedConfig(job.getId()))
            .setTimingStats(new DatafeedTimingStats(job.getId()))
            .setRestartTimeInfo(restartTimeInfo)
            .setModelSnapshot(modelSnapshot)
            .build();
    }

    private static ModelSnapshot newSnapshot(@Nullable Long latestResultTime, @Nullable Long latestRecordTime) {
        ModelSnapshot snapshot = mock(ModelSnapshot.class);
        when(snapshot.getLatestResultTimeStamp()).thenReturn(latestResultTime == null ? null : new Date(latestResultTime));
        when(snapshot.getLatestRecordTimeStamp()).thenReturn(latestRecordTime == null ? null : new Date(latestRecordTime));
        return snapshot;
    }
}
