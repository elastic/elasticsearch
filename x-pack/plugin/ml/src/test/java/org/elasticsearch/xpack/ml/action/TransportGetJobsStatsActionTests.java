/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.ml.action.TransportGetJobsStatsAction.determineJobIdsWithoutLiveStats;

public class TransportGetJobsStatsActionTests extends ESTestCase {

    public void testDetermineJobIds() {

        List<String> result = determineJobIdsWithoutLiveStats(Collections.singletonList("id1"), Collections.emptyList());
        assertEquals(1, result.size());
        assertEquals("id1", result.get(0));

        result = determineJobIdsWithoutLiveStats(
            Collections.singletonList("id1"),
            Collections.singletonList(
                new GetJobsStatsAction.Response.JobStats(
                    "id1", new DataCounts("id1"), null, null, JobState.OPENED, null, null, null, new TimingStats("id1"))));
        assertEquals(0, result.size());

        result = determineJobIdsWithoutLiveStats(Arrays.asList("id1", "id2", "id3"), Collections.emptyList());
        assertEquals(3, result.size());
        assertEquals("id1", result.get(0));
        assertEquals("id2", result.get(1));
        assertEquals("id3", result.get(2));

        result = determineJobIdsWithoutLiveStats(Arrays.asList("id1", "id2", "id3"),
                Collections.singletonList(new GetJobsStatsAction.Response.JobStats("id1", new DataCounts("id1"), null, null,
                        JobState.OPENED, null, null, null, new TimingStats("id1")))
        );
        assertEquals(2, result.size());
        assertEquals("id2", result.get(0));
        assertEquals("id3", result.get(1));

        result = determineJobIdsWithoutLiveStats(Arrays.asList("id1", "id2", "id3"), Arrays.asList(
                new GetJobsStatsAction.Response.JobStats(
                    "id1", new DataCounts("id1"), null, null, JobState.OPENED, null, null, null, new TimingStats("id1")),
                new GetJobsStatsAction.Response.JobStats(
                    "id3", new DataCounts("id3"), null, null, JobState.OPENED, null, null, null, new TimingStats("id3"))
        ));
        assertEquals(1, result.size());
        assertEquals("id2", result.get(0));

        result = determineJobIdsWithoutLiveStats(Arrays.asList("id1", "id2", "id3"), Arrays.asList(
                new GetJobsStatsAction.Response.JobStats(
                    "id1", new DataCounts("id1"), null, null, JobState.OPENED, null, null, null, new TimingStats("id1")),
                new GetJobsStatsAction.Response.JobStats(
                    "id2", new DataCounts("id2"), null, null, JobState.OPENED, null, null, null, new TimingStats("id2")),
                new GetJobsStatsAction.Response.JobStats(
                    "id3", new DataCounts("id3"), null, null, JobState.OPENED, null, null, null, new TimingStats("id3"))));
        assertEquals(0, result.size());
    }

    public void testDurationToTimeValue() {
        assertNull(TransportGetJobsStatsAction.durationToTimeValue(Optional.empty()));

        Duration duration = Duration.ofSeconds(10L);
        TimeValue timeValue = TransportGetJobsStatsAction.durationToTimeValue(Optional.of(duration));
        assertEquals(10L, timeValue.getSeconds());
    }
}
