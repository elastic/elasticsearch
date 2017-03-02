/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.ml.action.GetJobsStatsAction.TransportAction.determineJobIdsWithoutLiveStats;

public class GetJobsStatsActionTests extends ESTestCase {

    public void testDetermineJobIds() {
        List<String> result = determineJobIdsWithoutLiveStats(Collections.singletonList("id1"), Collections.emptyList());
        assertEquals(1, result.size());
        assertEquals("id1", result.get(0));

        result = determineJobIdsWithoutLiveStats(Collections.singletonList("id1"), Collections.singletonList(
                new GetJobsStatsAction.Response.JobStats("id1", new DataCounts("id1"), null, JobState.CLOSED, null, null, null)));
        assertEquals(0, result.size());

        result = determineJobIdsWithoutLiveStats(
                Arrays.asList("id1", "id2", "id3"), Collections.emptyList());
        assertEquals(3, result.size());
        assertEquals("id1", result.get(0));
        assertEquals("id2", result.get(1));
        assertEquals("id3", result.get(2));

        result = determineJobIdsWithoutLiveStats(
                Arrays.asList("id1", "id2", "id3"),
                Collections.singletonList(new GetJobsStatsAction.Response.JobStats("id1", new DataCounts("id1"), null,
                        JobState.CLOSED, null, null, null))
        );
        assertEquals(2, result.size());
        assertEquals("id2", result.get(0));
        assertEquals("id3", result.get(1));

        result = determineJobIdsWithoutLiveStats(Arrays.asList("id1", "id2", "id3"), Arrays.asList(
                new GetJobsStatsAction.Response.JobStats("id1", new DataCounts("id1"), null, JobState.CLOSED, null, null, null),
                new GetJobsStatsAction.Response.JobStats("id3", new DataCounts("id3"), null, JobState.CLOSED, null, null, null)
        ));
        assertEquals(1, result.size());
        assertEquals("id2", result.get(0));

        result = determineJobIdsWithoutLiveStats(Arrays.asList("id1", "id2", "id3"),
                Arrays.asList(
                        new GetJobsStatsAction.Response.JobStats("id1", new DataCounts("id1"), null, JobState.CLOSED, null, null, null),
                        new GetJobsStatsAction.Response.JobStats("id2", new DataCounts("id2"), null, JobState.CLOSED, null, null, null),
                        new GetJobsStatsAction.Response.JobStats("id3", new DataCounts("id3"), null, JobState.CLOSED, null, null, null)));
        assertEquals(0, result.size());
    }

    public void testDurationToTimeValue() {
        assertNull(GetJobsStatsAction.TransportAction.durationToTimeValue(Optional.empty()));

        Duration duration = Duration.ofSeconds(10L);
        TimeValue timeValue = GetJobsStatsAction.TransportAction.durationToTimeValue(Optional.of(duration));
        assertEquals(10L, timeValue.getSeconds());
    }
}
