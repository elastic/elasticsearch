/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TransportDeleteForecastActionTests extends ESTestCase {

    private static final int TEST_RUNS = 10;

    public void testValidateForecastStateWithAllFailedFinished() {
        for (int i = 0; i < TEST_RUNS; ++i) {
            List<ForecastRequestStats> forecastRequestStats = Stream.generate(
                () -> createForecastStats(randomFrom(
                    ForecastRequestStats.ForecastRequestStatus.FAILED,
                    ForecastRequestStats.ForecastRequestStatus.FINISHED
                )))
                .limit(randomInt(10))
                .collect(Collectors.toList());

            // This should not throw.
            TransportDeleteForecastAction.validateForecastState(
                forecastRequestStats,
                randomFrom(JobState.values()),
                randomAlphaOfLength(10));
        }
    }

    public void testValidateForecastStateWithSomeFailedFinished() {
        for (int i = 0; i < TEST_RUNS; ++i) {
            List<ForecastRequestStats> forecastRequestStats = Stream.generate(
                () -> createForecastStats(randomFrom(
                    ForecastRequestStats.ForecastRequestStatus.values()
                )))
                .limit(randomInt(10))
                .collect(Collectors.toList());

            forecastRequestStats.add(createForecastStats(ForecastRequestStats.ForecastRequestStatus.STARTED));

            {
                JobState jobState = randomFrom(JobState.CLOSED, JobState.CLOSING, JobState.FAILED);
                try {
                    TransportDeleteForecastAction.validateForecastState(forecastRequestStats, jobState, randomAlphaOfLength(10));
                } catch (Exception ex) {
                    fail("Should not have thrown: " + ex.getMessage());
                }
            }
            {
                JobState jobState = JobState.OPENED;
                expectThrows(
                    ElasticsearchStatusException.class,
                    () -> TransportDeleteForecastAction.validateForecastState(forecastRequestStats, jobState, randomAlphaOfLength(10))
                );
            }
        }
    }


    private static ForecastRequestStats createForecastStats(ForecastRequestStats.ForecastRequestStatus status) {
        ForecastRequestStats forecastRequestStats = new ForecastRequestStats(randomAlphaOfLength(10), randomAlphaOfLength(10));
        forecastRequestStats.setStatus(status);
        return forecastRequestStats;
    }

}
