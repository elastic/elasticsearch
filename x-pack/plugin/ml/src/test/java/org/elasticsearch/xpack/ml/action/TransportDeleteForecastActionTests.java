/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransportDeleteForecastActionTests extends ESTestCase {

    private static final int TEST_RUNS = 10;

    public void testValidateForecastStateWithAllFailedFinished() {
        for (int i = 0; i < TEST_RUNS; ++i) {
            List<SearchHit> forecastRequestStatsHits = Stream.generate(
                () -> createForecastStatsHit(
                    randomFrom(ForecastRequestStats.ForecastRequestStatus.FAILED, ForecastRequestStats.ForecastRequestStatus.FINISHED)
                )
            ).limit(randomInt(10)).collect(Collectors.toList());

            // This should not throw.
            TransportDeleteForecastAction.extractForecastIds(
                forecastRequestStatsHits.toArray(new SearchHit[0]),
                randomFrom(JobState.values()),
                randomAlphaOfLength(10)
            );
        }
    }

    public void testValidateForecastStateWithSomeFailedFinished() {
        for (int i = 0; i < TEST_RUNS; ++i) {
            List<SearchHit> forecastRequestStatsHits = Stream.generate(
                () -> createForecastStatsHit(randomFrom(ForecastRequestStats.ForecastRequestStatus.values()))
            ).limit(randomInt(10)).collect(Collectors.toList());

            forecastRequestStatsHits.add(createForecastStatsHit(ForecastRequestStats.ForecastRequestStatus.STARTED));

            {
                JobState jobState = randomFrom(JobState.CLOSED, JobState.CLOSING, JobState.FAILED);
                try {
                    TransportDeleteForecastAction.extractForecastIds(
                        forecastRequestStatsHits.toArray(new SearchHit[0]),
                        jobState,
                        randomAlphaOfLength(10)
                    );
                } catch (Exception ex) {
                    fail("Should not have thrown: " + ex.getMessage());
                }
            }
            {
                JobState jobState = JobState.OPENED;
                expectThrows(
                    ElasticsearchStatusException.class,
                    () -> TransportDeleteForecastAction.extractForecastIds(
                        forecastRequestStatsHits.toArray(new SearchHit[0]),
                        jobState,
                        randomAlphaOfLength(10)
                    )
                );
            }
        }
    }

    private static SearchHit createForecastStatsHit(ForecastRequestStats.ForecastRequestStatus status) {
        Map<String, DocumentField> documentFields = Maps.newMapWithExpectedSize(2);
        documentFields.put(
            ForecastRequestStats.FORECAST_ID.getPreferredName(),
            new DocumentField(ForecastRequestStats.FORECAST_ID.getPreferredName(), Collections.singletonList(""))
        );
        documentFields.put(
            ForecastRequestStats.STATUS.getPreferredName(),
            new DocumentField(ForecastRequestStats.STATUS.getPreferredName(), Collections.singletonList(status.toString()))
        );
        return new SearchHit(0, "", documentFields, Collections.emptyMap());
    }
}
