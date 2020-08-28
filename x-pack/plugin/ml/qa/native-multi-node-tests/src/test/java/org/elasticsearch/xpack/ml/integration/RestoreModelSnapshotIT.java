/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

/**
 * This test aims to catch regressions where,
 * when a job is reopened, it does not get restored
 * with its model snapshot. To achieve this we
 * leverage the forecast API. Requesting a forecast
 * when there's no model state results to an error.
 * Thus, we create a job, send some data, and we close it.
 * Then we open it again and we request a forecast asserting
 * the forecast was successful.
 */
public class RestoreModelSnapshotIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void tearDownData() {
        cleanUp();
    }

    public void test() throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        int bucketCount = 72;

        List<String> data = new ArrayList<>();
        long now = System.currentTimeMillis();
        long timestamp = now - bucketCount * bucketSpan.getMillis();
        for (int i = 0; i < bucketCount; i++) {
            Map<String, Object> record = new HashMap<>();
            record.put("time", timestamp);
            data.add(createJsonRecord(record));
            timestamp += bucketSpan.getMillis();
        }

        // Create the job, post the data and close the job
        Job.Builder job = buildAndRegisterJob("restore-model-snapshot-job", bucketSpan);
        openJob(job.getId());
        // Forecast should fail when the model has seen no data, ie model state not initialized
        expectThrows(ElasticsearchStatusException.class, () -> forecast(job.getId(), TimeValue.timeValueHours(3), null));
        postData(job.getId(), data.stream().collect(Collectors.joining()));
        closeJob(job.getId());

        // Reopen the job and check forecast works
        openJob(job.getId());
        String forecastId = forecast(job.getId(), TimeValue.timeValueHours(3), null);
        waitForecastToFinish(job.getId(), forecastId);
        // In a multi-node cluster the replica may not be up to date
        // so wait for the change
        assertBusy(() -> {
            ForecastRequestStats forecastStats = getForecastStats(job.getId(), forecastId);
            assertThat(forecastStats.getMessages(), anyOf(nullValue(), empty()));
            assertThat(forecastStats.getMemoryUsage(), greaterThan(0L));
            assertThat(forecastStats.getRecordCount(), equalTo(3L));
        });

        closeJob(job.getId());
    }

    private Job.Builder buildAndRegisterJob(String jobId, TimeValue bucketSpan) throws Exception {
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        job.setDataDescription(dataDescription);
        registerJob(job);
        putJob(job);
        return job;
    }
}
