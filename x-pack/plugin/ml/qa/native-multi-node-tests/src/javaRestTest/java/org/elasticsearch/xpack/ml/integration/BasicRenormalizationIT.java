/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.junit.After;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * This is a minimal test to ensure renormalization takes place
 */
public class BasicRenormalizationIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void tearDownData() {
        cleanUp();
    }

    public void testDefaultRenormalization() throws Exception {

        String jobId = "basic-renormalization-it-test-default-renormalization-job";
        createAndRunJob(jobId, null);

        GetRecordsAction.Request getRecordsRequest = new GetRecordsAction.Request(jobId);
        // Setting the record score to 10.0, to avoid the low score records due to multibucket trailing effect
        getRecordsRequest.setRecordScore(10.0);

        List<AnomalyRecord> records = getRecords(getRecordsRequest);
        assertThat(records.size(), equalTo(2));
        AnomalyRecord laterRecord = records.get(0);
        assertThat(laterRecord.getActual().get(0), equalTo(100.0));
        AnomalyRecord earlierRecord = records.get(1);
        assertThat(earlierRecord.getActual().get(0), equalTo(10.0));
        assertThat(laterRecord.getRecordScore(), greaterThan(earlierRecord.getRecordScore()));

        // This is the key assertion: if renormalization never happened then the record_score would
        // be the same as the initial_record_score on the anomaly record that happened earlier
        assertThat(earlierRecord.getInitialRecordScore(), greaterThan(earlierRecord.getRecordScore()));
    }

    public void testRenormalizationDisabled() throws Exception {
        String jobId = "basic-renormalization-it-test-renormalization-disabled-job";
        createAndRunJob(jobId, 0L);

        List<AnomalyRecord> records = getRecords(jobId);
        for (AnomalyRecord record : records) {
            assertThat(record.getInitialRecordScore(), equalTo(record.getRecordScore()));
        }
    }

    private void createAndRunJob(String jobId, Long renormalizationWindow) throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        long startTime = 1491004800000L;

        Job.Builder job = buildAndRegisterJob(jobId, bucketSpan, renormalizationWindow);
        openJob(job.getId());
        postData(job.getId(), generateData(startTime, bucketSpan, 50, bucketIndex -> {
            if (bucketIndex == 35) {
                // First anomaly is 10 events
                return 10;
            } else if (bucketIndex == 45) {
                // Second anomaly is 100, should get the highest score and should bring the first score down
                return 100;
            } else {
                return 1;
            }
        }).stream().collect(Collectors.joining()));
        closeJob(job.getId());
    }

    private Job.Builder buildAndRegisterJob(String jobId, TimeValue bucketSpan, Long renormalizationWindow) {
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        job.setDataDescription(dataDescription);
        if (renormalizationWindow != null) {
            job.setRenormalizationWindowDays(renormalizationWindow);
        }
        putJob(job);
        return job;
    }
}
