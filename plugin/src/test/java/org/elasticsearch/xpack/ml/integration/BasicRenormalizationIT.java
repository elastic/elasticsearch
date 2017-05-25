/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * This is a minimal test to ensure renormalization takes place
 */
public class BasicRenormalizationIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void tearDownData() throws Exception {
        cleanUp();
    }

    public void test() throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        long startTime = 1491004800000L;

        Job.Builder job = buildAndRegisterJob("basic-renormalization-it-job", bucketSpan);
        openJob(job.getId());
        postData(job.getId(), generateData(startTime, bucketSpan, 50,
                bucketIndex -> {
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

        List<AnomalyRecord> records = getRecords(job.getId());
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

    private Job.Builder buildAndRegisterJob(String jobId, TimeValue bucketSpan) throws Exception {
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        job.setDataDescription(dataDescription);
        registerJob(job);
        putJob(job);
        return job;
    }

    private static List<String> generateData(long timestamp, TimeValue bucketSpan, int bucketCount,
                                             Function<Integer, Integer> timeToCountFunction) throws IOException {
        List<String> data = new ArrayList<>();
        long now = timestamp;
        for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
            for (int count = 0; count < timeToCountFunction.apply(bucketIndex); count++) {
                Map<String, Object> record = new HashMap<>();
                record.put("time", now);
                data.add(createJsonRecord(record));
            }
            now += bucketSpan.getMillis();
        }
        return data;
    }
}
