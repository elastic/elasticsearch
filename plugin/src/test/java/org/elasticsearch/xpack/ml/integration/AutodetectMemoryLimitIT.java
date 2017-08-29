/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * A set of tests that ensure we comply to the model memory limit
 */
public class AutodetectMemoryLimitIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanUpTest() throws Exception {
        cleanUp();
    }

    public void testTooManyPartitions() throws Exception {
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setPartitionFieldName("user");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
                Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("autodetect-memory-limit-test-too-many-partitions");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        // Set the memory limit to 30MB
        AnalysisLimits limits = new AnalysisLimits(30L, null);
        job.setAnalysisLimits(limits);

        registerJob(job);
        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 8 * bucketSpan.seconds();
        List<String> data = new ArrayList<>();
        while (timestamp < now) {
            for (int i = 0; i < 10000; i++) {
                data.add(createJsonRecord(createRecord(timestamp, String.valueOf(i), "")));
            }
            timestamp += bucketSpan.seconds();
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        closeJob(job.getId());

        // Assert we haven't violated the limit too much
        // and a balance of partitions/by fields were created
        GetJobsStatsAction.Response.JobStats jobStats = getJobStats(job.getId()).get(0);
        ModelSizeStats modelSizeStats = jobStats.getModelSizeStats();
        assertThat(modelSizeStats.getMemoryStatus(), equalTo(ModelSizeStats.MemoryStatus.HARD_LIMIT));
        assertThat(modelSizeStats.getModelBytes(), lessThan(35000000L));
        assertThat(modelSizeStats.getModelBytes(), greaterThan(30000000L));

        // it is important to check that while we rejected partitions, we still managed
        // to create some by fields; it shows we utilize memory in a meaningful way
        // rather than creating empty partitions
        assertThat(modelSizeStats.getTotalPartitionFieldCount(), lessThan(700L));
        assertThat(modelSizeStats.getTotalPartitionFieldCount(), greaterThan(600L));
        assertThat(modelSizeStats.getTotalByFieldCount(), lessThan(700L));
        assertThat(modelSizeStats.getTotalByFieldCount(), greaterThan(600L));
    }

    public void testTooManyByFields() throws Exception {
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setByFieldName("user");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
                Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("autodetect-memory-limit-test-too-many-by-fields");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        // Set the memory limit to 30MB
        AnalysisLimits limits = new AnalysisLimits(30L, null);
        job.setAnalysisLimits(limits);

        registerJob(job);
        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 8 * bucketSpan.seconds();
        List<String> data = new ArrayList<>();
        while (timestamp < now) {
            for (int i = 0; i < 10000; i++) {
                data.add(createJsonRecord(createRecord(timestamp, String.valueOf(i), "")));
            }
            timestamp += bucketSpan.seconds();
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        closeJob(job.getId());

        // Assert we haven't violated the limit too much
        GetJobsStatsAction.Response.JobStats jobStats = getJobStats(job.getId()).get(0);
        ModelSizeStats modelSizeStats = jobStats.getModelSizeStats();
        assertThat(modelSizeStats.getMemoryStatus(), equalTo(ModelSizeStats.MemoryStatus.HARD_LIMIT));
        assertThat(modelSizeStats.getModelBytes(), lessThan(36000000L));
        assertThat(modelSizeStats.getModelBytes(), greaterThan(30000000L));
        assertThat(modelSizeStats.getTotalByFieldCount(), lessThan(1600L));
        assertThat(modelSizeStats.getTotalByFieldCount(), greaterThan(1500L));
    }

    public void testTooManyByAndOverFields() throws Exception {
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setByFieldName("department");
        detector.setOverFieldName("user");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
                Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("autodetect-memory-limit-test-too-many-by-and-over-fields");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        // Set the memory limit to 30MB
        AnalysisLimits limits = new AnalysisLimits(30L, null);
        job.setAnalysisLimits(limits);

        registerJob(job);
        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 8 * bucketSpan.seconds();
        while (timestamp < now) {
            for (int department = 0; department < 10; department++) {
                List<String> data = new ArrayList<>();
                for (int user = 0; user < 10000; user++) {
                    data.add(createJsonRecord(createRecord(
                            timestamp, String.valueOf(department) + "_" + String.valueOf(user), String.valueOf(department))));
                }
                postData(job.getId(), data.stream().collect(Collectors.joining()));
            }
            timestamp += bucketSpan.seconds();
        }

        closeJob(job.getId());

        // Assert we haven't violated the limit too much
        GetJobsStatsAction.Response.JobStats jobStats = getJobStats(job.getId()).get(0);
        ModelSizeStats modelSizeStats = jobStats.getModelSizeStats();
        assertThat(modelSizeStats.getMemoryStatus(), equalTo(ModelSizeStats.MemoryStatus.HARD_LIMIT));
        assertThat(modelSizeStats.getModelBytes(), lessThan(36000000L));
        assertThat(modelSizeStats.getModelBytes(), greaterThan(24000000L));
        assertThat(modelSizeStats.getTotalByFieldCount(), equalTo(8L));
        assertThat(modelSizeStats.getTotalOverFieldCount(), greaterThan(50000L));
        assertThat(modelSizeStats.getTotalOverFieldCount(), lessThan(60000L));
    }

    private static Map<String, Object> createRecord(long timestamp, String user, String department) {
        Map<String, Object> record = new HashMap<>();
        record.put("time", timestamp);
        record.put("user", user);
        record.put("department", department);
        return record;
    }
}
