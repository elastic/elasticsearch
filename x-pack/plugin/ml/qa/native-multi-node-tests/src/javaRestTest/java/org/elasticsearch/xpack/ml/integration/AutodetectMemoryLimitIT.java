/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * A set of tests that ensure we comply to the model memory limit
 */
public class AutodetectMemoryLimitIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanUpTest() {
        cleanUp();
    }

    public void testTooManyPartitions() throws Exception {
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setPartitionFieldName("user");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("autodetect-memory-limit-test-too-many-partitions");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        // Set the memory limit to 30MB
        AnalysisLimits limits = new AnalysisLimits(30L, null);
        job.setAnalysisLimits(limits);

        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 8 * bucketSpan.seconds();
        List<String> data = new ArrayList<>();
        while (timestamp < now) {
            for (int i = 0; i < 11000; i++) {
                // It's important that the values used here are either always represented in less than 16 UTF-8 bytes or
                // always represented in more than 22 UTF-8 bytes. Otherwise platform differences in when the small string
                // optimisation is used will make the results of this test very different for the different platforms.
                data.add(createJsonRecord(createRecord(timestamp, String.valueOf(i), "")));
            }
            timestamp += bucketSpan.seconds();
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        closeJob(job.getId());

        // Assert we haven't violated the limit too much
        GetJobsStatsAction.Response.JobStats jobStats = getJobStats(job.getId()).get(0);
        ModelSizeStats modelSizeStats = jobStats.getModelSizeStats();
        assertThat(modelSizeStats.getModelBytes(), lessThan(50200000L));
        assertThat(modelSizeStats.getModelBytes(), greaterThan(24000000L));
        assertThat(
            modelSizeStats.getMemoryStatus(),
            anyOf(equalTo(ModelSizeStats.MemoryStatus.SOFT_LIMIT), equalTo(ModelSizeStats.MemoryStatus.HARD_LIMIT))
        );
    }

    public void testTooManyByFields() throws Exception {
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setByFieldName("user");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("autodetect-memory-limit-test-too-many-by-fields");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        // Set the memory limit to 30MB
        AnalysisLimits limits = new AnalysisLimits(30L, null);
        job.setAnalysisLimits(limits);

        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 8 * bucketSpan.seconds();
        List<String> data = new ArrayList<>();
        while (timestamp < now) {
            for (int i = 0; i < 10000; i++) {
                // It's important that the values used here are either always represented in less than 16 UTF-8 bytes or
                // always represented in more than 22 UTF-8 bytes. Otherwise platform differences in when the small string
                // optimisation is used will make the results of this test very different for the different platforms.
                data.add(createJsonRecord(createRecord(timestamp, String.valueOf(i), "")));
            }
            timestamp += bucketSpan.seconds();
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        closeJob(job.getId());

        // Assert we haven't violated the limit too much
        GetJobsStatsAction.Response.JobStats jobStats = getJobStats(job.getId()).get(0);
        ModelSizeStats modelSizeStats = jobStats.getModelSizeStats();
        assertThat(modelSizeStats.getModelBytes(), lessThan(45000000L));
        assertThat(modelSizeStats.getModelBytes(), greaterThan(25000000L));
        assertThat(modelSizeStats.getMemoryStatus(), equalTo(ModelSizeStats.MemoryStatus.HARD_LIMIT));
    }

    public void testTooManyByAndOverFields() throws Exception {
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setByFieldName("department");
        detector.setOverFieldName("user");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("autodetect-memory-limit-test-too-many-by-and-over-fields");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        // Set the memory limit to 30MB
        AnalysisLimits limits = new AnalysisLimits(30L, null);
        job.setAnalysisLimits(limits);

        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 8 * bucketSpan.seconds();
        while (timestamp < now) {
            for (int department = 0; department < 10; department++) {
                List<String> data = new ArrayList<>();
                for (int user = 0; user < 10000; user++) {
                    // It's important that the values used here are either always represented in less than 16 UTF-8 bytes or
                    // always represented in more than 22 UTF-8 bytes. Otherwise platform differences in when the small string
                    // optimisation is used will make the results of this test very different for the different platforms.
                    data.add(
                        createJsonRecord(
                            createRecord(timestamp, String.valueOf(department) + "_" + String.valueOf(user), String.valueOf(department))
                        )
                    );
                }
                postData(job.getId(), data.stream().collect(Collectors.joining()));
            }
            timestamp += bucketSpan.seconds();
        }

        closeJob(job.getId());

        // Assert we haven't violated the limit too much
        GetJobsStatsAction.Response.JobStats jobStats = getJobStats(job.getId()).get(0);
        ModelSizeStats modelSizeStats = jobStats.getModelSizeStats();
        assertThat(modelSizeStats.getModelBytes(), lessThan(71000000L));
        assertThat(modelSizeStats.getModelBytes(), greaterThan(24000000L));
        assertThat(modelSizeStats.getMemoryStatus(), equalTo(ModelSizeStats.MemoryStatus.HARD_LIMIT));
    }

    public void testManyDistinctOverFields() throws Exception {
        Detector.Builder detector = new Detector.Builder("sum", "value");
        detector.setOverFieldName("user");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("autodetect-memory-limit-test-too-many-distinct-over-fields");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        // Set the memory limit to 110MB
        AnalysisLimits limits = new AnalysisLimits(110L, null);
        job.setAnalysisLimits(limits);

        putJob(job);
        openJob(job.getId());

        long now = Instant.now().getEpochSecond();
        long timestamp = now - 15 * bucketSpan.seconds();
        int user = 0;
        while (timestamp < now) {
            List<String> data = new ArrayList<>();
            for (int i = 0; i < 20000; i++) {
                // It's important that the values used here are either always represented in less than 16 UTF-8 bytes or
                // always represented in more than 22 UTF-8 bytes. Otherwise platform differences in when the small string
                // optimisation is used will make the results of this test very different for the different platforms.
                Map<String, Object> record = new HashMap<>();
                record.put("time", timestamp);
                record.put("user", user++);
                record.put("value", 42.0);
                data.add(createJsonRecord(record));
            }
            postData(job.getId(), data.stream().collect(Collectors.joining()));
            timestamp += bucketSpan.seconds();
        }

        closeJob(job.getId());

        // Assert we haven't violated the limit too much
        GetJobsStatsAction.Response.JobStats jobStats = getJobStats(job.getId()).get(0);
        ModelSizeStats modelSizeStats = jobStats.getModelSizeStats();
        assertThat(modelSizeStats.getModelBytes(), lessThan(120500000L));
        assertThat(modelSizeStats.getModelBytes(), greaterThan(70000000L));
        assertThat(modelSizeStats.getMemoryStatus(), equalTo(ModelSizeStats.MemoryStatus.HARD_LIMIT));
    }

    public void testOpenJobShouldHaveModelSizeStats() throws Exception {
        // When a job is opened, it should have non-zero model stats that indicate the memory limit and the assignment basis
        Detector.Builder detector = new Detector.Builder("sum", "value");
        detector.setOverFieldName("user");

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder("autodetect-open-job-should-have-model-size-stats");
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        // Set the memory limit to 110MB
        AnalysisLimits limits = new AnalysisLimits(110L, null);
        job.setAnalysisLimits(limits);

        putJob(job);
        openJob(job.getId());
        GetJobsStatsAction.Response.JobStats jobStats = getJobStats(job.getId()).get(0);
        ModelSizeStats modelSizeStats = jobStats.getModelSizeStats();
        closeJob(job.getId());

        assertThat(modelSizeStats.getModelBytes(), equalTo(0L));
        assertThat(modelSizeStats.getModelBytesMemoryLimit(), equalTo(110L));
        assertThat(modelSizeStats.getMemoryStatus(), equalTo(ModelSizeStats.MemoryStatus.OK));
        assertThat(modelSizeStats.getAssignmentMemoryBasis(), equalTo(ModelSizeStats.AssignmentMemoryBasis.MODEL_MEMORY_LIMIT));

    }

    private static Map<String, Object> createRecord(long timestamp, String user, String department) {
        Map<String, Object> record = new HashMap<>();
        record.put("time", timestamp);
        record.put("user", user);
        record.put("department", department);
        return record;
    }
}
