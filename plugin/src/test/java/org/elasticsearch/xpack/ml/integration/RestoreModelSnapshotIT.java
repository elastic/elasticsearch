/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

/**
 * This test generates data and it runs them through 2 jobs.
 * The data covers 3 days. During the first 2 days, bucket values alternate
 * between 2 modes. During the 3rd day, there is only one of the 2 modes
 * except for a single bucket when the other mode reappears.
 * * The first job receives the data in one go. The second job receives the
 * data split into 2 parts: the alternating part and the stable part.
 * After the first half, the job is closed and reopened, forcing the model
 * snapshot to be restored.
 *
 * The test is designed so that no anomalies should be detected. However,
 * for the split job, if the model fails to be restored the reappearance of
 * the lost mode should cause an anomaly.
 *
 * The test asserts the 2 jobs have equal data counts and no records.
 */
public class RestoreModelSnapshotIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void tearDownData() throws Exception {
        cleanUp();
    }

    public void test() throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        int bucketCount = 72;
        List<String> byFieldValues = Arrays.asList("foo", "bar");

        List<String> data = new ArrayList<>();
        long now = System.currentTimeMillis();
        long timestamp = now - bucketCount * bucketSpan.getMillis();
        boolean isMode1 = true;
        int alternatingModesCutoff = 48;
        int differentModeBucketAfterAlternatingCutoff = 60;
        for (int i = 0; i < bucketCount; i++) {
            for (String byFieldValue : byFieldValues) {
                double value = isMode1 ? 10.0 : 100.0;
                if (i == differentModeBucketAfterAlternatingCutoff) {
                    value = isMode1 ? 100.0 : 10.0;
                }

                Map<String, Object> record = new HashMap<>();
                record.put("time", timestamp);
                record.put("value", value);
                record.put("by_field", byFieldValue);
                data.add(createJsonRecord(record));
            }
            timestamp += bucketSpan.getMillis();
            if (i < alternatingModesCutoff) {
                isMode1 = !isMode1;
            }
        }

        Job.Builder oneGoJob = buildAndRegisterJob("restore-model-snapshot-one-go-job", bucketSpan);
        openJob(oneGoJob.getId());
        postData(oneGoJob.getId(), joinBetween(0, data.size(), data));
        closeJob(oneGoJob.getId());

        Job.Builder splitJob = buildAndRegisterJob("restore-model-snapshot-split-job", bucketSpan);
        openJob(splitJob.getId());
        int splitPoint = alternatingModesCutoff * byFieldValues.size();
        postData(splitJob.getId(), joinBetween(0, splitPoint, data));
        closeJob(splitJob.getId());

        openJob(splitJob.getId());
        postData(splitJob.getId(), joinBetween(splitPoint, data.size(), data));
        closeJob(splitJob.getId());

        // Compare data counts
        GetJobsStatsAction.Response.JobStats oneGoJobStats = getJobStats(oneGoJob.getId()).get(0);
        GetJobsStatsAction.Response.JobStats splitJobStats = getJobStats(splitJob.getId()).get(0);
        assertThat(oneGoJobStats.getDataCounts().getProcessedRecordCount(),
                equalTo(splitJobStats.getDataCounts().getProcessedRecordCount()));
        assertThat(oneGoJobStats.getDataCounts().getLatestRecordTimeStamp(),
                equalTo(splitJobStats.getDataCounts().getLatestRecordTimeStamp()));

        List<Bucket> oneGoBuckets = getBuckets(oneGoJob.getId());
        assertThat(oneGoBuckets.size(), greaterThanOrEqualTo(70));
        assertThat(getBuckets(splitJob.getId()).size(), equalTo(oneGoBuckets.size()));
        assertThat(getRecords(oneGoJob.getId()).isEmpty(), is(true));
        assertThat(getRecords(splitJob.getId()).isEmpty(), is(true));

        // Since these jobs ran for 72 buckets, it's a good place to assert
        // that established model memory matches model memory in the job stats
        for (Job.Builder job : Arrays.asList(oneGoJob, splitJob)) {
            GetJobsStatsAction.Response.JobStats jobStats = getJobStats(job.getId()).get(0);
            ModelSizeStats modelSizeStats = jobStats.getModelSizeStats();
            Job updatedJob = getJob(job.getId()).get(0);
            assertThat(updatedJob.getEstablishedModelMemory(), equalTo(modelSizeStats.getModelBytes()));
        }
    }

    private Job.Builder buildAndRegisterJob(String jobId, TimeValue bucketSpan) throws Exception {
        Detector.Builder detector = new Detector.Builder("mean", "value");
        detector.setByFieldName("by_field");
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

    private String joinBetween(int start, int end, List<String> input) {
        StringBuilder result = new StringBuilder();
        for (int i = start; i < end; i++) {
            result.append(input.get(i));
        }
        return result.toString();
    }
}
