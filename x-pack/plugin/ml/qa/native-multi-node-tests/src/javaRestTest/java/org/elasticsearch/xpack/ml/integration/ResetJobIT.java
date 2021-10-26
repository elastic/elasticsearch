/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Blocked;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.junit.After;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ResetJobIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void tearDownData() {
        cleanUp();
    }

    public void testReset() throws IOException {
        TimeValue bucketSpan = TimeValue.timeValueMinutes(30);
        long startTime = 1514764800000L;
        final int bucketCount = 100;
        Job.Builder job = createJob("test-reset", bucketSpan);

        openJob(job.getId());
        postData(job.getId(), generateData(startTime, bucketSpan, bucketCount + 1, bucketIndex -> randomIntBetween(100, 200))
            .stream().collect(Collectors.joining()));
        closeJob(job.getId());

        List<Bucket> buckets = getBuckets(job.getId());
        assertThat(buckets.isEmpty(), is(false));

        DataCounts dataCounts = getJobStats(job.getId()).get(0).getDataCounts();
        assertThat(dataCounts.getProcessedRecordCount(), greaterThan(0L));

        resetJob(job.getId());

        buckets = getBuckets(job.getId());
        assertThat(buckets.isEmpty(), is(true));

        dataCounts = getJobStats(job.getId()).get(0).getDataCounts();
        assertThat(dataCounts.getProcessedRecordCount(), equalTo(0L));

        Job jobAfterReset = getJob(job.getId()).get(0);
        assertThat(jobAfterReset.getBlocked(), equalTo(Blocked.none()));
        assertThat(jobAfterReset.getModelSnapshotId(), is(nullValue()));
        assertThat(jobAfterReset.getFinishedTime(), is(nullValue()));

        List<String> auditMessages = fetchAllAuditMessages(job.getId());
        assertThat(auditMessages.isEmpty(), is(false));
        assertThat(auditMessages.get(auditMessages.size() - 1), equalTo("Job has been reset"));
    }

    private Job.Builder createJob(String jobId, TimeValue bucketSpan) {
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        job.setDataDescription(dataDescription);
        putJob(job);

        return job;
    }
}
