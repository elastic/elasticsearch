/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that after reopening a job and sending more
 * data after a gap, data counts are reported correctly.
 */
public class ReopenJobWithGapIT extends MlNativeAutodetectIntegTestCase {

    private static final String JOB_ID = "reopen-job-with-gap-test";
    private static final long BUCKET_SPAN_SECONDS = 3600;

    @After
    public void cleanUpTest() {
        cleanUp();
    }

    public void test() throws Exception {
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
            Collections.singletonList(new Detector.Builder("count", null).build())
        );
        analysisConfig.setBucketSpan(TimeValue.timeValueSeconds(BUCKET_SPAN_SECONDS));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder(JOB_ID);
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());

        long timestamp = 1483228800L; // 2017-01-01T00:00:00Z
        List<String> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(createJsonRecord(createRecord(timestamp)));
            timestamp += BUCKET_SPAN_SECONDS;
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        flushJob(job.getId(), true);
        closeJob(job.getId());

        GetBucketsAction.Request request = new GetBucketsAction.Request(job.getId());
        request.setExcludeInterim(true);
        assertThat(client().execute(GetBucketsAction.INSTANCE, request).actionGet().getBuckets().count(), equalTo(9L));
        assertThat(getJobStats(job.getId()).get(0).getDataCounts().getBucketCount(), equalTo(9L));

        timestamp += 10 * BUCKET_SPAN_SECONDS;
        data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(createJsonRecord(createRecord(timestamp)));
            timestamp += BUCKET_SPAN_SECONDS;
        }

        openJob(job.getId());
        postData(job.getId(), data.stream().collect(Collectors.joining()));
        flushJob(job.getId(), true);
        closeJob(job.getId());

        assertThat(client().execute(GetBucketsAction.INSTANCE, request).actionGet().getBuckets().count(), equalTo(29L));
        DataCounts dataCounts = getJobStats(job.getId()).get(0).getDataCounts();
        assertThat(dataCounts.getBucketCount(), equalTo(29L));
        assertThat(dataCounts.getEmptyBucketCount(), equalTo(10L));
    }

    private static Map<String, Object> createRecord(long timestamp) {
        Map<String, Object> record = new HashMap<>();
        record.put("time", timestamp);
        return record;
    }
}
