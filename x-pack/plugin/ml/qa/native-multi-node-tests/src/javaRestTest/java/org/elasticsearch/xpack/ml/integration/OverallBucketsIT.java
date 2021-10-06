/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetOverallBucketsAction;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that overall bucket results are calculated correctly
 * for jobs that have many buckets.
 */
public class OverallBucketsIT extends MlNativeAutodetectIntegTestCase {

    private static final String JOB_ID = "overall-buckets-test";
    private static final long BUCKET_SPAN_SECONDS = 3600;

    @After
    public void cleanUpTest() {
        cleanUp();
    }

    public void test() throws Exception {
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
                Collections.singletonList(new Detector.Builder("count", null).build()));
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
        for (int i = 0; i < 3000; i++) {
            data.add(createJsonRecord(createRecord(timestamp)));
            if (i % 1000 == 0) {
                data.add(createJsonRecord(createRecord(timestamp)));
                data.add(createJsonRecord(createRecord(timestamp)));
                data.add(createJsonRecord(createRecord(timestamp)));
            }
            timestamp += BUCKET_SPAN_SECONDS;
        }

        postData(job.getId(), data.stream().collect(Collectors.joining()));
        flushJob(job.getId(), true);
        closeJob(job.getId());

        GetBucketsAction.Request request = new GetBucketsAction.Request(job.getId());
        request.setPageParams(new PageParams(0, 3000));
        assertThat(client().execute(GetBucketsAction.INSTANCE, request).actionGet().getBuckets().count(), equalTo(3000L));

        {
            // Check we get equal number of overall buckets on a default request
            GetOverallBucketsAction.Request overallBucketsRequest = new GetOverallBucketsAction.Request(job.getId());
            GetOverallBucketsAction.Response overallBucketsResponse = client().execute(
                    GetOverallBucketsAction.INSTANCE, overallBucketsRequest).actionGet();
            assertThat(overallBucketsResponse.getOverallBuckets().count(), equalTo(3000L));
        }

        {
            // Check overall buckets are half when the bucket_span is set to double the job bucket span
            GetOverallBucketsAction.Request aggregatedOverallBucketsRequest = new GetOverallBucketsAction.Request(job.getId());
            aggregatedOverallBucketsRequest.setBucketSpan(TimeValue.timeValueSeconds(2 * BUCKET_SPAN_SECONDS));
            GetOverallBucketsAction.Response aggregatedOverallBucketsResponse = client().execute(
                    GetOverallBucketsAction.INSTANCE, aggregatedOverallBucketsRequest).actionGet();
            assertThat(aggregatedOverallBucketsResponse.getOverallBuckets().count(), equalTo(1500L));
        }

        {
            // Check overall score filtering works when chunking takes place
            GetOverallBucketsAction.Request filteredOverallBucketsRequest = new GetOverallBucketsAction.Request(job.getId());
            filteredOverallBucketsRequest.setOverallScore(0.1);
            GetOverallBucketsAction.Response filteredOverallBucketsResponse = client().execute(
                    GetOverallBucketsAction.INSTANCE, filteredOverallBucketsRequest).actionGet();
            assertThat(filteredOverallBucketsResponse.getOverallBuckets().count(), equalTo(2L));
        }
    }

    private static Map<String, Object> createRecord(long timestamp) {
        Map<String, Object> record = new HashMap<>();
        record.put("time", timestamp);
        return record;
    }
}
