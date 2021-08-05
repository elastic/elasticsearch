/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.junit.After;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class InterimResultsIT extends MlNativeAutodetectIntegTestCase {

    private static final long BUCKET_SPAN_SECONDS = 1000;

    private long time;

    @After
    public void cleanUpTest() {
        cleanUp();
    }

    public void testInterimResultsUpdates() throws Exception {
        String jobId = "test-interim-results-updates";
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
                Collections.singletonList(new Detector.Builder("max", "value").build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueSeconds(BUCKET_SPAN_SECONDS));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());

        time = 1400000000;

        // push some data, flush job, verify no interim results
        assertThat(postData(job.getId(), createData(50)).getProcessedRecordCount(), equalTo(50L));
        flushJob(job.getId(), false);
        assertThat(getInterimResults(job.getId()).isEmpty(), is(true));

        // push some more data, flush job, verify no interim results
        assertThat(postData(job.getId(), createData(30)).getProcessedRecordCount(), equalTo(30L));
        flushJob(job.getId(), false);
        assertThat(getInterimResults(job.getId()).isEmpty(), is(true));
        assertThat(time, equalTo(1400040000L));

        // push some data up to a 1/4 bucket boundary, flush (with interim), check interim results
        String data = "{\"time\":1400040000,\"value\":14}\n"
                + "{\"time\":1400040500,\"value\":12}\n"
                + "{\"time\":1400040510,\"value\":16}\n";
        assertThat(postData(job.getId(), data).getProcessedRecordCount(), equalTo(3L));
        flushJob(job.getId(), true);

        // We might need to retry this while waiting for a refresh
        assertBusy(() -> {
            List<Bucket> firstInterimBuckets = getInterimResults(job.getId());
            assertThat("interim buckets were: " + firstInterimBuckets, firstInterimBuckets.size(), equalTo(1));
            assertThat(firstInterimBuckets.get(0).getTimestamp().getTime(), equalTo(1400040000000L));
            assertThat(firstInterimBuckets.get(0).getRecords().get(0).getActual().get(0), equalTo(16.0));
        });

        // push 1 more record, flush (with interim), check same interim result
        data = "{\"time\":1400040520,\"value\":15}\n";
        assertThat(postData(job.getId(), data).getProcessedRecordCount(), equalTo(1L));
        flushJob(job.getId(), true);

        assertBusy(() -> {
            List<Bucket> secondInterimBuckets = getInterimResults(job.getId());
            assertThat(secondInterimBuckets.get(0).getTimestamp().getTime(), equalTo(1400040000000L));
        });

        // push rest of data, close, verify no interim results
        time += BUCKET_SPAN_SECONDS;
        assertThat(postData(job.getId(), createData(30)).getProcessedRecordCount(), equalTo(30L));
        closeJob(job.getId());
        assertThat(getInterimResults(job.getId()).isEmpty(), is(true));

        // Verify interim results have been replaced with finalized results
        GetBucketsAction.Request bucketRequest = new GetBucketsAction.Request(job.getId());
        bucketRequest.setTimestamp("1400040000000");
        bucketRequest.setExpand(true);
        List<Bucket> bucket = client().execute(GetBucketsAction.INSTANCE, bucketRequest).get().getBuckets().results();
        assertThat(bucket.size(), equalTo(1));
        assertThat(bucket.get(0).getRecords().get(0).getActual().get(0), equalTo(16.0));
    }

    public void testNoInterimResultsAfterAdvancingBucket() throws Exception {
        String jobId = "test-no-inerim-results-after-advancing-bucket";
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
            Collections.singletonList(new Detector.Builder("count", null).build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueSeconds(BUCKET_SPAN_SECONDS));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("epoch");
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(dataDescription);

        putJob(job);
        openJob(job.getId());

        time = 1400000000;

        // push some data, flush job, verify no interim results
        assertThat(postData(job.getId(), createData(50)).getProcessedRecordCount(), equalTo(50L));
        FlushJobAction.Response flushResponse = flushJob(job.getId(), false);
        assertThat(getInterimResults(job.getId()).isEmpty(), is(true));

        // advance time and request interim results
        long lastFinalizedBucketEnd = flushResponse.getLastFinalizedBucketEnd().toEpochMilli();
        FlushJobAction.Request advanceTimeRequest = new FlushJobAction.Request(jobId);
        advanceTimeRequest.setAdvanceTime(String.valueOf(lastFinalizedBucketEnd + BUCKET_SPAN_SECONDS * 1000));
        advanceTimeRequest.setCalcInterim(true);
        assertThat(client().execute(FlushJobAction.INSTANCE, advanceTimeRequest).actionGet().isFlushed(), is(true));

        List<Bucket> interimResults = getInterimResults(job.getId());
        assertThat(interimResults.size(), equalTo(1));

        // We expect there are no records. The bucket count is low but at the same time
        // it is too early into the bucket to consider it an anomaly. Let's verify that.
        List<AnomalyRecord> records = interimResults.get(0).getRecords();
        List<String> recordsJson = records.stream().map(Strings::toString).collect(Collectors.toList());
        assertThat("Found interim records: " + recordsJson, records.isEmpty(), is(true));

        closeJob(jobId);
    }

    private String createData(int halfBuckets) {
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < halfBuckets; i++) {
            int value = randomIntBetween(1, 3);
            data.append("{\"time\":").append(time).append(", \"value\":").append(value).append("}\n");
            time += BUCKET_SPAN_SECONDS / 2;
        }
        return data.toString();
    }

    private List<Bucket> getInterimResults(String jobId) {
        GetBucketsAction.Request request = new GetBucketsAction.Request(jobId);
        request.setExpand(true);
        request.setPageParams(new PageParams(0, 1500));
        GetBucketsAction.Response response = client().execute(GetBucketsAction.INSTANCE, request).actionGet();
        assertThat(response.getBuckets().count(), lessThan(1500L));
        List<Bucket> buckets = response.getBuckets().results();
        assertThat(buckets.size(), greaterThan(0));
        return buckets.stream().filter(Bucket::isInterim).collect(Collectors.toList());
    }
}
