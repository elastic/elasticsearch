/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.junit.After;
import org.junit.Before;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeedBuilder;
import static org.hamcrest.Matchers.greaterThan;

public class BulkFailureRetryIT extends MlNativeAutodetectIntegTestCase {

    private final String index = "bulk-failure-retry";
    private long now = System.currentTimeMillis();
    private static long DAY = Duration.ofDays(1).toMillis();
    private final String jobId = "bulk-failure-retry-job";
    private final String resultsIndex = ".ml-anomalies-custom-bulk-failure-retry-job";

    @Before
    public void putPastDataIntoIndex() {
        client().admin().indices().prepareCreate(index)
            .setMapping("time", "type=date", "value", "type=long")
            .get();
        long twoDaysAgo = now - DAY * 2;
        long threeDaysAgo = now - DAY * 3;
        writeData(logger, index, 250, threeDaysAgo, twoDaysAgo);
    }

    @After
    public void cleanUpTest() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .putNull("xpack.ml.persist_results_max_retries")
                .putNull("logger.org.elasticsearch.xpack.ml.datafeed.DatafeedJob")
                .putNull("logger.org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister")
                .putNull("logger.org.elasticsearch.xpack.ml.job.process.autodetect.output")
                .build()).get();
        cleanUp();
    }

    private void ensureAnomaliesWrite() throws InterruptedException {
        Settings settings = Settings.builder().put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), false).build();
        AtomicReference<AcknowledgedResponse> acknowledgedResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(
            listener -> client().admin().indices().prepareUpdateSettings(resultsIndex).setSettings(settings).execute(listener),
            acknowledgedResponseHolder,
            exceptionHolder);
        if (exceptionHolder.get() != null) {
            fail("FAILED TO MARK ["+ resultsIndex + "] as read-write again" + exceptionHolder.get());
        }
    }

    private void setAnomaliesReadOnlyBlock() throws InterruptedException {
        Settings settings = Settings.builder().put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true).build();
        AtomicReference<AcknowledgedResponse> acknowledgedResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(
            listener -> client().admin().indices().prepareUpdateSettings(resultsIndex).setSettings(settings).execute(listener),
            acknowledgedResponseHolder,
            exceptionHolder);
        if (exceptionHolder.get() != null) {
            fail("FAILED TO MARK ["+ resultsIndex + "] as read-ONLY: " + exceptionHolder.get());
        }
    }

    public void testBulkFailureRetries() throws Exception {
        Job.Builder job = createJob(jobId, TimeValue.timeValueMinutes(5), "count", null);
        job.setResultsIndexName(jobId);

        DatafeedConfig.Builder datafeedConfigBuilder =
            createDatafeedBuilder(job.getId() + "-datafeed", job.getId(), Collections.singletonList(index));
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
        registerJob(job);
        putJob(job);
        openJob(job.getId());
        registerDatafeed(datafeedConfig);
        putDatafeed(datafeedConfig);
        long twoDaysAgo = now - 2 * DAY;
        startDatafeed(datafeedConfig.getId(), 0L, twoDaysAgo);
        waitUntilJobIsClosed(jobId);

        // Get the job stats
        Bucket initialLatestBucket = getLatestFinalizedBucket(jobId);
        assertThat(initialLatestBucket.getEpoch(), greaterThan(0L));

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put("logger.org.elasticsearch.xpack.ml.datafeed.DatafeedJob", "TRACE")
                .put("logger.org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister", "TRACE")
                .put("logger.org.elasticsearch.xpack.ml.job.process.autodetect.output", "TRACE")
                .put("xpack.ml.persist_results_max_retries", "15")
                .build()).get();

        setAnomaliesReadOnlyBlock();

        int moreDocs = 1_000;
        writeData(logger, index, moreDocs, twoDaysAgo, now);

        openJob(job.getId());
        startDatafeed(datafeedConfig.getId(), twoDaysAgo, now);

        ensureAnomaliesWrite();
        waitUntilJobIsClosed(jobId);

        Bucket newLatestBucket = getLatestFinalizedBucket(jobId);
        assertThat(newLatestBucket.getEpoch(), greaterThan(initialLatestBucket.getEpoch()));
    }

    private Job.Builder createJob(String id, TimeValue bucketSpan, String function, String field) {
        return createJob(id, bucketSpan, function, field, null);
    }

    private Job.Builder createJob(String id, TimeValue bucketSpan, String function, String field, String summaryCountField) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.XCONTENT);
        dataDescription.setTimeField("time");
        dataDescription.setTimeFormat(DataDescription.EPOCH_MS);

        Detector.Builder d = new Detector.Builder(function, field);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()))
            .setBucketSpan(bucketSpan)
            .setSummaryCountFieldName(summaryCountField);

        return new Job.Builder().setId(id).setAnalysisConfig(analysisConfig).setDataDescription(dataDescription);
    }

    private void writeData(Logger logger, String index, long numDocs, long start, long end) {
        int maxDelta = (int) (end - start - 1);
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            IndexRequest indexRequest = new IndexRequest(index);
            long timestamp = start + randomIntBetween(0, maxDelta);
            assert timestamp >= start && timestamp < end;
            indexRequest.source("time", timestamp, "value", i);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        if (bulkResponse.hasFailures()) {
            int failures = 0;
            for (BulkItemResponse itemResponse : bulkResponse) {
                if (itemResponse.isFailed()) {
                    failures++;
                    logger.error("Item response failure [{}]", itemResponse.getFailureMessage());
                }
            }
            fail("Bulk response contained " + failures + " failures");
        }
        logger.info("Indexed [{}] documents", numDocs);
    }

    private Bucket getLatestFinalizedBucket(String jobId) {
        GetBucketsAction.Request getBucketsRequest = new GetBucketsAction.Request(jobId);
        getBucketsRequest.setExcludeInterim(true);
        getBucketsRequest.setSort(Result.TIMESTAMP.getPreferredName());
        getBucketsRequest.setDescending(true);
        getBucketsRequest.setPageParams(new PageParams(0, 1));
        return getBuckets(getBucketsRequest).get(0);
    }

    private <T> void blockingCall(Consumer<ActionListener<T>> function,
                                  AtomicReference<T> response,
                                  AtomicReference<Exception> error) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(
            r -> {
                response.set(r);
                latch.countDown();
            },
            e -> {
                error.set(e);
                latch.countDown();
            }
        );

        function.accept(listener);
        latch.await();
    }
}
