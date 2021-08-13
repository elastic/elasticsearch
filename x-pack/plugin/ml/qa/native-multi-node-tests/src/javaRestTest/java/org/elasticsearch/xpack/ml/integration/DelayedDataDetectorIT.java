/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetector;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeedBuilder;
import static org.hamcrest.Matchers.equalTo;

public class DelayedDataDetectorIT extends MlNativeAutodetectIntegTestCase {

    private final String index = "delayed-data";
    private final long now = System.currentTimeMillis();
    private long numDocs;

    @Before
    public void putDataintoIndex() {
        client().admin().indices().prepareCreate(index)
            .setMapping("time", "type=date", "value", "type=long")
            .get();
        numDocs = randomIntBetween(32, 128);
        long oneDayAgo = now - 86400000;
        writeData(logger, index, numDocs, oneDayAgo, now);
    }

    @After
    public void cleanUpTest() {
        cleanUp();
    }

    public void testMissingDataDetection() throws Exception {
        final String jobId = "delayed-data-detection-job";
        Job.Builder job = createJob(jobId, TimeValue.timeValueMinutes(5), "count", null);

        DatafeedConfig.Builder datafeedConfigBuilder =
            createDatafeedBuilder(job.getId() + "-datafeed", job.getId(), Collections.singletonList(index));
        datafeedConfigBuilder.setDelayedDataCheckConfig(DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(12)));
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
        putJob(job);
        openJob(job.getId());


        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, now);
        waitUntilJobIsClosed(jobId);

        // Get the latest finalized bucket
        Bucket lastBucket = getLatestFinalizedBucket(jobId);

        DelayedDataDetector delayedDataDetector = newDetector(job.build(new Date()), datafeedConfig);

        List<BucketWithMissingData> response = delayedDataDetector.detectMissingData(lastBucket.getEpoch()*1000);
        assertThat(response.stream().mapToLong(BucketWithMissingData::getMissingDocumentCount).sum(), equalTo(0L));

        long missingDocs = randomIntBetween(32, 128);
        // Simply adding data within the current delayed data detection, the choice of 43100000 is arbitrary and within the window
        // for the DatafeedDelayedDataDetector
        writeData(logger, index, missingDocs, now - 43100000, lastBucket.getEpoch()*1000);

        response = delayedDataDetector.detectMissingData(lastBucket.getEpoch()*1000);
        assertThat(response.stream().mapToLong(BucketWithMissingData::getMissingDocumentCount).sum(), equalTo(missingDocs));
        // Assert that the are returned in order
        List<Long> timeStamps = response.stream().map(BucketWithMissingData::getTimeStamp).collect(Collectors.toList());
        assertEquals(timeStamps.stream().sorted().collect(Collectors.toList()), timeStamps);
    }

    public void testMissingDataDetectionInSpecificBucket() throws Exception {
        final String jobId = "delayed-data-detection-job-missing-test-specific-bucket";
        Job.Builder job = createJob(jobId, TimeValue.timeValueMinutes(5), "count", null);

        DatafeedConfig.Builder datafeedConfigBuilder =
            createDatafeedBuilder(job.getId() + "-datafeed", job.getId(), Collections.singletonList(index));
        datafeedConfigBuilder.setDelayedDataCheckConfig(DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(12)));
        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();

        putJob(job);
        openJob(job.getId());


        putDatafeed(datafeedConfig);

        startDatafeed(datafeedConfig.getId(), 0L, now);
        waitUntilJobIsClosed(jobId);

        // Get the latest finalized bucket
        Bucket lastBucket = getLatestFinalizedBucket(jobId);

        DelayedDataDetector delayedDataDetector = newDetector(job.build(new Date()), datafeedConfig);

        long missingDocs = randomIntBetween(1, 10);

        // Write our missing data in the bucket right before the last finalized bucket
        writeData(logger, index, missingDocs, (lastBucket.getEpoch() - lastBucket.getBucketSpan())*1000, lastBucket.getEpoch()*1000);
        List<BucketWithMissingData> response = delayedDataDetector.detectMissingData(lastBucket.getEpoch()*1000);

        boolean hasBucketWithMissing = false;
        for (BucketWithMissingData bucketWithMissingData : response) {
            if (bucketWithMissingData.getBucket().getEpoch() == lastBucket.getEpoch() - lastBucket.getBucketSpan()) {
                assertThat(bucketWithMissingData.getMissingDocumentCount(), equalTo(missingDocs));
                hasBucketWithMissing = true;
            }
        }
        assertThat(hasBucketWithMissing, equalTo(true));

        // Assert that the are returned in order
        List<Long> timeStamps = response.stream().map(BucketWithMissingData::getTimeStamp).collect(Collectors.toList());
        assertEquals(timeStamps.stream().sorted().collect(Collectors.toList()), timeStamps);
    }

    public void testMissingDataDetectionWithAggregationsAndQuery() throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueMinutes(10);
        final String jobId = "delayed-data-detection-job-aggs-no-missing-test";
        Job.Builder job = createJob(jobId, bucketSpan, "mean", "value", "doc_count");

        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time");
        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg("value").field("value");
        DatafeedConfig.Builder datafeedConfigBuilder = createDatafeedBuilder(job.getId() + "-datafeed",
            job.getId(),
            Collections.singletonList(index));
        datafeedConfigBuilder.setParsedAggregations(new AggregatorFactories.Builder().addAggregator(
                AggregationBuilders.histogram("time")
                    .subAggregation(maxTime)
                    .subAggregation(avgAggregationBuilder)
                    .field("time")
                    .interval(TimeValue.timeValueMinutes(5).millis())));
        datafeedConfigBuilder.setParsedQuery(QueryBuilders.rangeQuery("value").gte(numDocs/2));
        datafeedConfigBuilder.setFrequency(TimeValue.timeValueMinutes(5));
        datafeedConfigBuilder.setDelayedDataCheckConfig(DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(12)));

        DatafeedConfig datafeedConfig = datafeedConfigBuilder.build();
        putJob(job);
        openJob(job.getId());


        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, now);
        waitUntilJobIsClosed(jobId);

        // Get the latest finalized bucket
        Bucket lastBucket = getLatestFinalizedBucket(jobId);

        DelayedDataDetector delayedDataDetector = newDetector(job.build(new Date()), datafeedConfig);

        List<BucketWithMissingData> response = delayedDataDetector.detectMissingData(lastBucket.getEpoch()*1000);
        assertThat(response.stream().mapToLong(BucketWithMissingData::getMissingDocumentCount).sum(), equalTo(0L));

        long missingDocs = numDocs;
        // Simply adding data within the current delayed data detection, the choice of 43100000 is arbitrary and within the window
        // for the DatafeedDelayedDataDetector
        writeData(logger, index, missingDocs, now - 43100000, lastBucket.getEpoch()*1000);

        response = delayedDataDetector.detectMissingData(lastBucket.getEpoch()*1000);
        assertThat(response.stream().mapToLong(BucketWithMissingData::getMissingDocumentCount).sum(), equalTo((missingDocs+1)/2));
        // Assert that the are returned in order
        List<Long> timeStamps = response.stream().map(BucketWithMissingData::getTimeStamp).collect(Collectors.toList());
        assertEquals(timeStamps.stream().sorted().collect(Collectors.toList()), timeStamps);
    }

    private Job.Builder createJob(String id, TimeValue bucketSpan, String function, String field) {
        return createJob(id, bucketSpan, function, field, null);
    }

    private Job.Builder createJob(String id, TimeValue bucketSpan, String function, String field, String summaryCountField) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        dataDescription.setTimeFormat(DataDescription.EPOCH_MS);

        Detector.Builder d = new Detector.Builder(function, field);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        analysisConfig.setSummaryCountFieldName(summaryCountField);

        Job.Builder builder = new Job.Builder();
        builder.setId(id);
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(dataDescription);
        return builder;
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

    private DelayedDataDetector newDetector(Job job, DatafeedConfig datafeedConfig) {
        return DelayedDataDetectorFactory.buildDetector(job, datafeedConfig, client(), xContentRegistry());
    }
}
