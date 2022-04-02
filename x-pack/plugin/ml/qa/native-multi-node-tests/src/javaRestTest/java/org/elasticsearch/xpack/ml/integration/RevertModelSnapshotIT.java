/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.Annotation.Event;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.junit.After;

import java.io.IOException;
import java.sql.Date;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.annotations.AnnotationTests.randomAnnotation;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * This test pushes data through a job in 2 runs creating
 * 2 model snapshots. It then reverts to the earlier snapshot
 * and asserts the reversion worked as expected.
 */
public class RevertModelSnapshotIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void tearDownData() {
        cleanUp();
    }

    public void testRevertModelSnapshot() throws Exception {
        testRunJobInTwoPartsAndRevertSnapshotAndRunToCompletion("revert-model-snapshot-it-job", false);
    }

    public void testRevertModelSnapshot_DeleteInterveningResults() throws Exception {
        testRunJobInTwoPartsAndRevertSnapshotAndRunToCompletion("revert-model-snapshot-it-job-delete-intervening-results", true);
    }

    public void testRevertToEmptySnapshot() throws Exception {
        String jobId = "revert-to-empty-snapshot-test";

        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        long startTime = 1491004800000L;

        String data = generateData(
            startTime,
            bucketSpan,
            20,
            Arrays.asList("foo"),
            (bucketIndex, series) -> bucketIndex == 19 ? 100.0 : 10.0
        ).stream().collect(Collectors.joining());

        Job.Builder job = buildAndRegisterJob(jobId, bucketSpan);
        openJob(job.getId());
        postData(job.getId(), data);
        flushJob(job.getId(), true);
        closeJob(job.getId());

        assertThat(getJob(jobId).get(0).getModelSnapshotId(), is(notNullValue()));
        List<Bucket> expectedBuckets = getBuckets(jobId);
        assertThat(expectedBuckets.size(), equalTo(20));
        List<AnomalyRecord> expectedRecords = getRecords(jobId);
        assertThat(expectedBuckets.isEmpty(), is(false));
        assertThat(expectedRecords.isEmpty(), is(false));

        RevertModelSnapshotAction.Response revertResponse = revertModelSnapshot(jobId, "empty", true);
        assertThat(revertResponse.getModel().getSnapshotId(), equalTo("empty"));

        assertThat(getJob(jobId).get(0).getModelSnapshotId(), is(nullValue()));
        assertThat(getBuckets(jobId).isEmpty(), is(true));
        assertThat(getRecords(jobId).isEmpty(), is(true));
        assertThat(getJobStats(jobId).get(0).getDataCounts().getLatestRecordTimeStamp(), is(nullValue()));

        // Now run again and see we get same results
        openJob(job.getId());
        DataCounts dataCounts = postData(job.getId(), data);
        assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));
        flushJob(job.getId(), true);
        closeJob(job.getId());

        assertThat(getBuckets(jobId).size(), equalTo(expectedBuckets.size()));
        assertThat(getRecords(jobId), equalTo(expectedRecords));
    }

    private void testRunJobInTwoPartsAndRevertSnapshotAndRunToCompletion(String jobId, boolean deleteInterveningResults) throws Exception {
        TimeValue bucketSpan = TimeValue.timeValueHours(1);
        long startTime = 1491004800000L;

        Job.Builder job = buildAndRegisterJob(jobId, bucketSpan);
        openJob(job.getId());
        postData(
            job.getId(),
            generateData(startTime, bucketSpan, 10, Arrays.asList("foo"), (bucketIndex, series) -> bucketIndex == 5 ? 100.0 : 10.0).stream()
                .collect(Collectors.joining())
        );
        flushJob(job.getId(), true);
        String forecastId = forecast(job.getId(), TimeValue.timeValueHours(10), TimeValue.timeValueDays(100));
        waitForecastToFinish(job.getId(), forecastId);
        closeJob(job.getId());
        long numForecastDocs = countForecastDocs(job.getId(), forecastId);
        assertThat(numForecastDocs, greaterThan(0L));

        ModelSizeStats modelSizeStats1 = getJobStats(job.getId()).get(0).getModelSizeStats();
        Quantiles quantiles1 = getQuantiles(job.getId());

        List<Bucket> midwayBuckets = getBuckets(job.getId());
        Bucket revertPointBucket = midwayBuckets.get(midwayBuckets.size() - 1);
        assertThat(revertPointBucket.isInterim(), is(true));

        // We need to wait a second to ensure the second time around model snapshot will have a different ID (it depends on epoch seconds)
        waitUntil(() -> false, 1, TimeUnit.SECONDS);

        openJob(job.getId());
        postData(
            job.getId(),
            generateData(
                startTime + 10 * bucketSpan.getMillis(),
                bucketSpan,
                10,
                Arrays.asList("foo", "bar"),
                (bucketIndex, series) -> 10.0
            ).stream().collect(Collectors.joining())
        );
        closeJob(job.getId());

        ModelSizeStats modelSizeStats2 = getJobStats(job.getId()).get(0).getModelSizeStats();
        Quantiles quantiles2 = getQuantiles(job.getId());

        // Check model has grown since a new series was introduced
        assertThat(modelSizeStats2.getModelBytes(), greaterThan(modelSizeStats1.getModelBytes()));

        // Check quantiles have changed
        assertThat(quantiles2, not(equalTo(quantiles1)));

        List<Bucket> finalPreRevertBuckets = getBuckets(job.getId());
        Bucket finalPreRevertPointBucket = finalPreRevertBuckets.get(midwayBuckets.size() - 1);
        assertThat(finalPreRevertPointBucket.isInterim(), is(false));

        List<ModelSnapshot> modelSnapshots = getModelSnapshots(job.getId());
        assertThat(modelSnapshots.size(), equalTo(2));

        // Snapshots are sorted in descending timestamp order so we revert to the last of the list/earliest.
        assertThat(modelSnapshots.get(0).getTimestamp().getTime(), greaterThan(modelSnapshots.get(1).getTimestamp().getTime()));
        assertThat(getJob(job.getId()).get(0).getModelSnapshotId(), equalTo(modelSnapshots.get(0).getSnapshotId()));
        ModelSnapshot revertSnapshot = modelSnapshots.get(1);

        // Check there are 2 annotations (one per model snapshot)
        assertThatNumberOfAnnotationsIsEqualTo(2);

        // Add 3 new annotations...
        Instant lastResultTimestamp = revertSnapshot.getLatestResultTimeStamp().toInstant();
        client().index(randomAnnotationIndexRequest(job.getId(), lastResultTimestamp.plusSeconds(10), Event.DELAYED_DATA)).actionGet();
        client().index(randomAnnotationIndexRequest(job.getId(), lastResultTimestamp.plusSeconds(20), Event.MODEL_CHANGE)).actionGet();
        client().index(randomAnnotationIndexRequest(job.getId(), lastResultTimestamp.minusSeconds(10), Event.MODEL_CHANGE)).actionGet();
        // ... and check there are 5 annotations in total now
        assertThatNumberOfAnnotationsIsEqualTo(5);

        GetJobsStatsAction.Response.JobStats statsBeforeRevert = getJobStats(jobId).get(0);
        Instant timeBeforeRevert = Instant.now();

        assertThat(
            revertModelSnapshot(job.getId(), revertSnapshot.getSnapshotId(), deleteInterveningResults).status(),
            equalTo(RestStatus.OK)
        );

        GetJobsStatsAction.Response.JobStats statsAfterRevert = getJobStats(job.getId()).get(0);

        // Check model_size_stats has been reverted
        assertThat(statsAfterRevert.getModelSizeStats().getModelBytes(), equalTo(modelSizeStats1.getModelBytes()));

        if (deleteInterveningResults) {
            // Check data counts have been reverted
            assertThat(statsAfterRevert.getDataCounts().getLatestRecordTimeStamp(), equalTo(revertSnapshot.getLatestRecordTimeStamp()));
            assertThat(statsAfterRevert.getDataCounts().getLogTime(), greaterThanOrEqualTo(timeBeforeRevert));
        } else {
            assertThat(statsAfterRevert.getDataCounts(), equalTo(statsBeforeRevert.getDataCounts()));
        }

        // Check quantiles have been reverted
        assertThat(getQuantiles(job.getId()).getTimestamp(), equalTo(revertSnapshot.getLatestResultTimeStamp()));

        // Check annotations with event type from {delayed_data, model_change} have been removed if deleteInterveningResults flag is set
        assertThatNumberOfAnnotationsIsEqualTo(deleteInterveningResults ? 3 : 5);

        // Reverting should not have deleted any forecast docs
        assertThat(countForecastDocs(job.getId(), forecastId), is(numForecastDocs));

        // Re-run 2nd half of data
        openJob(job.getId());
        postData(
            job.getId(),
            generateData(
                startTime + 10 * bucketSpan.getMillis(),
                bucketSpan,
                10,
                Arrays.asList("foo", "bar"),
                (bucketIndex, series) -> 10.0
            ).stream().collect(Collectors.joining())
        );
        closeJob(job.getId());

        List<Bucket> finalPostRevertBuckets = getBuckets(job.getId());
        Bucket finalPostRevertPointBucket = finalPostRevertBuckets.get(midwayBuckets.size() - 1);
        assertThat(finalPostRevertPointBucket.getTimestamp(), equalTo(finalPreRevertPointBucket.getTimestamp()));
        assertThat(finalPostRevertPointBucket.getAnomalyScore(), equalTo(finalPreRevertPointBucket.getAnomalyScore()));
        assertThat(finalPostRevertPointBucket.getEventCount(), equalTo(finalPreRevertPointBucket.getEventCount()));

        // Re-running should not have deleted any forecast docs
        assertThat(countForecastDocs(job.getId(), forecastId), is(numForecastDocs));
    }

    private Job.Builder buildAndRegisterJob(String jobId, TimeValue bucketSpan) throws Exception {
        Detector.Builder detector = new Detector.Builder("mean", "value");
        detector.setPartitionFieldName("series");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(bucketSpan);
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        job.setDataDescription(dataDescription);
        putJob(job);
        return job;
    }

    private static List<String> generateData(
        long timestamp,
        TimeValue bucketSpan,
        int bucketCount,
        List<String> series,
        BiFunction<Integer, String, Double> timeAndSeriesToValueFunction
    ) throws IOException {
        List<String> data = new ArrayList<>();
        long now = timestamp;
        for (int i = 0; i < bucketCount; i++) {
            for (String field : series) {
                Map<String, Object> record = new HashMap<>();
                record.put("time", now);
                record.put("value", timeAndSeriesToValueFunction.apply(i, field));
                record.put("series", field);
                data.add(createJsonRecord(record));

                record = new HashMap<>();
                record.put("time", now + bucketSpan.getMillis() / 2);
                record.put("value", timeAndSeriesToValueFunction.apply(i, field));
                record.put("series", field);
                data.add(createJsonRecord(record));
            }
            now += bucketSpan.getMillis();
        }
        return data;
    }

    private Quantiles getQuantiles(String jobId) {
        SearchResponse response = client().prepareSearch(".ml-state*")
            .setQuery(QueryBuilders.idsQuery().addIds(Quantiles.documentId(jobId)))
            .setSize(1)
            .get();
        SearchHits hits = response.getHits();
        assertThat(hits.getTotalHits().value, equalTo(1L));
        try {
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                hits.getAt(0).getSourceAsString()
            );
            return Quantiles.LENIENT_PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static IndexRequest randomAnnotationIndexRequest(String jobId, Instant timestamp, Event event) throws IOException {
        Annotation annotation = new Annotation.Builder(randomAnnotation(jobId)).setTimestamp(Date.from(timestamp))
            .setCreateUsername(XPackUser.NAME)
            .setEvent(event)
            .build();
        try (XContentBuilder xContentBuilder = annotation.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)) {
            return new IndexRequest(AnnotationIndex.WRITE_ALIAS_NAME).source(xContentBuilder)
                .setRequireAlias(true)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
    }
}
