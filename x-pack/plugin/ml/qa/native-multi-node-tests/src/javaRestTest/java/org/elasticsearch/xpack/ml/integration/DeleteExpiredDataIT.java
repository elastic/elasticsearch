/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.ml.annotations.AnnotationTests.randomAnnotation;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class DeleteExpiredDataIT extends MlNativeAutodetectIntegTestCase {

    private static final String DATA_INDEX = "delete-expired-data-test-data";
    private static final String TIME_FIELD = "time";
    private static final String USER_NAME = "some-user";

    @Before
    public void setUpData()  {
        client().admin().indices().prepareCreate(DATA_INDEX)
                .setMapping("time", "type=date,format=epoch_millis")
                .get();

        // We are going to create 3 days of data ending 1 hr ago
        long latestBucketTime = System.currentTimeMillis() - TimeValue.timeValueHours(1).millis();
        int totalBuckets = 3 * 24;
        int normalRate = 10;
        int anomalousRate = 100;
        int anomalousBucket = 30;
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int bucket = 0; bucket < totalBuckets; bucket++) {
            long timestamp = latestBucketTime - TimeValue.timeValueHours(totalBuckets - bucket).getMillis();
            int bucketRate = bucket == anomalousBucket ? anomalousRate : normalRate;
            for (int point = 0; point < bucketRate; point++) {
                IndexRequest indexRequest = new IndexRequest(DATA_INDEX);
                indexRequest.source("time", timestamp);
                bulkRequestBuilder.add(indexRequest);
            }
        }

        BulkResponse bulkResponse = bulkRequestBuilder
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        assertThat(bulkResponse.hasFailures(), is(false));
    }

    @After
    public void tearDownData() {
        client().admin().indices().prepareDelete(DATA_INDEX).get();
        cleanUp();
    }

    public void testDeleteExpiredData_GivenNothingToDelete() throws Exception {
        // Tests that nothing goes wrong when there's nothing to delete
        client().execute(DeleteExpiredDataAction.INSTANCE, new DeleteExpiredDataAction.Request()).get();
    }

    @AwaitsFix( bugUrl = "https://github.com/elastic/elasticsearch/issues/62699")
    public void testDeleteExpiredDataNoThrottle() throws Exception {
        testExpiredDeletion(null, 10010);
    }

    /**
     * Verifies empty state indices deletion. Here is the summary of indices used by the test:
     *
     * +------------------+--------+----------+-------------------------+
     * | index name       | empty? | current? | expected to be removed? |
     * +------------------+--------+----------+-------------------------+
     * | .ml-state        | yes    | no       | yes                     |
     * | .ml-state-000001 | no     | no       | no                      |
     * | .ml-state-000003 | yes    | no       | yes                     |
     * | .ml-state-000005 | no     | no       | no                      |
     * | .ml-state-000007 | yes    | yes      | no                      |
     * +------------------+--------+----------+-------------------------+
     */
    public void testDeleteExpiredDataActionDeletesEmptyStateIndices() throws Exception {
        client().admin().indices().prepareCreate(".ml-state").get();
        client().admin().indices().prepareCreate(".ml-state-000001").get();
        client().prepareIndex(".ml-state-000001").setSource("field_1", "value_1").get();
        client().admin().indices().prepareCreate(".ml-state-000003").get();
        client().admin().indices().prepareCreate(".ml-state-000005").get();
        client().prepareIndex(".ml-state-000005").setSource("field_5", "value_5").get();
        client().admin().indices().prepareCreate(".ml-state-000007").addAlias(new Alias(".ml-state-write").isHidden(true)).get();
        refresh();

        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(".ml-state*").get();
        assertThat(Strings.toString(getIndexResponse),
            getIndexResponse.getIndices(),
            is(arrayContaining(".ml-state", ".ml-state-000001", ".ml-state-000003", ".ml-state-000005", ".ml-state-000007")));

        client().execute(DeleteExpiredDataAction.INSTANCE, new DeleteExpiredDataAction.Request()).get();
        refresh();

        getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(".ml-state*").get();
        assertThat(Strings.toString(getIndexResponse),
            getIndexResponse.getIndices(),
            // Only non-empty or current indices should survive deletion process
            is(arrayContaining(".ml-state-000001", ".ml-state-000005", ".ml-state-000007")));
    }

    @AwaitsFix( bugUrl = "https://github.com/elastic/elasticsearch/issues/62699")
    public void testDeleteExpiredDataWithStandardThrottle() throws Exception {
        testExpiredDeletion(-1.0f, 100);
    }

    private void testExpiredDeletion(Float customThrottle, int numUnusedState) throws Exception {
        // Index some unused state documents (more than 10K to test scrolling works)
        String mlStateIndexName = AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001";
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numUnusedState; i++) {
            String docId = "non_existing_job_" + randomFrom("model_state_1234567#" + i, "quantiles", "categorizer_state#" + i);
            IndexRequest indexRequest =
                new IndexRequest(mlStateIndexName)
                    .id(docId)
                    .source(Collections.emptyMap());
            bulkRequestBuilder.add(indexRequest);
        }
        ActionFuture<BulkResponse> indexUnusedStateDocsResponse = bulkRequestBuilder.execute();
        List<Job.Builder> jobs = new ArrayList<>();

        // These jobs don't thin out model state; ModelSnapshotRetentionIT tests that
        jobs.add(newJobBuilder("no-retention")
            .setResultsRetentionDays(null).setModelSnapshotRetentionDays(1000L).setDailyModelSnapshotRetentionAfterDays(1000L));
        jobs.add(newJobBuilder("results-retention")
            .setResultsRetentionDays(1L).setModelSnapshotRetentionDays(1000L).setDailyModelSnapshotRetentionAfterDays(1000L));
        jobs.add(newJobBuilder("snapshots-retention")
            .setResultsRetentionDays(null).setModelSnapshotRetentionDays(2L).setDailyModelSnapshotRetentionAfterDays(2L));
        jobs.add(newJobBuilder("snapshots-retention-with-retain")
            .setResultsRetentionDays(null).setModelSnapshotRetentionDays(2L).setDailyModelSnapshotRetentionAfterDays(2L));
        jobs.add(newJobBuilder("results-and-snapshots-retention")
            .setResultsRetentionDays(1L).setModelSnapshotRetentionDays(2L).setDailyModelSnapshotRetentionAfterDays(2L));

        List<String> shortExpiryForecastIds = new ArrayList<>();

        long now = System.currentTimeMillis();
        long oneDayAgo = now - TimeValue.timeValueHours(48).getMillis() - 1;

        // Start all jobs
        for (Job.Builder job : jobs) {
            putJob(job);

            String datafeedId = job.getId() + "-feed";
            DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, job.getId());
            datafeedConfig.setIndices(Collections.singletonList(DATA_INDEX));
            DatafeedConfig datafeed = datafeedConfig.build();

            putDatafeed(datafeed);

            // Run up to a day ago
            openJob(job.getId());
            startDatafeed(datafeedId, 0, now - TimeValue.timeValueHours(24).getMillis());
        }

        // Now let's wait for all jobs to be closed
        for (Job.Builder job : jobs) {
            waitUntilJobIsClosed(job.getId());
        }

        for (Job.Builder job : jobs) {
            assertThat(getBuckets(job.getId()).size(), is(greaterThanOrEqualTo(47)));
            assertThat(getRecords(job.getId()).size(), equalTo(2));
            List<ModelSnapshot> modelSnapshots = getModelSnapshots(job.getId());
            assertThat(modelSnapshots.size(), equalTo(1));
            String snapshotDocId = ModelSnapshot.documentId(modelSnapshots.get(0));

            // Update snapshot timestamp to force it out of snapshot retention window
            String snapshotUpdate = "{ \"timestamp\": " + oneDayAgo + "}";
            UpdateRequest updateSnapshotRequest = new UpdateRequest(".ml-anomalies-" + job.getId(), snapshotDocId);
            updateSnapshotRequest.doc(snapshotUpdate.getBytes(StandardCharsets.UTF_8), XContentType.JSON);
            client().execute(UpdateAction.INSTANCE, updateSnapshotRequest).get();

            // Now let's create some forecasts
            openJob(job.getId());

            // We must set a very small value for expires_in to keep this testable as the deletion cutoff point is the moment
            // the DeleteExpiredDataAction is called.
            String forecastShortExpiryId = forecast(job.getId(), TimeValue.timeValueHours(1), TimeValue.timeValueSeconds(1));
            shortExpiryForecastIds.add(forecastShortExpiryId);
            String forecastDefaultExpiryId = forecast(job.getId(), TimeValue.timeValueHours(1), null);
            String forecastNoExpiryId = forecast(job.getId(), TimeValue.timeValueHours(1), TimeValue.ZERO);
            waitForecastToFinish(job.getId(), forecastShortExpiryId);
            waitForecastToFinish(job.getId(), forecastDefaultExpiryId);
            waitForecastToFinish(job.getId(), forecastNoExpiryId);
        }

        // Refresh to ensure the snapshot timestamp updates are visible
        refresh("*");

        // We need to wait for the clock to tick to a new second to ensure the second time
        // around model snapshots will have a different ID (it depends on epoch seconds)
        long before = System.currentTimeMillis() / 1000;
        assertBusy(() -> assertNotEquals(before, System.currentTimeMillis() / 1000), 1, TimeUnit.SECONDS);

        for (Job.Builder job : jobs) {
            // Run up to now
            startDatafeed(job.getId() + "-feed", 0, now);
            waitUntilJobIsClosed(job.getId());
            assertThat(getBuckets(job.getId()).size(), is(greaterThanOrEqualTo(70)));
            assertThat(getRecords(job.getId()).size(), equalTo(2));
            List<ModelSnapshot> modelSnapshots = getModelSnapshots(job.getId());
            assertThat(modelSnapshots.size(), equalTo(2));
        }

        retainAllSnapshots("snapshots-retention-with-retain");

        long totalModelSizeStatsBeforeDelete = client().prepareSearch("*")
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
            .setQuery(QueryBuilders.termQuery("result_type", "model_size_stats"))
            .get().getHits().getTotalHits().value;
        long totalNotificationsCountBeforeDelete =
            client().prepareSearch(NotificationsIndex.NOTIFICATIONS_INDEX).get().getHits().getTotalHits().value;
        assertThat(totalModelSizeStatsBeforeDelete, greaterThan(0L));
        assertThat(totalNotificationsCountBeforeDelete, greaterThan(0L));

        // Verify forecasts were created
        List<ForecastRequestStats> forecastStats = getForecastStats();
        assertThat(forecastStats.size(), equalTo(jobs.size() * 3));
        for (ForecastRequestStats forecastStat : forecastStats) {
            assertThat(countForecastDocs(forecastStat.getJobId(), forecastStat.getForecastId()), equalTo(forecastStat.getRecordCount()));
        }

        // Before we call the delete-expired-data action we need to make sure the unused state docs were indexed
        assertThat(indexUnusedStateDocsResponse.get().status(), equalTo(RestStatus.OK));

        // Now call the action under test
        assertThat(deleteExpiredData(customThrottle).isDeleted(), is(true));

        // no-retention job should have kept all data
        assertThat(getBuckets("no-retention").size(), is(greaterThanOrEqualTo(70)));
        assertThat(getRecords("no-retention").size(), equalTo(2));
        assertThat(getModelSnapshots("no-retention").size(), equalTo(2));

        List<Bucket> buckets = getBuckets("results-retention");
        assertThat(buckets.size(), is(lessThanOrEqualTo(25)));
        assertThat(buckets.size(), is(greaterThanOrEqualTo(22)));
        assertThat(buckets.get(0).getTimestamp().getTime(), greaterThanOrEqualTo(oneDayAgo));
        assertThat(getRecords("results-retention").size(), equalTo(0));
        assertThat(getModelSnapshots("results-retention").size(), equalTo(2));

        assertThat(getBuckets("snapshots-retention").size(), is(greaterThanOrEqualTo(70)));
        assertThat(getRecords("snapshots-retention").size(), equalTo(2));
        assertThat(getModelSnapshots("snapshots-retention").size(), equalTo(1));

        assertThat(getBuckets("snapshots-retention-with-retain").size(), is(greaterThanOrEqualTo(70)));
        assertThat(getRecords("snapshots-retention-with-retain").size(), equalTo(2));
        assertThat(getModelSnapshots("snapshots-retention-with-retain").size(), equalTo(2));

        buckets = getBuckets("results-and-snapshots-retention");
        assertThat(buckets.size(), is(lessThanOrEqualTo(25)));
        assertThat(buckets.size(), is(greaterThanOrEqualTo(22)));
        assertThat(buckets.get(0).getTimestamp().getTime(), greaterThanOrEqualTo(oneDayAgo));
        assertThat(getRecords("results-and-snapshots-retention").size(), equalTo(0));
        assertThat(getModelSnapshots("results-and-snapshots-retention").size(), equalTo(1));

        long totalModelSizeStatsAfterDelete = client().prepareSearch("*")
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
            .setQuery(QueryBuilders.termQuery("result_type", "model_size_stats"))
            .get().getHits().getTotalHits().value;
        long totalNotificationsCountAfterDelete =
            client().prepareSearch(NotificationsIndex.NOTIFICATIONS_INDEX).get().getHits().getTotalHits().value;
        assertThat(totalModelSizeStatsAfterDelete, equalTo(totalModelSizeStatsBeforeDelete));
        assertThat(totalNotificationsCountAfterDelete, greaterThanOrEqualTo(totalNotificationsCountBeforeDelete));

        // Verify short expiry forecasts were deleted only
        forecastStats = getForecastStats();
        assertThat(forecastStats.size(), equalTo(jobs.size() * 2));
        for (ForecastRequestStats forecastStat : forecastStats) {
            assertThat(countForecastDocs(forecastStat.getJobId(), forecastStat.getForecastId()), equalTo(forecastStat.getRecordCount()));
        }
        for (Job.Builder job : jobs) {
            for (String forecastId : shortExpiryForecastIds) {
                assertThat(countForecastDocs(job.getId(), forecastId), equalTo(0L));
            }
        }

        // Verify .ml-state doesn't contain unused state documents
        SearchResponse stateDocsResponse = client().prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
            .setFetchSource(false)
            .setTrackTotalHits(true)
            .setSize(10000)
            .get();

        // Assert at least one state doc for each job
        assertThat(stateDocsResponse.getHits().getTotalHits().value, greaterThanOrEqualTo(5L));

        int nonExistingJobDocsCount = 0;
        List<String> nonExistingJobExampleIds = new ArrayList<>();
        for (SearchHit hit : stateDocsResponse.getHits().getHits()) {
            if (hit.getId().startsWith("non_existing_job")) {
                nonExistingJobDocsCount++;
                if (nonExistingJobExampleIds.size() < 10) {
                    nonExistingJobExampleIds.add(hit.getId());
                }
            }
        }
        assertThat("Documents for non_existing_job are still around; examples: " + nonExistingJobExampleIds,
            nonExistingJobDocsCount, equalTo(0));
    }

    public void testDeleteExpiresDataDeletesAnnotations() throws Exception {
        String jobId = "delete-annotations-a";
        String datafeedId = jobId + "-feed";

        // No annotations so far
        assertThatNumberOfAnnotationsIsEqualTo(0);

        Job.Builder job =
            new Job.Builder(jobId)
                .setResultsRetentionDays(2L)
                .setAnnotationsRetentionDays(1L)
                .setAnalysisConfig(
                    new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder().setFunction("count").build()))
                        .setBucketSpan(TimeValue.timeValueHours(1)))
                .setDataDescription(
                    new DataDescription.Builder()
                        .setTimeField(TIME_FIELD));

        putJob(job);

        DatafeedConfig datafeed =
            new DatafeedConfig.Builder(datafeedId, jobId)
                .setIndices(Collections.singletonList(DATA_INDEX))
                .build();

        putDatafeed(datafeed);

        openJob(jobId);
        // Run up to a day ago
        Instant now = Instant.now();
        startDatafeed(datafeedId, 0, now.minus(Duration.ofDays(1)).toEpochMilli());
        waitUntilJobIsClosed(jobId);

        assertThatNumberOfAnnotationsIsEqualTo(1);

        // The following 4 annotations are created by the system and the 2 oldest ones *will* be deleted
        client().index(randomAnnotationIndexRequest(jobId, now.minus(Duration.ofDays(1)), XPackUser.NAME)).actionGet();
        client().index(randomAnnotationIndexRequest(jobId, now.minus(Duration.ofDays(2)), XPackUser.NAME)).actionGet();
        client().index(randomAnnotationIndexRequest(jobId, now.minus(Duration.ofDays(3)), XPackUser.NAME)).actionGet();
        client().index(randomAnnotationIndexRequest(jobId, now.minus(Duration.ofDays(4)), XPackUser.NAME)).actionGet();
        // The following 4 annotations are created by the user and *will not* be deleted
        client().index(randomAnnotationIndexRequest(jobId, now.minus(Duration.ofDays(1)), USER_NAME)).actionGet();
        client().index(randomAnnotationIndexRequest(jobId, now.minus(Duration.ofDays(2)), USER_NAME)).actionGet();
        client().index(randomAnnotationIndexRequest(jobId, now.minus(Duration.ofDays(3)), USER_NAME)).actionGet();
        client().index(randomAnnotationIndexRequest(jobId, now.minus(Duration.ofDays(4)), USER_NAME)).actionGet();

        assertThatNumberOfAnnotationsIsEqualTo(9);

        client().execute(DeleteExpiredDataAction.INSTANCE, new DeleteExpiredDataAction.Request()).get();
        refresh();

        assertThatNumberOfAnnotationsIsEqualTo(7);
    }

    private static IndexRequest randomAnnotationIndexRequest(String jobId, Instant timestamp, String createUsername) throws IOException {
        Annotation annotation =
            new Annotation.Builder(randomAnnotation(jobId))
                .setTimestamp(Date.from(timestamp))
                .setCreateUsername(createUsername)
                .build();
        try (XContentBuilder xContentBuilder = annotation.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)) {
            return new IndexRequest(AnnotationIndex.WRITE_ALIAS_NAME)
                .source(xContentBuilder)
                .setRequireAlias(true)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
    }

    private static Job.Builder newJobBuilder(String id) {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = new Job.Builder(id);
        jobBuilder.setAnalysisConfig(analysisConfig);
        jobBuilder.setDataDescription(dataDescription);
        return jobBuilder;
    }

    private void retainAllSnapshots(String jobId) throws Exception {
        List<ModelSnapshot> modelSnapshots = getModelSnapshots(jobId);
        for (ModelSnapshot modelSnapshot : modelSnapshots) {
            UpdateModelSnapshotAction.Request request = new UpdateModelSnapshotAction.Request(jobId, modelSnapshot.getSnapshotId());
            request.setRetain(true);
            client().execute(UpdateModelSnapshotAction.INSTANCE, request).get();
        }
        // We need to refresh to ensure the updates are visible
        refresh("*");
    }
}
