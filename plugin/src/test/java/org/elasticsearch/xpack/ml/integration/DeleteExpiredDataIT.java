/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.security.Security;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class DeleteExpiredDataIT extends SecurityIntegTestCase {
    private static final String DATA_INDEX = "delete-expired-data-test-data";
    private static final String DATA_TYPE = "my_type";

    private List<Job> jobs;

    @Override
    protected Settings externalClusterClientSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4);
        builder.put(Security.USER_SETTING.getKey(), "elastic:changeme");
        builder.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), true);
        return builder.build();
    }

    @Before
    public void setUpData() throws IOException {
        jobs = new ArrayList<>();

        client().admin().indices().prepareCreate(DATA_INDEX)
                .addMapping(DATA_TYPE, "time", "type=date,format=epoch_millis")
                .get();

        // We are going to create data for last 2 days
        long nowMillis = System.currentTimeMillis();
        int totalBuckets = 3 * 24;
        int normalRate = 10;
        int anomalousRate = 100;
        int anomalousBucket = 30;
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int bucket = 0; bucket < totalBuckets; bucket++) {
            long timestamp = nowMillis - TimeValue.timeValueHours(totalBuckets - bucket).getMillis();
            int bucketRate = bucket == anomalousBucket ? anomalousRate : normalRate;
            for (int point = 0; point < bucketRate; point++) {
                IndexRequest indexRequest = new IndexRequest(DATA_INDEX, DATA_TYPE);
                indexRequest.source("time", timestamp);
                bulkRequestBuilder.add(indexRequest);
            }
        }

        BulkResponse bulkResponse = bulkRequestBuilder
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        assertThat(bulkResponse.hasFailures(), is(false));

        // Ensure all data is searchable
        client().admin().indices().prepareRefresh(DATA_INDEX).get();
    }

    @After
    public void tearDownData() throws Exception {
        client().admin().indices().prepareDelete(DATA_INDEX).get();
        for (Job job : jobs) {
            DeleteDatafeedAction.Request deleteDatafeedRequest = new DeleteDatafeedAction.Request(job.getId() + "-feed");
            client().execute(DeleteDatafeedAction.INSTANCE, deleteDatafeedRequest).get();
            DeleteJobAction.Request deleteJobRequest = new DeleteJobAction.Request(job.getId());
            client().execute(DeleteJobAction.INSTANCE, deleteJobRequest).get();
        }
    }

    public void testDeleteExpiredData() throws Exception {
        jobs.add(newJobBuilder("no-retention").build());
        jobs.add(newJobBuilder("results-retention").setResultsRetentionDays(1L).build());
        jobs.add(newJobBuilder("snapshots-retention").setModelSnapshotRetentionDays(2L).build());
        jobs.add(newJobBuilder("results-and-snapshots-retention").setResultsRetentionDays(1L).setModelSnapshotRetentionDays(2L).build());

        long now = System.currentTimeMillis();
        long oneDayAgo = now - TimeValue.timeValueHours(48).getMillis() - 1;
        for (Job job : jobs) {
            PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
            client().execute(PutJobAction.INSTANCE, putJobRequest).get();

            String datafeedId = job.getId() + "-feed";
            DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, job.getId());
            datafeedConfig.setIndexes(Arrays.asList(DATA_INDEX));
            datafeedConfig.setTypes(Arrays.asList(DATA_TYPE));

            PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(datafeedConfig.build());
            client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).get();

            // Run up to a day ago
            openJob(job.getId());
            startDatafeed(datafeedId, 0, now - TimeValue.timeValueHours(24).getMillis());
            waitUntilJobIsClosed(job.getId());
            assertThat(getBuckets(job.getId()).size(), is(greaterThanOrEqualTo(47)));
            assertThat(getRecords(job.getId()).size(), equalTo(1));
            List<ModelSnapshot> modelSnapshots = getModelSnapshots(job.getId());
            assertThat(modelSnapshots.size(), equalTo(1));
            String snapshotDocId = job.getId() + "-" + modelSnapshots.get(0).getSnapshotId();

            // Update snapshot timestamp to force it out of snapshot retention window
            String snapshotUpdate = "{ \"timestamp\": " + oneDayAgo + "}";
            UpdateRequest updateSnapshotRequest = new UpdateRequest(".ml-anomalies-" + job.getId(), "model_snapshot", snapshotDocId);
            updateSnapshotRequest.doc(snapshotUpdate.getBytes(StandardCharsets.UTF_8), XContentType.JSON);
            client().execute(UpdateAction.INSTANCE, updateSnapshotRequest).get();
        }
        // Refresh to ensure the snapshot timestamp updates are visible
        client().admin().indices().prepareRefresh("*").get();

        // We need to wait a second to ensure the second time around model snapshots will have a different ID (it depends on epoch seconds)
        awaitBusy(() -> false, 1, TimeUnit.SECONDS);

        for (Job job : jobs) {
            // Run up to now
            openJob(job.getId());
            startDatafeed(job.getId() + "-feed", 0, now);
            waitUntilJobIsClosed(job.getId());
            assertThat(getBuckets(job.getId()).size(), is(greaterThanOrEqualTo(70)));
            assertThat(getRecords(job.getId()).size(), equalTo(1));
            List<ModelSnapshot> modelSnapshots = getModelSnapshots(job.getId());
            assertThat(modelSnapshots.size(), equalTo(2));
        }

        long totalModelSizeStatsBeforeDelete = client().prepareSearch("*").setTypes("result")
                .setQuery(QueryBuilders.termQuery("result_type", "model_size_stats"))
                .get().getHits().totalHits;
        long totalNotificationsCountBeforeDelete = client().prepareSearch(".ml-notifications").get().getHits().totalHits;
        assertThat(totalModelSizeStatsBeforeDelete, greaterThan(0L));
        assertThat(totalNotificationsCountBeforeDelete, greaterThan(0L));

        client().execute(DeleteExpiredDataAction.INSTANCE, new DeleteExpiredDataAction.Request()).get();

        // We need to refresh to ensure the deletion is visible
        client().admin().indices().prepareRefresh("*").get();

        // no-retention job should have kept all data
        assertThat(getBuckets("no-retention").size(), is(greaterThanOrEqualTo(70)));
        assertThat(getRecords("no-retention").size(), equalTo(1));
        assertThat(getModelSnapshots("no-retention").size(), equalTo(2));

        List<Bucket> buckets = getBuckets("results-retention");
        assertThat(buckets.size(), is(lessThanOrEqualTo(24)));
        assertThat(buckets.size(), is(greaterThanOrEqualTo(22)));
        assertThat(buckets.get(0).getTimestamp().getTime(), greaterThanOrEqualTo(oneDayAgo));
        assertThat(getRecords("results-retention").size(), equalTo(0));
        assertThat(getModelSnapshots("results-retention").size(), equalTo(2));

        assertThat(getBuckets("snapshots-retention").size(), is(greaterThanOrEqualTo(70)));
        assertThat(getRecords("snapshots-retention").size(), equalTo(1));
        assertThat(getModelSnapshots("snapshots-retention").size(), equalTo(1));

        buckets = getBuckets("results-and-snapshots-retention");
        assertThat(buckets.size(), is(lessThanOrEqualTo(24)));
        assertThat(buckets.size(), is(greaterThanOrEqualTo(22)));
        assertThat(buckets.get(0).getTimestamp().getTime(), greaterThanOrEqualTo(oneDayAgo));
        assertThat(getRecords("results-and-snapshots-retention").size(), equalTo(0));
        assertThat(getModelSnapshots("results-and-snapshots-retention").size(), equalTo(1));

        long totalModelSizeStatsAfterDelete = client().prepareSearch("*").setTypes("result")
                .setQuery(QueryBuilders.termQuery("result_type", "model_size_stats"))
                .get().getHits().totalHits;
        long totalNotificationsCountAfterDelete = client().prepareSearch(".ml-notifications").get().getHits().totalHits;
        assertThat(totalModelSizeStatsAfterDelete, equalTo(totalModelSizeStatsBeforeDelete));
        assertThat(totalNotificationsCountAfterDelete, greaterThanOrEqualTo(totalNotificationsCountBeforeDelete));
    }

    private static Job.Builder newJobBuilder(String id) {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = new Job.Builder(id);
        jobBuilder.setAnalysisConfig(analysisConfig);
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        return jobBuilder;
    }

    private void openJob(String jobId) throws Exception {
        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(jobId);
        client().execute(OpenJobAction.INSTANCE, openJobRequest).get();
    }
    private void startDatafeed(String datafeedId, long start, long end) throws Exception {
        StartDatafeedAction.Request startRequest = new StartDatafeedAction.Request(datafeedId, start);
        startRequest.setEndTime(end);
        client().execute(StartDatafeedAction.INSTANCE, startRequest).get();
    }

    private void waitUntilJobIsClosed(String jobId) throws Exception {
        assertBusy(() -> {
            try {
                GetJobsStatsAction.Request request = new GetJobsStatsAction.Request(jobId);
                GetJobsStatsAction.Response response = client().execute(GetJobsStatsAction.INSTANCE, request).get();
                assertThat(response.getResponse().results().get(0).getState(), equalTo(JobState.CLOSED));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private List<Bucket> getBuckets(String jobId) throws Exception {
        GetBucketsAction.Request request = new GetBucketsAction.Request(jobId);
        GetBucketsAction.Response response = client().execute(GetBucketsAction.INSTANCE, request).get();
        return response.getBuckets().results();
    }

    private List<AnomalyRecord> getRecords(String jobId) throws Exception {
        GetRecordsAction.Request request = new GetRecordsAction.Request(jobId);
        GetRecordsAction.Response response = client().execute(GetRecordsAction.INSTANCE, request).get();
        return response.getRecords().results();
    }

    private List<ModelSnapshot> getModelSnapshots(String jobId) throws Exception {
        GetModelSnapshotsAction.Request request = new GetModelSnapshotsAction.Request(jobId, null);
        GetModelSnapshotsAction.Response response = client().execute(GetModelSnapshotsAction.INSTANCE, request).get();
        return response.getPage().results();
    }

    @Override
    protected void ensureClusterStateConsistency() throws IOException {
        // this method in ESIntegTestCase is not plugin-friendly - it does not account for plugin NamedWritableRegistries
    }
}
