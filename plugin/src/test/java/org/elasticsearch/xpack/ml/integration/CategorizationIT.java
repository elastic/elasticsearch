/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.action.util.PageParams;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.security.Security;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * A fast integration test for categorization
 */
public class CategorizationIT extends SecurityIntegTestCase {

    private static final String DATA_INDEX = "log-data";
    private static final String DATA_TYPE = "log";

    private List<Job.Builder> jobs;
    private long nowMillis;

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
                .addMapping(DATA_TYPE, "time", "type=date,format=epoch_millis",
                        "msg", "type=keyword")
                .get();

        nowMillis = System.currentTimeMillis();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        IndexRequest indexRequest = new IndexRequest(DATA_INDEX, DATA_TYPE);
        indexRequest.source("time", nowMillis - TimeValue.timeValueHours(2).millis(),
                "msg", "Node 1 started");
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX, DATA_TYPE);
        indexRequest.source("time", nowMillis - TimeValue.timeValueHours(2).millis() + 1,
                "msg", "Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused " +
                        "by foo exception]");
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX, DATA_TYPE);
        indexRequest.source("time", nowMillis - TimeValue.timeValueHours(1).millis(),
                "msg", "Node 2 started");
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX, DATA_TYPE);
        indexRequest.source("time", nowMillis - TimeValue.timeValueHours(1).millis() + 1,
                "msg", "Failed to shutdown [error but this time completely different]");
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX, DATA_TYPE);
        indexRequest.source("time", nowMillis, "msg", "Node 3 started");
        bulkRequestBuilder.add(indexRequest);

        BulkResponse bulkResponse = bulkRequestBuilder
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .get();
        assertThat(bulkResponse.hasFailures(), is(false));
    }

    @After
    public void tearDownData() throws Exception {
        client().admin().indices().prepareDelete(DATA_INDEX).get();
        for (Job.Builder job : jobs) {
            DeleteDatafeedAction.Request deleteDatafeedRequest =
                    new DeleteDatafeedAction.Request(job.getId() + "-feed");
            client().execute(DeleteDatafeedAction.INSTANCE, deleteDatafeedRequest).get();
            DeleteJobAction.Request deleteJobRequest = new DeleteJobAction.Request(job.getId());
            client().execute(DeleteJobAction.INSTANCE, deleteJobRequest).get();
        }
        client().admin().indices().prepareRefresh("*").get();
    }

    public void testBasicCategorization() throws Exception {
        Job.Builder job = newJobBuilder("categorization", Collections.emptyList());
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        jobs.add(job);

        openJob(job.getId());

        String datafeedId = job.getId() + "-feed";
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, job.getId());
        datafeedConfig.setIndexes(Arrays.asList(DATA_INDEX));
        datafeedConfig.setTypes(Arrays.asList(DATA_TYPE));

        PutDatafeedAction.Request putDatafeedRequest =
                new PutDatafeedAction.Request(datafeedConfig.build());
        client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).get();
        startDatafeed(datafeedId, 0, nowMillis);
        waitUntilJobIsClosed(job.getId());

        List<CategoryDefinition> categories = getCategories(job.getId());
        assertThat(categories.size(), equalTo(3));

        CategoryDefinition category1 = categories.get(0);
        assertThat(category1.getRegex(), equalTo(".*?started.*"));
        assertThat(category1.getExamples(),
                equalTo(Arrays.asList("Node 1 started", "Node 2 started")));

        CategoryDefinition category2 = categories.get(1);
        assertThat(category2.getRegex(), equalTo(".*?Failed.+?to.+?shutdown.+?error.+?" +
                "org.aaaa.bbbb.Cccc.+?line.+?caused.+?by.+?foo.+?exception.*"));
        assertThat(category2.getExamples(), equalTo(Arrays.asList(
                "Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused by foo exception]")));

        CategoryDefinition category3 = categories.get(2);
        assertThat(category3.getRegex(), equalTo(".*?Failed.+?to.+?shutdown.+?error.+?but.+?" +
                "this.+?time.+?completely.+?different.*"));
        assertThat(category3.getExamples(), equalTo(Arrays.asList(
                "Failed to shutdown [error but this time completely different]")));

        openJob("categorization");
        startDatafeed(datafeedId, 0, nowMillis + 1);
        waitUntilJobIsClosed(job.getId());

        categories = getCategories(job.getId());
        assertThat(categories.size(), equalTo(3));
        assertThat(categories.get(0).getExamples(),
                equalTo(Arrays.asList("Node 1 started", "Node 2 started", "Node 3 started")));
    }

    public void testCategorizationWithFilters() throws Exception {
        Job.Builder job = newJobBuilder("categorization-with-filters",
                Arrays.asList("\\[.*\\]"));
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        jobs.add(job);

        openJob(job.getId());

        String datafeedId = job.getId() + "-feed";
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, job.getId());
        datafeedConfig.setIndexes(Arrays.asList(DATA_INDEX));
        datafeedConfig.setTypes(Arrays.asList(DATA_TYPE));

        PutDatafeedAction.Request putDatafeedRequest =
                new PutDatafeedAction.Request(datafeedConfig.build());
        client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).get();
        startDatafeed(datafeedId, 0, nowMillis);
        waitUntilJobIsClosed(job.getId());

        List<CategoryDefinition> categories = getCategories(job.getId());
        assertThat(categories.size(), equalTo(2));

        CategoryDefinition category1 = categories.get(0);
        assertThat(category1.getRegex(), equalTo(".*?started.*"));
        assertThat(category1.getExamples(),
                equalTo(Arrays.asList("Node 1 started", "Node 2 started")));

        CategoryDefinition category2 = categories.get(1);
        assertThat(category2.getRegex(), equalTo(".*?Failed.+?to.+?shutdown.*"));
        assertThat(category2.getExamples(), equalTo(Arrays.asList(
                "Failed to shutdown [error but this time completely different]",
                "Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused by foo exception]")));
    }

    private static Job.Builder newJobBuilder(String id, List<String> categorizationFilters) {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detector.setByFieldName("mlcategory");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
                Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        analysisConfig.setCategorizationFieldName("msg");
        analysisConfig.setCategorizationFilters(categorizationFilters);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = new Job.Builder(id);
        jobBuilder.setAnalysisConfig(analysisConfig);
        jobBuilder.setDataDescription(dataDescription);
        return jobBuilder;
    }

    private void openJob(String jobId) throws Exception {
        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(jobId);
        client().execute(OpenJobAction.INSTANCE, openJobRequest).get();
    }
    private void startDatafeed(String datafeedId, long start, long end) throws Exception {
        StartDatafeedAction.Request startRequest =
                new StartDatafeedAction.Request(datafeedId, start);
        startRequest.setEndTime(end);
        client().execute(StartDatafeedAction.INSTANCE, startRequest).get();
    }

    private void waitUntilJobIsClosed(String jobId) throws Exception {
        assertBusy(() -> {
            try {
                GetJobsStatsAction.Request request = new GetJobsStatsAction.Request(jobId);
                GetJobsStatsAction.Response response = client().execute(
                        GetJobsStatsAction.INSTANCE, request).get();
                assertThat(response.getResponse().results().get(0).getState(),
                        equalTo(JobState.CLOSED));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        // Refresh to ensure the snapshot timestamp updates are visible
        client().admin().indices().prepareRefresh("*").get();
    }

    private List<CategoryDefinition> getCategories(String jobId) throws Exception {
        GetCategoriesAction.Request getCategoriesRequest =
                new GetCategoriesAction.Request(jobId);
        getCategoriesRequest.setPageParams(new PageParams());
        GetCategoriesAction.Response categoriesResponse = client().execute(
                GetCategoriesAction.INSTANCE, getCategoriesRequest).get();
        return categoriesResponse.getResult().results();
    }

    @Override
    protected void ensureClusterStateConsistency() throws IOException {
        // this method in ESIntegTestCase is not plugin-friendly;
        // it does not account for plugin NamedWritableRegistries
    }
}
