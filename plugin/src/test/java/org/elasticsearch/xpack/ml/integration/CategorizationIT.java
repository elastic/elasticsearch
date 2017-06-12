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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinition;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * A fast integration test for categorization
 */
public class CategorizationIT extends MlNativeAutodetectIntegTestCase {

    private static final String DATA_INDEX = "log-data";
    private static final String DATA_TYPE = "log";

    private long nowMillis;

    @Before
    public void setUpData() throws IOException {
        client().admin().indices().prepareCreate(DATA_INDEX)
                .addMapping(DATA_TYPE, "time", "type=date,format=epoch_millis",
                        "msg", "type=text")
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
        cleanUp();
        client().admin().indices().prepareDelete(DATA_INDEX).get();
        client().admin().indices().prepareRefresh("*").get();
    }

    public void testBasicCategorization() throws Exception {
        Job.Builder job = newJobBuilder("categorization", Collections.emptyList());
        registerJob(job);
        putJob(job);
        openJob(job.getId());

        String datafeedId = job.getId() + "-feed";
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, job.getId());
        datafeedConfig.setIndices(Arrays.asList(DATA_INDEX));
        datafeedConfig.setTypes(Arrays.asList(DATA_TYPE));
        DatafeedConfig datafeed = datafeedConfig.build();
        registerDatafeed(datafeed);
        putDatafeed(datafeed);
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
        Job.Builder job = newJobBuilder("categorization-with-filters", Arrays.asList("\\[.*\\]"));
        registerJob(job);
        putJob(job);
        openJob(job.getId());

        String datafeedId = job.getId() + "-feed";
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, job.getId());
        datafeedConfig.setIndices(Arrays.asList(DATA_INDEX));
        datafeedConfig.setTypes(Arrays.asList(DATA_TYPE));
        DatafeedConfig datafeed = datafeedConfig.build();
        registerDatafeed(datafeed);
        putDatafeed(datafeed);
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
}
