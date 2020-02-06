/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * A fast integration test for categorization
 */
public class CategorizationIT extends MlNativeAutodetectIntegTestCase {

    private static final String DATA_INDEX = "log-data";

    private long nowMillis;

    @Before
    public void setUpData() {
        client().admin().indices().prepareCreate(DATA_INDEX)
                .setMapping("time", "type=date,format=epoch_millis",
                        "msg", "type=text")
                .get();

        nowMillis = System.currentTimeMillis();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        IndexRequest indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source("time", nowMillis - TimeValue.timeValueHours(2).millis(),
                "msg", "Node 1 started");
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source("time", nowMillis - TimeValue.timeValueHours(2).millis() + 1,
                "msg", "Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused " +
                        "by foo exception]");
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source("time", nowMillis - TimeValue.timeValueHours(1).millis(),
                "msg", "Node 2 started");
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source("time", nowMillis - TimeValue.timeValueHours(1).millis() + 1,
                "msg", "Failed to shutdown [error but this time completely different]");
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source("time", nowMillis, "msg", "Node 3 started");
        bulkRequestBuilder.add(indexRequest);

        BulkResponse bulkResponse = bulkRequestBuilder
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .get();
        assertThat(bulkResponse.hasFailures(), is(false));
    }

    @After
    public void tearDownData() {
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
        datafeedConfig.setIndices(Collections.singletonList(DATA_INDEX));
        DatafeedConfig datafeed = datafeedConfig.build();
        registerDatafeed(datafeed);
        putDatafeed(datafeed);
        startDatafeed(datafeedId, 0, nowMillis);
        waitUntilJobIsClosed(job.getId());

        List<CategoryDefinition> categories = getCategories(job.getId());
        assertThat(categories.size(), equalTo(3));

        CategoryDefinition category1 = categories.get(0);
        assertThat(category1.getRegex(), equalTo(".*?Node.+?started.*"));
        assertThat(category1.getExamples(),
                equalTo(Arrays.asList("Node 1 started", "Node 2 started")));

        CategoryDefinition category2 = categories.get(1);
        assertThat(category2.getRegex(), equalTo(".*?Failed.+?to.+?shutdown.+?error.+?" +
                "org\\.aaaa\\.bbbb\\.Cccc.+?line.+?caused.+?by.+?foo.+?exception.*"));
        assertThat(category2.getExamples(), equalTo(Collections.singletonList(
                "Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused by foo exception]")));

        CategoryDefinition category3 = categories.get(2);
        assertThat(category3.getRegex(), equalTo(".*?Failed.+?to.+?shutdown.+?error.+?but.+?" +
                "this.+?time.+?completely.+?different.*"));
        assertThat(category3.getExamples(), equalTo(Collections.singletonList(
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
        Job.Builder job = newJobBuilder("categorization-with-filters", Collections.singletonList("\\[.*\\]"));
        registerJob(job);
        putJob(job);
        openJob(job.getId());

        String datafeedId = job.getId() + "-feed";
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, job.getId());
        datafeedConfig.setIndices(Collections.singletonList(DATA_INDEX));
        DatafeedConfig datafeed = datafeedConfig.build();
        registerDatafeed(datafeed);
        putDatafeed(datafeed);
        startDatafeed(datafeedId, 0, nowMillis);
        waitUntilJobIsClosed(job.getId());

        List<CategoryDefinition> categories = getCategories(job.getId());
        assertThat(categories.size(), equalTo(2));

        CategoryDefinition category1 = categories.get(0);
        assertThat(category1.getRegex(), equalTo(".*?Node.+?started.*"));
        assertThat(category1.getExamples(),
                equalTo(Arrays.asList("Node 1 started", "Node 2 started")));

        CategoryDefinition category2 = categories.get(1);
        assertThat(category2.getRegex(), equalTo(".*?Failed.+?to.+?shutdown.*"));
        assertThat(category2.getExamples(), equalTo(Arrays.asList(
                "Failed to shutdown [error but this time completely different]",
                "Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused by foo exception]")));
    }

    public void testCategorizationPerformance() {
        // To compare Java/C++ tokenization performance:
        // 1. Change false to true in this assumption
        // 2. Run the test several times
        // 3. Change MachineLearning.CATEGORIZATION_TOKENIZATION_IN_JAVA to false
        // 4. Run the test several more times
        // 5. Check the timings that get logged
        // 6. Revert the changes to this assumption and MachineLearning.CATEGORIZATION_TOKENIZATION_IN_JAVA
        assumeTrue("This is time consuming to run on every build - it should be run manually when comparing Java/C++ tokenization",
                false);

        int testBatchSize = 1000;
        int testNumBatches = 1000;
        String[] possibleMessages = new String[] {
                "<sol13m-9402.1.p2ps: Info: Tue Apr 06  19:00:16 2010> Source LOTS on 33080:817 has shut down.<END>",
                "<lnl00m-8601.1.p2ps: Alert: Tue Apr 06  18:57:24 2010> P2PS failed to connect to the hrm server. "
                        + "Reason: Failed to connect to hrm server - No ACK from SIPC<END>",
                "<sol00m-8607.1.p2ps: Debug: Tue Apr 06  18:56:43 2010>  Did not receive an image data for IDN_SELECTFEED:7630.T on 493. "
                        + "Recalling item. <END>",
                "<lnl13m-8602.1.p2ps.rrcpTransport.0.sinkSide.rrcp.transmissionBus: Warning: Tue Apr 06  18:36:32 2010> "
                        + "RRCP STATUS MSG: RRCP_REBOOT: node 33191 has rebooted<END>",
                "<sol00m-8608.1.p2ps: Info: Tue Apr 06  18:30:02 2010> Source PRISM_VOBr on 33069:757 has shut down.<END>",
                "<lnl06m-9402.1.p2ps: Info: Thu Mar 25  18:30:01 2010> Service PRISM_VOB has shut down.<END>"
        };

        String jobId = "categorization-performance";
        Job.Builder job = newJobBuilder(jobId, Collections.emptyList());
        registerJob(job);
        putJob(job);
        openJob(job.getId());

        long startTime = System.currentTimeMillis();

        for (int batchNum = 0; batchNum < testNumBatches; ++batchNum) {
            StringBuilder json = new StringBuilder(testBatchSize * 100);
            for (int docNum = 0; docNum < testBatchSize; ++docNum) {
                json.append(String.format(Locale.ROOT, "{\"time\":1000000,\"msg\":\"%s\"}\n",
                        possibleMessages[docNum % possibleMessages.length]));
            }
            postData(jobId, json.toString());
        }
        flushJob(jobId, false);

        long duration = System.currentTimeMillis() - startTime;
        LogManager.getLogger(CategorizationIT.class).info("Performance test with tokenization in " +
                (MachineLearning.CATEGORIZATION_TOKENIZATION_IN_JAVA ? "Java" : "C++") + " took " + duration + "ms");
    }

    private static Job.Builder newJobBuilder(String id, List<String> categorizationFilters) {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detector.setByFieldName("mlcategory");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
                Collections.singletonList(detector.build()));
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
