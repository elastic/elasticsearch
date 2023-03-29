/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.PerPartitionCategorizationConfig;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizationStatus;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerStats;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * A fast integration test for categorization
 */
public class CategorizationIT extends MlNativeAutodetectIntegTestCase {

    private static final String DATA_INDEX = "log-data";

    private long nowMillis;

    @Before
    public void setUpData() {
        client().admin().indices().prepareCreate(DATA_INDEX).setMapping("time", "type=date,format=epoch_millis", "msg", "type=text").get();

        nowMillis = System.currentTimeMillis();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        IndexRequest indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source("time", nowMillis - TimeValue.timeValueHours(2).millis(), "msg", "Node 1 started", "part", "nodes");
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source(
            "time",
            nowMillis - TimeValue.timeValueHours(2).millis() + 1,
            "msg",
            "Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused by foo exception]",
            "part",
            "shutdowns"
        );
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source("time", nowMillis - TimeValue.timeValueHours(1).millis(), "msg", "Node 2 started", "part", "nodes");
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source(
            "time",
            nowMillis - TimeValue.timeValueHours(1).millis() + 1,
            "msg",
            "Failed to shutdown [error but this time completely different]",
            "part",
            "shutdowns"
        );
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(DATA_INDEX);
        indexRequest.source("time", nowMillis, "msg", "Node 3 started", "part", "nodes");
        bulkRequestBuilder.add(indexRequest);

        BulkResponse bulkResponse = bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL).get();
        assertThat(bulkResponse.hasFailures(), is(false));
    }

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testBasicCategorization() throws Exception {
        Job.Builder job = newJobBuilder("categorization", Collections.emptyList(), false);
        putJob(job);
        openJob(job.getId());

        String datafeedId = job.getId() + "-feed";
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, job.getId());
        datafeedConfig.setIndices(Collections.singletonList(DATA_INDEX));
        DatafeedConfig datafeed = datafeedConfig.build();

        putDatafeed(datafeed);
        startDatafeed(datafeedId, 0, nowMillis);
        waitUntilJobIsClosed(job.getId());

        List<CategoryDefinition> categories = getCategories(job.getId());
        assertThat(categories, hasSize(3));

        CategoryDefinition category1 = categories.get(0);
        assertThat(category1.getRegex(), equalTo(".*?Node.+?started.*"));
        assertThat(category1.getExamples(), equalTo(Arrays.asList("Node 1 started", "Node 2 started")));

        CategoryDefinition category2 = categories.get(1);
        assertThat(
            category2.getRegex(),
            equalTo(".*?Failed.+?to.+?shutdown.+?error.+?org\\.aaaa\\.bbbb\\.Cccc.+?line.+?caused.+?by.+?foo.+?exception.*")
        );
        assertThat(
            category2.getExamples(),
            equalTo(Collections.singletonList("Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused by foo exception]"))
        );

        CategoryDefinition category3 = categories.get(2);
        assertThat(category3.getRegex(), equalTo(".*?Failed.+?to.+?shutdown.+?error.+?but.+?this.+?time.+?completely.+?different.*"));
        assertThat(
            category3.getExamples(),
            equalTo(Collections.singletonList("Failed to shutdown [error but this time completely different]"))
        );

        List<CategorizerStats> stats = getCategorizerStats(job.getId());
        assertThat(stats, hasSize(1));
        assertThat(stats.get(0).getCategorizationStatus(), equalTo(CategorizationStatus.OK));
        assertThat(stats.get(0).getCategorizedDocCount(), equalTo(4L));
        assertThat(stats.get(0).getTotalCategoryCount(), equalTo(3L));
        assertThat(stats.get(0).getFrequentCategoryCount(), equalTo(1L));
        assertThat(stats.get(0).getRareCategoryCount(), equalTo(2L));
        assertThat(stats.get(0).getDeadCategoryCount(), equalTo(0L));
        assertThat(stats.get(0).getFailedCategoryCount(), equalTo(0L));
        assertThat(stats.get(0).getPartitionFieldName(), nullValue());
        assertThat(stats.get(0).getPartitionFieldValue(), nullValue());

        openJob(job.getId());
        startDatafeed(datafeedId, 0, nowMillis + 1);
        waitUntilJobIsClosed(job.getId());

        categories = getCategories(job.getId());
        assertThat(categories, hasSize(3));
        assertThat(categories.get(0).getExamples(), equalTo(Arrays.asList("Node 1 started", "Node 2 started", "Node 3 started")));

        stats = getCategorizerStats(job.getId());
        assertThat(stats, hasSize(2));
    }

    public void testPerPartitionCategorization() throws Exception {
        Job.Builder job = newJobBuilder("per-partition-categorization", Collections.emptyList(), true);
        putJob(job);
        openJob(job.getId());

        String datafeedId = job.getId() + "-feed";
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, job.getId());
        datafeedConfig.setIndices(Collections.singletonList(DATA_INDEX));
        DatafeedConfig datafeed = datafeedConfig.build();

        putDatafeed(datafeed);
        startDatafeed(datafeedId, 0, nowMillis);
        waitUntilJobIsClosed(job.getId());

        List<CategoryDefinition> categories = getCategories(job.getId());
        assertThat(categories, hasSize(3));

        CategoryDefinition category1 = categories.get(0);
        assertThat(category1.getRegex(), equalTo(".*?Node.+?started.*"));
        assertThat(category1.getExamples(), equalTo(Arrays.asList("Node 1 started", "Node 2 started")));
        assertThat(category1.getPartitionFieldName(), equalTo("part"));
        assertThat(category1.getPartitionFieldValue(), equalTo("nodes"));

        CategoryDefinition category2 = categories.get(1);
        assertThat(
            category2.getRegex(),
            equalTo(".*?Failed.+?to.+?shutdown.+?error.+?org\\.aaaa\\.bbbb\\.Cccc.+?line.+?caused.+?by.+?foo.+?exception.*")
        );
        assertThat(
            category2.getExamples(),
            equalTo(Collections.singletonList("Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused by foo exception]"))
        );
        assertThat(category2.getPartitionFieldName(), equalTo("part"));
        assertThat(category2.getPartitionFieldValue(), equalTo("shutdowns"));

        CategoryDefinition category3 = categories.get(2);
        assertThat(category3.getRegex(), equalTo(".*?Failed.+?to.+?shutdown.+?error.+?but.+?this.+?time.+?completely.+?different.*"));
        assertThat(
            category3.getExamples(),
            equalTo(Collections.singletonList("Failed to shutdown [error but this time completely different]"))
        );
        assertThat(category3.getPartitionFieldName(), equalTo("part"));
        assertThat(category3.getPartitionFieldValue(), equalTo("shutdowns"));

        List<CategorizerStats> stats = getCategorizerStats(job.getId());
        assertThat(stats, hasSize(2));
        for (int i = 0; i < 2; ++i) {
            if ("nodes".equals(stats.get(i).getPartitionFieldValue())) {
                assertThat(stats.get(i).getCategorizationStatus(), equalTo(CategorizationStatus.OK));
                assertThat(stats.get(i).getCategorizedDocCount(), equalTo(2L));
                assertThat(stats.get(i).getTotalCategoryCount(), equalTo(1L));
                assertThat(stats.get(i).getFrequentCategoryCount(), equalTo(1L));
                assertThat(stats.get(i).getRareCategoryCount(), equalTo(0L));
                assertThat(stats.get(i).getDeadCategoryCount(), equalTo(0L));
                assertThat(stats.get(i).getFailedCategoryCount(), equalTo(0L));
                assertThat(stats.get(i).getPartitionFieldName(), equalTo("part"));
            } else {
                assertThat(stats.get(i).getCategorizationStatus(), equalTo(CategorizationStatus.OK));
                assertThat(stats.get(i).getCategorizedDocCount(), equalTo(2L));
                assertThat(stats.get(i).getTotalCategoryCount(), equalTo(2L));
                assertThat(stats.get(i).getFrequentCategoryCount(), equalTo(0L));
                assertThat(stats.get(i).getRareCategoryCount(), equalTo(2L));
                assertThat(stats.get(i).getDeadCategoryCount(), equalTo(0L));
                assertThat(stats.get(i).getFailedCategoryCount(), equalTo(0L));
                assertThat(stats.get(i).getPartitionFieldName(), equalTo("part"));
                assertThat(stats.get(i).getPartitionFieldValue(), equalTo("shutdowns"));
            }
        }

        openJob(job.getId());
        startDatafeed(datafeedId, 0, nowMillis + 1);
        waitUntilJobIsClosed(job.getId());

        categories = getCategories(job.getId());
        assertThat(categories, hasSize(3));
        assertThat(categories.get(0).getExamples(), equalTo(Arrays.asList("Node 1 started", "Node 2 started", "Node 3 started")));
        assertThat(categories.get(0).getPartitionFieldName(), equalTo("part"));
        assertThat(categories.get(0).getPartitionFieldValue(), equalTo("nodes"));

        stats = getCategorizerStats(job.getId());
        assertThat(stats, hasSize(3));
    }

    public void testCategorizationWithFilters() throws Exception {
        Job.Builder job = newJobBuilder("categorization-with-filters", Collections.singletonList("\\[.*\\]"), false);
        putJob(job);
        openJob(job.getId());

        String datafeedId = job.getId() + "-feed";
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, job.getId());
        datafeedConfig.setIndices(Collections.singletonList(DATA_INDEX));
        DatafeedConfig datafeed = datafeedConfig.build();

        putDatafeed(datafeed);
        startDatafeed(datafeedId, 0, nowMillis);
        waitUntilJobIsClosed(job.getId());

        List<CategoryDefinition> categories = getCategories(job.getId());
        assertThat(categories, hasSize(2));

        CategoryDefinition category1 = categories.get(0);
        assertThat(category1.getRegex(), equalTo(".*?Node.+?started.*"));
        assertThat(category1.getExamples(), equalTo(Arrays.asList("Node 1 started", "Node 2 started")));

        CategoryDefinition category2 = categories.get(1);
        assertThat(category2.getRegex(), equalTo(".*?Failed.+?to.+?shutdown.*"));
        assertThat(
            category2.getExamples(),
            equalTo(
                Arrays.asList(
                    "Failed to shutdown [error but this time completely different]",
                    "Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused by foo exception]"
                )
            )
        );
    }

    public void testCategorizationStatePersistedOnSwitchToRealtime() throws Exception {
        Job.Builder job = newJobBuilder("categorization-swtich-to-realtime", Collections.emptyList(), false);
        putJob(job);
        openJob(job.getId());

        String datafeedId = job.getId() + "-feed";
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, job.getId());
        datafeedConfig.setIndices(Collections.singletonList(DATA_INDEX));
        DatafeedConfig datafeed = datafeedConfig.build();

        putDatafeed(datafeed);
        startDatafeed(datafeedId, 0, null);

        // When the datafeed switches to realtime the C++ process will be told to persist
        // state, and this should include the categorizer state. We assert that this exists
        // before closing the job to prove that it was persisted in the background at the
        // end of lookback rather than when the job was closed.
        assertBusy(() -> {
            SearchResponse stateDocsResponse = client().prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
                .setQuery(QueryBuilders.idsQuery().addIds(CategorizerState.documentId(job.getId(), 1)))
                .get();

            SearchHit[] hits = stateDocsResponse.getHits().getHits();
            assertThat(hits, arrayWithSize(1));
            assertThat(hits[0].getSourceAsMap(), hasKey("compressed"));
        }, 30, TimeUnit.SECONDS);

        stopDatafeed(datafeedId);
        closeJob(job.getId());

        List<CategoryDefinition> categories = getCategories(job.getId());
        assertThat(categories, hasSize(3));

        CategoryDefinition category1 = categories.get(0);
        assertThat(category1.getRegex(), equalTo(".*?Node.+?started.*"));
        assertThat(category1.getExamples(), equalTo(Arrays.asList("Node 1 started", "Node 2 started")));

        CategoryDefinition category2 = categories.get(1);
        assertThat(
            category2.getRegex(),
            equalTo(".*?Failed.+?to.+?shutdown.+?error.+?" + "org\\.aaaa\\.bbbb\\.Cccc.+?line.+?caused.+?by.+?foo.+?exception.*")
        );
        assertThat(
            category2.getExamples(),
            equalTo(Collections.singletonList("Failed to shutdown [error org.aaaa.bbbb.Cccc line 54 caused by foo exception]"))
        );

        CategoryDefinition category3 = categories.get(2);
        assertThat(category3.getRegex(), equalTo(".*?Failed.+?to.+?shutdown.+?error.+?but.+?" + "this.+?time.+?completely.+?different.*"));
        assertThat(
            category3.getExamples(),
            equalTo(Collections.singletonList("Failed to shutdown [error but this time completely different]"))
        );
    }

    public void testStopOnWarn() throws IOException {

        long testTime = System.currentTimeMillis();

        String jobId = "categorization-stop-on-warn";
        Job.Builder job = newJobBuilder(jobId, Collections.emptyList(), true);
        putJob(job);
        openJob(job.getId());

        String[] messages = new String[] { "Node 1 started", "Failed to shutdown" };
        String[] partitions = new String[] { "nodes", "shutdowns" };

        StringBuilder json = new StringBuilder(1000);
        for (int docNum = 0; docNum < 200; ++docNum) {
            // Two thirds of our messages are "Node 1 started", the rest "Failed to shutdown"
            int partitionNum = (docNum % 3) / 2;
            json.append(Strings.format("""
                {"time":1000000,"part":"%s","msg":"%s"}
                """, partitions[partitionNum], messages[partitionNum]));
        }
        postData(jobId, json.toString());

        flushJob(jobId, false);

        Consumer<CategorizerStats> checkStatsAt1000000 = stats -> {
            assertThat(stats.getTimestamp().toEpochMilli(), is(1000000L));
            if ("nodes".equals(stats.getPartitionFieldValue())) {
                assertThat(stats.getCategorizationStatus(), equalTo(CategorizationStatus.WARN));
                // We've sent 134 messages but only 100 should have been categorized as the partition went to "warn" status after 100
                assertThat(stats.getCategorizedDocCount(), equalTo(100L));
                assertThat(stats.getTotalCategoryCount(), equalTo(1L));
                assertThat(stats.getFrequentCategoryCount(), equalTo(1L));
                assertThat(stats.getRareCategoryCount(), equalTo(0L));
                assertThat(stats.getDeadCategoryCount(), equalTo(0L));
                assertThat(stats.getFailedCategoryCount(), equalTo(0L));
                assertThat(stats.getPartitionFieldName(), equalTo("part"));
            } else {
                assertThat(stats.getCategorizationStatus(), equalTo(CategorizationStatus.OK));
                assertThat(stats.getCategorizedDocCount(), equalTo(66L));
                assertThat(stats.getTotalCategoryCount(), equalTo(1L));
                assertThat(stats.getFrequentCategoryCount(), equalTo(1L));
                assertThat(stats.getRareCategoryCount(), equalTo(0L));
                assertThat(stats.getDeadCategoryCount(), equalTo(0L));
                assertThat(stats.getFailedCategoryCount(), equalTo(0L));
                assertThat(stats.getPartitionFieldName(), equalTo("part"));
                assertThat(stats.getPartitionFieldValue(), equalTo("shutdowns"));
            }
            assertThat(stats.getLogTime().toEpochMilli(), greaterThanOrEqualTo(testTime));
        };

        List<CategorizerStats> stats = getCategorizerStats(jobId);
        assertThat(stats, hasSize(2));
        for (int i = 0; i < 2; ++i) {
            checkStatsAt1000000.accept(stats.get(i));
        }

        postData(jobId, json.toString().replace("1000000", "2000000"));
        closeJob(jobId);

        stats = getCategorizerStats(jobId);
        assertThat(stats, hasSize(3));
        int numStatsAt2000000 = 0;
        for (int i = 0; i < 3; ++i) {
            if (stats.get(i).getTimestamp().toEpochMilli() == 2000000L) {
                ++numStatsAt2000000;
                // Now the "shutdowns" partition has seen more than 100 messages and only has 1 category so that should be in "warn" status
                assertThat(stats.get(i).getCategorizationStatus(), equalTo(CategorizationStatus.WARN));
                // We've sent 132 messages but only 100 should have been categorized as the partition went to "warn" status after 100
                assertThat(stats.get(i).getCategorizedDocCount(), equalTo(100L));
                assertThat(stats.get(i).getTotalCategoryCount(), equalTo(1L));
                assertThat(stats.get(i).getFrequentCategoryCount(), equalTo(1L));
                assertThat(stats.get(i).getRareCategoryCount(), equalTo(0L));
                assertThat(stats.get(i).getDeadCategoryCount(), equalTo(0L));
                assertThat(stats.get(i).getFailedCategoryCount(), equalTo(0L));
                assertThat(stats.get(i).getPartitionFieldName(), equalTo("part"));
                assertThat(stats.get(i).getPartitionFieldValue(), equalTo("shutdowns"));
                assertThat(stats.get(i).getLogTime().toEpochMilli(), greaterThanOrEqualTo(testTime));
            } else {
                // The other stats documents are left over from the flush and should not have been updated,
                // so should be identical to how they were then
                checkStatsAt1000000.accept(stats.get(i));
            }
        }

        // The stats for the "nodes" partition haven't changed, so should not have been updated; all messages
        // sent at time 2000000 for the "nodes" partition should have been ignored as it had "warn" status
        assertThat(numStatsAt2000000, is(1));
    }

    public void testNumMatchesAndCategoryPreference() throws Exception {
        String index = "hadoop_logs";
        client().admin().indices().prepareCreate(index).setMapping("time", "type=date,format=epoch_millis", "msg", "type=text").get();

        nowMillis = System.currentTimeMillis();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        IndexRequest indexRequest = new IndexRequest(index);
        indexRequest.source(
            "time",
            nowMillis - TimeValue.timeValueHours(8).millis(),
            "msg",
            "2015-10-18 18:01:51,963 INFO [main] org.mortbay.log: jetty-6.1.26"
        );
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(index);
        indexRequest.source(
            "time",
            nowMillis - TimeValue.timeValueHours(7).millis(),
            "msg",
            "2015-10-18 18:01:52,728 INFO [main] org.mortbay.log: Started HttpServer2$SelectChannelConnectorWithSafeStartup@0.0.0.0:62267"
        );
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(index);
        indexRequest.source(
            "time",
            nowMillis - TimeValue.timeValueHours(6).millis(),
            "msg",
            "2015-10-18 18:01:53,400 INFO [main] org.apache.hadoop.yarn.webapp.WebApps: Registered webapp guice modules"
        );
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(index);
        indexRequest.source(
            "time",
            nowMillis - TimeValue.timeValueHours(5).millis(),
            "msg",
            "2015-10-18 18:01:53,447 INFO [main] org.apache.hadoop.mapreduce.v2.app.rm.RMContainerRequestor: nodeBlacklistingEnabled:true"
        );
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(index);
        indexRequest.source(
            "time",
            nowMillis - TimeValue.timeValueHours(4).millis(),
            "msg",
            "2015-10-18 18:01:52,728 INFO [main] org.apache.hadoop.yarn.webapp.WebApps: Web app /mapreduce started at 62267"
        );
        bulkRequestBuilder.add(indexRequest);
        indexRequest = new IndexRequest(index);
        indexRequest.source(
            "time",
            nowMillis - TimeValue.timeValueHours(2).millis(),
            "msg",
            "2015-10-18 18:01:53,557 INFO [main] org.apache.hadoop.yarn.client.RMProxy: "
                + "Connecting to ResourceManager at msra-sa-41/10.190.173.170:8030"
        );
        bulkRequestBuilder.add(indexRequest);

        indexRequest = new IndexRequest(index);
        indexRequest.source(
            "time",
            nowMillis - TimeValue.timeValueHours(1).millis(),
            "msg",
            "2015-10-18 18:01:53,713 INFO [main] org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator: "
                + "maxContainerCapability: <memory:8192, vCores:32>"
        );
        bulkRequestBuilder.add(indexRequest);

        indexRequest = new IndexRequest(index);
        indexRequest.source(
            "time",
            nowMillis,
            "msg",
            "2015-10-18 18:01:53,713 INFO [main] org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy: "
                + "yarn.client.max-cached-nodemanagers-proxies : 0"
        );
        bulkRequestBuilder.add(indexRequest);

        BulkResponse bulkResponse = bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertThat(bulkResponse.hasFailures(), is(false));

        Job.Builder job = newJobBuilder("categorization-with-preferred-categories", Collections.emptyList(), false);
        putJob(job);
        openJob(job.getId());

        String datafeedId = job.getId() + "-feed";
        DatafeedConfig.Builder datafeedConfig = new DatafeedConfig.Builder(datafeedId, job.getId());
        datafeedConfig.setIndices(Collections.singletonList(index));
        DatafeedConfig datafeed = datafeedConfig.build();

        putDatafeed(datafeed);
        startDatafeed(datafeedId, 0, nowMillis + 1);
        waitUntilJobIsClosed(job.getId());

        List<CategoryDefinition> categories = getCategories(job.getId());
        assertThat(categories, hasSize(7));

        CategoryDefinition category1 = categories.get(0);
        assertThat(category1.getNumMatches(), equalTo(2L));
        long[] expectedPreferenceTo = new long[] { 2L, 3L, 4L, 5L, 6L, 7L };
        assertThat(category1.getPreferredToCategories(), equalTo(expectedPreferenceTo));
        client().admin().indices().prepareDelete(index).get();
    }

    private static Job.Builder newJobBuilder(String id, List<String> categorizationFilters, boolean isPerPartition) {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detector.setByFieldName("mlcategory");
        if (isPerPartition) {
            detector.setPartitionFieldName("part");
        }
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        analysisConfig.setCategorizationFieldName("msg");
        analysisConfig.setCategorizationFilters(categorizationFilters);
        analysisConfig.setPerPartitionCategorizationConfig(new PerPartitionCategorizationConfig(isPerPartition, isPerPartition));
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = new Job.Builder(id);
        jobBuilder.setAnalysisConfig(analysisConfig);
        jobBuilder.setDataDescription(dataDescription);
        return jobBuilder;
    }

    private List<CategorizerStats> getCategorizerStats(String jobId) throws IOException {

        SearchResponse searchResponse = client().prepareSearch(AnomalyDetectorsIndex.jobResultsAliasedName(jobId))
            .setQuery(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), CategorizerStats.RESULT_TYPE_VALUE))
                    .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))
            )
            .setSize(1000)
            .get();

        List<CategorizerStats> stats = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        hit.getSourceRef().streamInput()
                    )
            ) {
                stats.add(CategorizerStats.LENIENT_PARSER.apply(parser, null).build());
            }
        }
        return stats;
    }
}
