/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;
import org.elasticsearch.xpack.core.rollup.action.PutRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;
import org.elasticsearch.xpack.core.rollup.action.StartRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.StopRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.DateHistoGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.IndexerState;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.hamcrest.core.IsEqual.equalTo;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RollupIT extends ESIntegTestCase {

    private String taskId = "test-bigID";

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateRollup.class, CommonAnalysisPlugin.class, Netty4Plugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder();
        builder.put(XPackSettings.ROLLUP_ENABLED.getKey(), true);
        builder.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        builder.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        return builder.build();
    }

    @Override
    protected Settings externalClusterClientSettings() {
        return nodeSettings(0);
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder().put(super.transportClientSettings())
                .put(XPackSettings.ROLLUP_ENABLED.getKey(), true)
                .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
                .build();
    }

    @Before
    public void createIndex() {
        client().admin().indices().prepareCreate("test-1").addMapping("doc", "{\"doc\": {\"properties\": {" +
                "\"date_histo\": {\"type\": \"date\"}, " +
                "\"histo\": {\"type\": \"integer\"}, " +
                "\"terms\": {\"type\": \"keyword\"}}}}", XContentType.JSON).get();
        client().admin().cluster().prepareHealth("test-1").setWaitForYellowStatus().get();

        BulkRequestBuilder bulk = client().prepareBulk();
        Map<String, Object> source = new HashMap<>(3);
        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 20; j++) {
                for (int k = 0; k < 20; k++) {
                    source.put("date_histo", new DateTime().minusDays(i).toString());
                    source.put("histo", Integer.toString(j * 100));
                    source.put("terms", Integer.toString(k * 100));
                    source.put("foo", k);
                    bulk.add(new IndexRequest("test-1", "doc").source(source));
                    source.clear();
                }
            }
        }
        bulk.get();
        client().admin().indices().prepareRefresh("test-1").get();
    }

    public void testGetJob() throws ExecutionException, InterruptedException {
        MetricConfig metricConfig = new MetricConfig.Builder()
                .setField("foo")
                .setMetrics(Arrays.asList("sum", "min", "max", "avg"))
                .build();

        DateHistoGroupConfig.Builder datehistoGroupConfig = new DateHistoGroupConfig.Builder();
        datehistoGroupConfig.setField("date_histo");
        datehistoGroupConfig.setInterval(new DateHistogramInterval("1d"));

        GroupConfig.Builder groupConfig = new GroupConfig.Builder();
        groupConfig.setDateHisto(datehistoGroupConfig.build());


        RollupJobConfig.Builder config = new RollupJobConfig.Builder();
        config.setIndexPattern("test-1");
        config.setRollupIndex("rolled");
        config.setId("testGet");
        config.setGroupConfig(groupConfig.build());
        config.setMetricsConfig(Collections.singletonList(metricConfig));
        config.setCron("* * * * * ? *");
        config.setPageSize(10);

        PutRollupJobAction.Request request = new PutRollupJobAction.Request();
        request.setConfig(config.build());
        client().execute(PutRollupJobAction.INSTANCE, request).get();

        GetRollupJobsAction.Request getRequest = new GetRollupJobsAction.Request("testGet");
        GetRollupJobsAction.Response response = client().execute(GetRollupJobsAction.INSTANCE, getRequest).get();
        assertThat(response.getJobs().size(), equalTo(1));
        assertThat(response.getJobs().get(0).getJob().getId(), equalTo("testGet"));
    }

    public void testIndexPattern() throws Exception {
        MetricConfig metricConfig = new MetricConfig.Builder()
                .setField("foo")
                .setMetrics(Arrays.asList("sum", "min", "max", "avg"))
                .build();

        DateHistoGroupConfig.Builder datehistoGroupConfig = new DateHistoGroupConfig.Builder();
        datehistoGroupConfig.setField("date_histo");
        datehistoGroupConfig.setInterval(new DateHistogramInterval("1d"));

        GroupConfig.Builder groupConfig = new GroupConfig.Builder();
        groupConfig.setDateHisto(datehistoGroupConfig.build());


        RollupJobConfig.Builder config = new RollupJobConfig.Builder();
        config.setIndexPattern("test-*");
        config.setId("testIndexPattern");
        config.setRollupIndex("rolled");
        config.setGroupConfig(groupConfig.build());
        config.setMetricsConfig(Collections.singletonList(metricConfig));
        config.setCron("* * * * * ? *");
        config.setPageSize(10);

        PutRollupJobAction.Request request = new PutRollupJobAction.Request();
        request.setConfig(config.build());
        client().execute(PutRollupJobAction.INSTANCE, request).get();

        StartRollupJobAction.Request startRequest = new StartRollupJobAction.Request("testIndexPattern");
        StartRollupJobAction.Response startResponse = client().execute(StartRollupJobAction.INSTANCE, startRequest).get();
        Assert.assertThat(startResponse.isStarted(), equalTo(true));

        // Make sure it started
        ESTestCase.assertBusy(() -> {
            RollupJobStatus rollupJobStatus = getRollupJobStatus("testIndexPattern");
            if (rollupJobStatus == null) {
                fail("null");
            }

            IndexerState state = rollupJobStatus.getIndexerState();
            assertTrue(state.equals(IndexerState.STARTED) || state.equals(IndexerState.INDEXING));
        }, 60, TimeUnit.SECONDS);

        // And wait for it to finish
        ESTestCase.assertBusy(() -> {
            RollupJobStatus rollupJobStatus = getRollupJobStatus("testIndexPattern");
            if (rollupJobStatus == null) {
                fail("null");
            }

            IndexerState state = rollupJobStatus.getIndexerState();
            assertTrue(state.equals(IndexerState.STARTED) && rollupJobStatus.getPosition() != null);
        }, 60, TimeUnit.SECONDS);

        GetRollupJobsAction.Request getRequest = new GetRollupJobsAction.Request("testIndexPattern");
        GetRollupJobsAction.Response response = client().execute(GetRollupJobsAction.INSTANCE, getRequest).get();
        Assert.assertThat(response.getJobs().size(), equalTo(1));
        Assert.assertThat(response.getJobs().get(0).getJob().getId(), equalTo("testIndexPattern"));

        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().addIndices("rolled").get();
        Assert.assertThat(getIndexResponse.indices().length, Matchers.greaterThan(0));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/30290")
    public void testTwoJobsStartStopDeleteOne() throws Exception {
        MetricConfig metricConfig = new MetricConfig.Builder()
                .setField("foo")
                .setMetrics(Arrays.asList("sum", "min", "max", "avg"))
                .build();

        DateHistoGroupConfig.Builder datehistoGroupConfig = new DateHistoGroupConfig.Builder();
        datehistoGroupConfig.setField("date_histo");
        datehistoGroupConfig.setInterval(new DateHistogramInterval("1d"));

        GroupConfig.Builder groupConfig = new GroupConfig.Builder();
        groupConfig.setDateHisto(datehistoGroupConfig.build());


        RollupJobConfig.Builder config = new RollupJobConfig.Builder();
        config.setIndexPattern("test-1");
        config.setRollupIndex("rolled");
        config.setId("job1");
        config.setGroupConfig(groupConfig.build());
        config.setMetricsConfig(Collections.singletonList(metricConfig));
        config.setCron("* * * * * ? *");
        config.setPageSize(10);

        PutRollupJobAction.Request request = new PutRollupJobAction.Request();
        request.setConfig(config.build());
        client().execute(PutRollupJobAction.INSTANCE, request).get();

        RollupJobConfig.Builder config2 = new RollupJobConfig.Builder();
        config2.setIndexPattern("test-1");
        config2.setRollupIndex("rolled");
        config2.setId("job2");
        config2.setGroupConfig(groupConfig.build());
        config2.setMetricsConfig(Collections.singletonList(metricConfig));
        config2.setCron("* * * * * ? *");
        config2.setPageSize(10);

        PutRollupJobAction.Request request2 = new PutRollupJobAction.Request();
        request2.setConfig(config2.build());
        client().execute(PutRollupJobAction.INSTANCE, request2).get();

        StartRollupJobAction.Request startRequest = new StartRollupJobAction.Request("job1");
        StartRollupJobAction.Response response = client().execute(StartRollupJobAction.INSTANCE, startRequest).get();
        Assert.assertThat(response.isStarted(), equalTo(true));

        // Make sure it started
        ESTestCase.assertBusy(() -> {
            RollupJobStatus rollupJobStatus = getRollupJobStatus("job1");
            if (rollupJobStatus == null) {
                fail("null");
            }

            IndexerState state = rollupJobStatus.getIndexerState();
            assertTrue(state.equals(IndexerState.STARTED) || state.equals(IndexerState.INDEXING));
        }, 60, TimeUnit.SECONDS);

        //but not the other task
        ESTestCase.assertBusy(() -> {
            RollupJobStatus rollupJobStatus = getRollupJobStatus("job2");

            IndexerState state = rollupJobStatus.getIndexerState();
            assertTrue(state.equals(IndexerState.STOPPED));
        }, 60, TimeUnit.SECONDS);

        // Delete the task
        DeleteRollupJobAction.Request deleteRequest = new DeleteRollupJobAction.Request("job1");
        DeleteRollupJobAction.Response deleteResponse = client().execute(DeleteRollupJobAction.INSTANCE, deleteRequest).get();
        Assert.assertTrue(deleteResponse.isAcknowledged());

        // Make sure the first job's task is gone
        ESTestCase.assertBusy(() -> {
            RollupJobStatus rollupJobStatus = getRollupJobStatus("job1");
            assertTrue(rollupJobStatus == null);
        }, 60, TimeUnit.SECONDS);

        // And that we don't see it in the GetJobs API
        GetRollupJobsAction.Request getRequest = new GetRollupJobsAction.Request("job1");
        GetRollupJobsAction.Response getResponse = client().execute(GetRollupJobsAction.INSTANCE, getRequest).get();
        Assert.assertThat(getResponse.getJobs().size(), equalTo(0));

        // But make sure the other job is still there
        getRequest = new GetRollupJobsAction.Request("job2");
        getResponse = client().execute(GetRollupJobsAction.INSTANCE, getRequest).get();
        Assert.assertThat(getResponse.getJobs().size(), equalTo(1));
        Assert.assertThat(getResponse.getJobs().get(0).getJob().getId(), equalTo("job2"));

        // and still STOPPED
        ESTestCase.assertBusy(() -> {
            RollupJobStatus rollupJobStatus = getRollupJobStatus("job2");

            IndexerState state = rollupJobStatus.getIndexerState();
            assertTrue(state.equals(IndexerState.STOPPED));
        }, 60, TimeUnit.SECONDS);
    }

    public void testBig() throws Exception {

        client().admin().indices().prepareCreate("test-big")
                .addMapping("test-big", "{\"test-big\": {\"properties\": {\"timestamp\": {\"type\": \"date\"}, " +
                    "\"thefield\": {\"type\": \"integer\"}}}}", XContentType.JSON)
                .setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).get();
        client().admin().cluster().prepareHealth("test-big").setWaitForYellowStatus().get();

        client().admin().indices().prepareCreate("test-verify")
                .addMapping("test-big", "{\"test-big\": {\"properties\": {\"timestamp\": {\"type\": \"date\"}, " +
                        "\"thefield\": {\"type\": \"integer\"}}}}", XContentType.JSON)
                .setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).get();
        client().admin().cluster().prepareHealth("test-verify").setWaitForYellowStatus().get();

        BulkRequestBuilder bulk = client().prepareBulk();
        Map<String, Object> source = new HashMap<>(3);

        int numDays = 90;
        int numDocsPerDay = 100;

        for (int i = 0; i < numDays; i++) {
            DateTime ts = new DateTime().minusDays(i);
            for (int j = 0; j < numDocsPerDay; j++) {

                int value = ESTestCase.randomIntBetween(0,100);
                source.put("timestamp", ts.toString());
                source.put("thefield", value);
                bulk.add(new IndexRequest("test-big", "test-big").source(source));
                bulk.add(new IndexRequest("test-verify", "test-big").source(source));
                source.clear();
            }

            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            bulk.get();
            bulk = client().prepareBulk();
            logger.info("Day: [" + i + "]: " + ts.toString() + " [" + ts.getMillis() + "]" );
        }


        client().admin().indices().prepareRefresh("test-big").get();
        client().admin().indices().prepareRefresh("test-verify").get();

        MetricConfig metricConfig = new MetricConfig.Builder()
                .setField("thefield")
                .setMetrics(Arrays.asList("sum", "min", "max", "avg"))
                .build();

        DateHistoGroupConfig.Builder datehistoGroupConfig = new DateHistoGroupConfig.Builder();
        datehistoGroupConfig.setField("timestamp");
        datehistoGroupConfig.setInterval(new DateHistogramInterval("1d"));

        GroupConfig.Builder groupConfig = new GroupConfig.Builder();
        groupConfig.setDateHisto(datehistoGroupConfig.build());

        RollupJobConfig.Builder config = new RollupJobConfig.Builder();
        config.setIndexPattern("test-big");
        config.setRollupIndex("rolled");
        config.setId(taskId);
        config.setGroupConfig(groupConfig.build());
        config.setMetricsConfig(Collections.singletonList(metricConfig));
        config.setCron("* * * * * ? *");
        config.setPageSize(1000);

        PutRollupJobAction.Request request = new PutRollupJobAction.Request();
        request.setConfig(config.build());
        client().execute(PutRollupJobAction.INSTANCE, request).get();

        StartRollupJobAction.Request startRequest = new StartRollupJobAction.Request(taskId);
        StartRollupJobAction.Response response = client().execute(StartRollupJobAction.INSTANCE, startRequest).get();
        Assert.assertThat(response.isStarted(), equalTo(true));

        ESTestCase.assertBusy(() -> {
            RollupJobStatus rollupJobStatus = getRollupJobStatus(taskId);
            if (rollupJobStatus == null) {
                fail("null");
            }

            IndexerState state = rollupJobStatus.getIndexerState();
            logger.error("state: [" + state + "]");
            assertTrue(state.equals(IndexerState.STARTED) && rollupJobStatus.getPosition() != null);
        }, 60, TimeUnit.SECONDS);

        RollupJobStatus rollupJobStatus = getRollupJobStatus(taskId);
        if (rollupJobStatus == null) {
            Assert.fail("rollup job status should not be null");
        }

        client().admin().indices().prepareRefresh("rolled").get();

        SearchResponse count = client().prepareSearch("rolled").setSize(10).get();
        // total document is numDays minus 1 because we don't build rollup for
        // buckets that are not full (bucket for the current day).
        Assert.assertThat(count.getHits().totalHits, equalTo(Long.valueOf(numDays-1)));

        if (ESTestCase.randomBoolean()) {
            client().admin().indices().prepareDelete("test-big").get();
            client().admin().indices().prepareRefresh().get();
        }

        // Execute the rollup search
        SearchRequest rollupRequest = new SearchRequest("rolled")
                .source(new SearchSourceBuilder()
                        .aggregation(dateHistogram("timestamp")
                                .interval(1000*86400)
                                .field("timestamp"))
                        .size(0));
        SearchResponse searchResponse = client().execute(RollupSearchAction.INSTANCE, rollupRequest).get();
        Assert.assertNotNull(searchResponse);

        // And a regular search against the verification index
        SearchRequest verifyRequest = new SearchRequest("test-verify")
                .source(new SearchSourceBuilder()
                        .aggregation(dateHistogram("timestamp")
                                .interval(1000*86400)
                                .field("timestamp"))
                        .size(0));
        SearchResponse verifyResponse = client().execute(SearchAction.INSTANCE, verifyRequest).get();

        Map<String, Aggregation> rollupAggs = searchResponse.getAggregations().asMap();

        for (Aggregation agg : verifyResponse.getAggregations().asList()) {
            Aggregation rollupAgg = rollupAggs.get(agg.getName());

            Assert.assertNotNull(rollupAgg);
            Assert.assertThat(rollupAgg.getType(), equalTo(agg.getType()));
            verifyAgg((InternalDateHistogram)agg, (InternalDateHistogram)rollupAgg);
        }

        // And a quick sanity check for doc type
        SearchRequest rollupRawRequest = new SearchRequest("rolled")
                .source(new SearchSourceBuilder().query(new MatchAllQueryBuilder())
                        .size(1));
        SearchResponse searchRawResponse = client().execute(SearchAction.INSTANCE, rollupRawRequest).get();
        Assert.assertNotNull(searchRawResponse);
        assertThat(searchRawResponse.getHits().getAt(0).getType(), equalTo("_doc"));
    }

    private void verifyAgg(InternalDateHistogram verify, InternalDateHistogram rollup) {
        for (int i = 0; i < rollup.getBuckets().size(); i++) {
            InternalDateHistogram.Bucket verifyBucket = verify.getBuckets().get(i);
            InternalDateHistogram.Bucket rollupBucket = rollup.getBuckets().get(i);
            Assert.assertThat(rollupBucket.getDocCount(), equalTo(verifyBucket.getDocCount()));
            Assert.assertThat(((DateTime)rollupBucket.getKey()).getMillis(), equalTo(((DateTime)verifyBucket.getKey()).getMillis()));
            Assert.assertTrue(rollupBucket.getAggregations().equals(verifyBucket.getAggregations()));
        }
    }

    private RollupJobStatus getRollupJobStatus(final String taskId) {
        final GetRollupJobsAction.Request request = new GetRollupJobsAction.Request(taskId);
        final GetRollupJobsAction.Response response = client().execute(GetRollupJobsAction.INSTANCE, request).actionGet();

        if (response.getJobs() != null && response.getJobs().isEmpty() == false) {
            assertThat("Expect 1 rollup job with id " + taskId, response.getJobs().size(), equalTo(1));
            return response.getJobs().iterator().next().getStatus();
        }
        return null;
    }

    @After
    public void cleanup() throws ExecutionException, InterruptedException {
        GetRollupJobsAction.Request getRequest = new GetRollupJobsAction.Request("_all");
        GetRollupJobsAction.Response response = client().execute(GetRollupJobsAction.INSTANCE, getRequest).get();

        for (GetRollupJobsAction.JobWrapper job : response.getJobs()) {
            StopRollupJobAction.Request stopRequest = new StopRollupJobAction.Request(job.getJob().getId());
            try {
                client().execute(StopRollupJobAction.INSTANCE, stopRequest).get();
            } catch (ElasticsearchException e) {
                //
            }

            DeleteRollupJobAction.Request deleteRequest = new DeleteRollupJobAction.Request(job.getJob().getId());
            client().execute(DeleteRollupJobAction.INSTANCE, deleteRequest).get();
        }
    }
}
