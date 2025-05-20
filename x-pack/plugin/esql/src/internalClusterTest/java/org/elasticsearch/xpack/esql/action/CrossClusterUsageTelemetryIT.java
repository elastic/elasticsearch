/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.stats.CCSTelemetrySnapshot;
import org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SkipUnavailableRule;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.ASYNC_FEATURE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.deleteAsyncId;
import static org.hamcrest.Matchers.equalTo;

public class CrossClusterUsageTelemetryIT extends AbstractCrossClusterUsageTelemetryIT {
    private static final String INDEX_WITH_RUNTIME_MAPPING = "blocking";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(CrossClusterQueryIT.InternalExchangePlugin.class);
        plugins.add(SimplePauseFieldPlugin.class);
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class); // allows the async_search DELETE action
        return plugins;
    }

    @Before
    public void resetPlugin() {
        SimplePauseFieldPlugin.resetPlugin();
    }

    public void assertPerClusterCount(CCSTelemetrySnapshot.PerClusterCCSTelemetry perCluster, long count) {
        assertThat(perCluster.getCount(), equalTo(count));
        assertThat(perCluster.getSkippedCount(), equalTo(0L));
        assertThat(perCluster.getTook().count(), equalTo(count));
    }

    public void testLocalRemote() throws Exception {
        setupClusters();
        var telemetry = getTelemetryFromQuery("from logs-*,c*:logs-* | stats sum (v)", "kibana");

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        assertThat(telemetry.getFailureReasons().size(), equalTo(0));
        assertThat(telemetry.getTook().count(), equalTo(1L));
        assertThat(telemetry.getTookMrtFalse().count(), equalTo(0L));
        assertThat(telemetry.getTookMrtTrue().count(), equalTo(0L));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        assertThat(telemetry.getClientCounts().size(), equalTo(1));
        assertThat(telemetry.getClientCounts().get("kibana"), equalTo(1L));
        assertThat(telemetry.getFeatureCounts().get(ASYNC_FEATURE), equalTo(null));

        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        for (String clusterAlias : remoteClusterAlias()) {
            assertPerClusterCount(perCluster.get(clusterAlias), 1L);
        }
        assertPerClusterCount(perCluster.get(LOCAL_CLUSTER), 1L);

        telemetry = getTelemetryFromQuery("from logs-*,c*:logs-* | stats sum (v)", "kibana");
        assertThat(telemetry.getTotalCount(), equalTo(2L));
        assertThat(telemetry.getClientCounts().get("kibana"), equalTo(2L));
        perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        for (String clusterAlias : remoteClusterAlias()) {
            assertPerClusterCount(perCluster.get(clusterAlias), 2L);
        }
        assertPerClusterCount(perCluster.get(LOCAL_CLUSTER), 2L);
    }

    public void testLocalOnly() throws Exception {
        setupClusters();
        // Should not produce any usage info since it's a local search
        var telemetry = getTelemetryFromQuery("from logs-* | stats sum (v)", "kibana");

        assertThat(telemetry.getTotalCount(), equalTo(0L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));
    }

    @SkipUnavailableRule.NotSkipped(aliases = REMOTE1)
    public void testFailed() throws Exception {
        setupClusters();
        // Should not produce any usage info since it's a local search
        var telemetry = getTelemetryFromFailedQuery("from no_such_index | stats sum (v)");

        assertThat(telemetry.getTotalCount(), equalTo(0L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));

        // Errors from both remotes
        telemetry = getTelemetryFromFailedQuery("from logs-*,c*:no_such_index | stats sum (v)");

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        Map<String, Long> expectedFailure = Map.of(CCSUsageTelemetry.Result.NOT_FOUND.getName(), 1L);
        assertThat(telemetry.getFailureReasons(), equalTo(expectedFailure));

        // this is only for cluster-a so no skipped remotes
        telemetry = getTelemetryFromFailedQuery("from logs-*,cluster-a:no_such_index | stats sum (v)");
        assertThat(telemetry.getTotalCount(), equalTo(2L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        expectedFailure = Map.of(CCSUsageTelemetry.Result.NOT_FOUND.getName(), 2L);
        assertThat(telemetry.getFailureReasons(), equalTo(expectedFailure));
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));
    }

    public void testRemoteOnly() throws Exception {
        setupClusters();
        var telemetry = getTelemetryFromQuery("from c*:logs-* | stats sum (v)", "kibana");

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        assertThat(telemetry.getFailureReasons().size(), equalTo(0));
        assertThat(telemetry.getTook().count(), equalTo(1L));
        assertThat(telemetry.getTookMrtFalse().count(), equalTo(0L));
        assertThat(telemetry.getTookMrtTrue().count(), equalTo(0L));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        assertThat(telemetry.getClientCounts().size(), equalTo(1));
        assertThat(telemetry.getClientCounts().get("kibana"), equalTo(1L));
        assertThat(telemetry.getFeatureCounts().get(ASYNC_FEATURE), equalTo(null));

        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(2));
        for (String clusterAlias : remoteClusterAlias()) {
            assertPerClusterCount(perCluster.get(clusterAlias), 1L);
        }
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(2));
    }

    public void testAsync() throws Exception {
        setupClusters();
        var telemetry = getTelemetryFromAsyncQuery("from logs-*,c*:logs-* | stats sum (v)");

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        assertThat(telemetry.getFailureReasons().size(), equalTo(0));
        assertThat(telemetry.getTook().count(), equalTo(1L));
        assertThat(telemetry.getTookMrtFalse().count(), equalTo(0L));
        assertThat(telemetry.getTookMrtTrue().count(), equalTo(0L));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        assertThat(telemetry.getClientCounts().size(), equalTo(0));
        assertThat(telemetry.getFeatureCounts().get(ASYNC_FEATURE), equalTo(1L));

        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        for (String clusterAlias : remoteClusterAlias()) {
            assertPerClusterCount(perCluster.get(clusterAlias), 1L);
        }
        assertPerClusterCount(perCluster.get(LOCAL_CLUSTER), 1L);

        // do it again
        telemetry = getTelemetryFromAsyncQuery("from logs-*,c*:logs-* |  stats sum (v)");
        assertThat(telemetry.getTotalCount(), equalTo(2L));
        assertThat(telemetry.getFeatureCounts().get(ASYNC_FEATURE), equalTo(2L));
        perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        for (String clusterAlias : remoteClusterAlias()) {
            assertPerClusterCount(perCluster.get(clusterAlias), 2L);
        }
        assertPerClusterCount(perCluster.get(LOCAL_CLUSTER), 2L);
    }

    public void testAsyncStop() throws Exception {
        setupClusters();
        populateRuntimeIndex(REMOTE1, "pause", INDEX_WITH_RUNTIME_MAPPING);
        populateRuntimeIndex(REMOTE2, "pause", INDEX_WITH_RUNTIME_MAPPING);

        EsqlQueryRequest request = EsqlQueryRequest.asyncEsqlQueryRequest();
        request.query("from logs-*,c*:logs-*,c*:blocking | eval v1=coalesce(const, v) | stats sum (v1)");
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.columnar(randomBoolean());
        request.includeCCSMetadata(randomBoolean());

        AtomicReference<String> asyncExecutionId = new AtomicReference<>();
        assertResponse(cluster(LOCAL_CLUSTER).client(queryNode).execute(EsqlQueryAction.INSTANCE, request), resp -> {
            if (resp.isRunning()) {
                assertNotNull("async execution id is null", resp.asyncExecutionId());
                asyncExecutionId.set(resp.asyncExecutionId().get());
            }
        });
        SimplePauseFieldPlugin.startEmitting.await(30, TimeUnit.SECONDS);
        AsyncStopRequest stopRequest = new AsyncStopRequest(asyncExecutionId.get());
        ActionFuture<EsqlQueryResponse> actionFuture = cluster(LOCAL_CLUSTER).client(queryNode)
            .execute(EsqlAsyncStopAction.INSTANCE, stopRequest);
        // Release the pause
        SimplePauseFieldPlugin.allowEmitting.countDown();
        try (EsqlQueryResponse resp = actionFuture.actionGet(30, TimeUnit.SECONDS)) {
            assertTrue(resp.getExecutionInfo().isPartial());

            CCSTelemetrySnapshot telemetry = getTelemetrySnapshot(queryNode);

            assertThat(telemetry.getTotalCount(), equalTo(1L));
            assertThat(telemetry.getSuccessCount(), equalTo(1L));
            assertThat(telemetry.getFailureReasons().size(), equalTo(0));
            assertThat(telemetry.getTook().count(), equalTo(1L));
            assertThat(telemetry.getTookMrtFalse().count(), equalTo(0L));
            assertThat(telemetry.getTookMrtTrue().count(), equalTo(0L));
            assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
            assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
            assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
            assertThat(telemetry.getClientCounts().size(), equalTo(0));
            assertThat(telemetry.getFeatureCounts().get(ASYNC_FEATURE), equalTo(1L));

            var perCluster = telemetry.getByRemoteCluster();
            assertThat(perCluster.size(), equalTo(3));
            for (String clusterAlias : remoteClusterAlias()) {
                assertPerClusterCount(perCluster.get(clusterAlias), 1L);
            }
            assertPerClusterCount(perCluster.get(LOCAL_CLUSTER), 1L);
        } finally {
            // Clean up
            assertAcked(deleteAsyncId(client(), asyncExecutionId.get()));
        }
    }

    public void testNoSuchCluster() throws Exception {
        setupClusters();
        // This is not recognized as a cross-cluster search
        var telemetry = getTelemetryFromFailedQuery("from c*:logs*, nocluster:nomatch | stats sum (v)");

        assertThat(telemetry.getTotalCount(), equalTo(0L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));
    }

    @SkipUnavailableRule.NotSkipped(aliases = REMOTE1)
    public void testDisconnect() throws Exception {
        setupClusters();
        // Disconnect remote1
        cluster(REMOTE1).close();
        var telemetry = getTelemetryFromFailedQuery("from logs-*,cluster-a:logs-* | stats sum (v)");

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        Map<String, Long> expectedFailure = Map.of(CCSUsageTelemetry.Result.REMOTES_UNAVAILABLE.getName(), 1L);
        assertThat(telemetry.getFailureReasons(), equalTo(expectedFailure));
    }

    void populateRuntimeIndex(String clusterAlias, String langName, String indexName) throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("const");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", langName).endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        mapping.endObject();
        client(clusterAlias).admin().indices().prepareCreate(indexName).setMapping(mapping).get();
        BulkRequestBuilder bulk = client(clusterAlias).prepareBulk(indexName).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 10; i++) {
            bulk.add(new IndexRequest().source("foo", i));
        }
        bulk.get();
    }

}
