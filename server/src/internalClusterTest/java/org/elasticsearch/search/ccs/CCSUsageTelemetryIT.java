/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.ccs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.stats.CCSTelemetrySnapshot;
import org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.Result;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.SlowRunningQueryBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.usage.UsageService;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.ASYNC_FEATURE;
import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.MRT_FEATURE;
import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.WILDCARD_FEATURE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class CCSUsageTelemetryIT extends AbstractMultiClustersTestCase {
    private static final Logger LOGGER = LogManager.getLogger(CCSUsageTelemetryIT.class);
    private static final String REMOTE1 = "cluster-a";
    private static final String REMOTE2 = "cluster-b";

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE1, REMOTE2);
    }

    @Rule
    public SkipUnavailableRule skipOverride = new SkipUnavailableRule(REMOTE1, REMOTE2);

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        var map = skipOverride.getMap();
        LOGGER.info("Using skip_unavailable map: [{}]", map);
        return map;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), CrossClusterSearchIT.TestQueryBuilderPlugin.class);
    }

    private SearchRequest makeSearchRequest(String... indices) {
        SearchRequest searchRequest = new SearchRequest(indices);
        searchRequest.allowPartialSearchResults(false);
        searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));
        return searchRequest;
    }

    /**
    * Run search request and get telemetry from it
    */
    private CCSTelemetrySnapshot getTelemetryFromSearch(SearchRequest searchRequest) throws ExecutionException, InterruptedException {
        // We want to send search to a specific node (we don't care which one) so that we could
        // collect the CCS telemetry from it later
        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        // We don't care here too much about the response, we just want to trigger the telemetry collection.
        // So we check it's not null and leave the rest to other tests.
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);
        return getTelemetrySnapshot(nodeName);
    }

    private CCSTelemetrySnapshot getTelemetryFromFailedSearch(SearchRequest searchRequest) throws Exception {
        // We want to send search to a specific node (we don't care which one) so that we could
        // collect the CCS telemetry from it later
        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();
        cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest, queryFuture);
        assertBusy(() -> assertTrue(queryFuture.isDone()));

        // We expect failure, but we don't care too much which failure it is in this test
        ExecutionException ee = expectThrows(ExecutionException.class, queryFuture::get);
        assertNotNull(ee.getCause());

        return getTelemetrySnapshot(nodeName);
    }

    /**
     * Create search request for indices and get telemetry from it
     */
    private CCSTelemetrySnapshot getTelemetryFromSearch(String... indices) throws ExecutionException, InterruptedException {
        return getTelemetryFromSearch(makeSearchRequest(indices));
    }

    /**
     * Search on all remotes
     */
    public void testAllRemotesSearch() throws ExecutionException, InterruptedException {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchRequest searchRequest = makeSearchRequest(localIndex, "*:" + remoteIndex);
        boolean minimizeRoundtrips = TransportSearchAction.shouldMinimizeRoundtrips(searchRequest);

        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        assertResponse(
            cluster(LOCAL_CLUSTER).client(nodeName)
                .filterWithHeader(Map.of(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "kibana"))
                .search(searchRequest),
            Assert::assertNotNull
        );
        CCSTelemetrySnapshot telemetry = getTelemetrySnapshot(nodeName);

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        assertThat(telemetry.getFailureReasons().size(), equalTo(0));
        assertThat(telemetry.getTook().count(), equalTo(1L));
        assertThat(telemetry.getTookMrtTrue().count(), equalTo(minimizeRoundtrips ? 1L : 0L));
        assertThat(telemetry.getTookMrtFalse().count(), equalTo(minimizeRoundtrips ? 0L : 1L));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        assertThat(telemetry.getClientCounts().size(), equalTo(1));
        assertThat(telemetry.getClientCounts().get("kibana"), equalTo(1L));
        if (minimizeRoundtrips) {
            assertThat(telemetry.getFeatureCounts().get(MRT_FEATURE), equalTo(1L));
        } else {
            assertThat(telemetry.getFeatureCounts().get(MRT_FEATURE), equalTo(null));
        }
        assertThat(telemetry.getFeatureCounts().get(ASYNC_FEATURE), equalTo(null));

        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        for (String clusterAlias : remoteClusterAlias()) {
            var clusterTelemetry = perCluster.get(clusterAlias);
            assertThat(clusterTelemetry.getCount(), equalTo(1L));
            assertThat(clusterTelemetry.getSkippedCount(), equalTo(0L));
            assertThat(clusterTelemetry.getTook().count(), equalTo(1L));
        }

        // another search
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);
        telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(2L));
        assertThat(telemetry.getSuccessCount(), equalTo(2L));
        assertThat(telemetry.getFailureReasons().size(), equalTo(0));
        assertThat(telemetry.getTook().count(), equalTo(2L));
        assertThat(telemetry.getTookMrtTrue().count(), equalTo(minimizeRoundtrips ? 2L : 0L));
        assertThat(telemetry.getTookMrtFalse().count(), equalTo(minimizeRoundtrips ? 0L : 2L));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        assertThat(telemetry.getClientCounts().size(), equalTo(1));
        assertThat(telemetry.getClientCounts().get("kibana"), equalTo(1L));
        perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        for (String clusterAlias : remoteClusterAlias()) {
            var clusterTelemetry = perCluster.get(clusterAlias);
            assertThat(clusterTelemetry.getCount(), equalTo(2L));
            assertThat(clusterTelemetry.getSkippedCount(), equalTo(0L));
            assertThat(clusterTelemetry.getTook().count(), equalTo(2L));
        }
    }

    /**
     * Search on a specific remote
     */
    public void testOneRemoteSearch() throws ExecutionException, InterruptedException {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        // Make request to cluster a
        SearchRequest searchRequest = makeSearchRequest(localIndex, REMOTE1 + ":" + remoteIndex);
        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);
        CCSTelemetrySnapshot telemetry = getTelemetrySnapshot(nodeName);
        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(2));
        assertThat(perCluster.get(REMOTE1).getCount(), equalTo(1L));
        assertThat(perCluster.get(REMOTE1).getTook().count(), equalTo(1L));
        assertThat(perCluster.get(REMOTE2), equalTo(null));
        assertThat(telemetry.getClientCounts().size(), equalTo(0));

        // Make request to cluster b
        searchRequest = makeSearchRequest(localIndex, REMOTE2 + ":" + remoteIndex);
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);
        telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(2L));
        assertThat(telemetry.getSuccessCount(), equalTo(2L));
        perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        assertThat(perCluster.get(REMOTE1).getCount(), equalTo(1L));
        assertThat(perCluster.get(REMOTE1).getTook().count(), equalTo(1L));
        assertThat(perCluster.get(REMOTE2).getCount(), equalTo(1L));
        assertThat(perCluster.get(REMOTE2).getTook().count(), equalTo(1L));
    }

    /**
     * Local search should not produce any telemetry at all
     */
    public void testLocalOnlySearch() throws ExecutionException, InterruptedException {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");

        CCSTelemetrySnapshot telemetry = getTelemetryFromSearch(localIndex);
        assertThat(telemetry.getTotalCount(), equalTo(0L));
    }

    /**
    * Search on remotes only, without local index
    */
    public void testRemoteOnlySearch() throws ExecutionException, InterruptedException {
        Map<String, Object> testClusterInfo = setupClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        CCSTelemetrySnapshot telemetry = getTelemetryFromSearch("*:" + remoteIndex);
        var perCluster = telemetry.getByRemoteCluster();
        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        assertThat(telemetry.getFailureReasons().size(), equalTo(0));
        assertThat(telemetry.getTook().count(), equalTo(1L));
        assertThat(perCluster.size(), equalTo(2));
        assertThat(telemetry.getClientCounts().size(), equalTo(0));
        assertThat(perCluster.get(REMOTE1).getCount(), equalTo(1L));
        assertThat(perCluster.get(REMOTE1).getSkippedCount(), equalTo(0L));
        assertThat(perCluster.get(REMOTE1).getTook().count(), equalTo(1L));
        assertThat(perCluster.get(REMOTE2).getCount(), equalTo(1L));
        assertThat(perCluster.get(REMOTE2).getSkippedCount(), equalTo(0L));
        assertThat(perCluster.get(REMOTE2).getTook().count(), equalTo(1L));
    }

    /**
     * Count wildcard searches. Only wildcards in index names (not in cluster names) are counted.
     */
    public void testWildcardSearch() throws ExecutionException, InterruptedException {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchRequest searchRequest = makeSearchRequest(localIndex, "*:" + remoteIndex);
        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);
        CCSTelemetrySnapshot telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getFeatureCounts().get(WILDCARD_FEATURE), equalTo(null));

        searchRequest = makeSearchRequest("*", REMOTE1 + ":" + remoteIndex);
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);
        telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(2L));
        assertThat(telemetry.getFeatureCounts().get(WILDCARD_FEATURE), equalTo(1L));

        searchRequest = makeSearchRequest(localIndex, REMOTE2 + ":*");
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);
        telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(3L));
        assertThat(telemetry.getFeatureCounts().get(WILDCARD_FEATURE), equalTo(2L));

        // Wildcards in cluster name do not count
        searchRequest = makeSearchRequest(localIndex, "*:" + remoteIndex);
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);
        telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(4L));
        assertThat(telemetry.getFeatureCounts().get(WILDCARD_FEATURE), equalTo(2L));

        // Wildcard in the middle of the index name counts
        searchRequest = makeSearchRequest(localIndex, REMOTE2 + ":rem*");
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);
        telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(5L));
        assertThat(telemetry.getFeatureCounts().get(WILDCARD_FEATURE), equalTo(3L));

        // Wildcard only counted once per search
        searchRequest = makeSearchRequest("*", REMOTE1 + ":rem*", REMOTE2 + ":remote*");
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);
        telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(6L));
        assertThat(telemetry.getFeatureCounts().get(WILDCARD_FEATURE), equalTo(4L));
    }

    /**
     * Test complete search failure
     */
    public void testFailedSearch() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchRequest searchRequest = makeSearchRequest(localIndex, "*:" + remoteIndex);
        // shardId -1 means to throw the Exception on all shards, so should result in complete search failure
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), -1);
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));
        searchRequest.allowPartialSearchResults(true);

        CCSTelemetrySnapshot telemetry = getTelemetryFromFailedSearch(searchRequest);
        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getTook().count(), equalTo(0L));
        assertThat(telemetry.getTookMrtTrue().count(), equalTo(0L));
        assertThat(telemetry.getTookMrtFalse().count(), equalTo(0L));
        Map<String, Long> expectedFailures = Map.of(Result.UNKNOWN.getName(), 1L);
        assertThat(telemetry.getFailureReasons(), equalTo(expectedFailures));
    }

    /**
     * Search when all the remotes failed and skipped
     */
    public void testSkippedAllRemotesSearch() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchRequest searchRequest = makeSearchRequest(localIndex, "*:" + remoteIndex);
        // throw Exception on all shards of remoteIndex, but not against localIndex
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(
            randomLong(),
            new IllegalStateException("index corrupted"),
            remoteIndex
        );
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));
        searchRequest.allowPartialSearchResults(true);

        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);

        CCSTelemetrySnapshot telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        // Note that this counts how many searches had skipped remotes, not how many remotes are skipped
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(1L));
        // Still count the remote that failed
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getTook().count(), equalTo(1L));
        // Each remote will have its skipped count bumped
        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        for (String remote : remoteClusterAlias()) {
            assertThat(perCluster.get(remote).getCount(), equalTo(0L));
            assertThat(perCluster.get(remote).getSkippedCount(), equalTo(1L));
            assertThat(perCluster.get(remote).getTook().count(), equalTo(0L));
        }
    }

    public void testSkippedOneRemoteSearch() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        // Remote1 will fail, Remote2 will just do nothing but it counts as success
        SearchRequest searchRequest = makeSearchRequest(localIndex, REMOTE1 + ":" + remoteIndex, REMOTE2 + ":" + "nosuchindex*");
        // throw Exception on all shards of remoteIndex, but not against localIndex
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(
            randomLong(),
            new IllegalStateException("index corrupted"),
            remoteIndex
        );
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));
        searchRequest.allowPartialSearchResults(true);

        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);

        CCSTelemetrySnapshot telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        // Note that this counts how many searches had skipped remotes, not how many remotes are skipped
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(1L));
        // Still count the remote that failed
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getTook().count(), equalTo(1L));
        // Each remote will have its skipped count bumped
        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        // This one is skipped
        assertThat(perCluster.get(REMOTE1).getCount(), equalTo(0L));
        assertThat(perCluster.get(REMOTE1).getSkippedCount(), equalTo(1L));
        assertThat(perCluster.get(REMOTE1).getTook().count(), equalTo(0L));
        // This one is OK
        assertThat(perCluster.get(REMOTE2).getCount(), equalTo(1L));
        assertThat(perCluster.get(REMOTE2).getSkippedCount(), equalTo(0L));
        assertThat(perCluster.get(REMOTE2).getTook().count(), equalTo(1L));
    }

    /**
     * Test what happens if remote times out - it should be skipped
     */
    public void testRemoteTimesOut() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchRequest searchRequest = makeSearchRequest(localIndex, REMOTE1 + ":" + remoteIndex);
        // This works only with minimize_roundtrips enabled, since otherwise timed out shards will be counted as
        // partial failure, and we disable partial results..
        searchRequest.setCcsMinimizeRoundtrips(true);

        TimeValue searchTimeout = new TimeValue(200, TimeUnit.MILLISECONDS);
        // query builder that will sleep for the specified amount of time in the query phase
        SlowRunningQueryBuilder slowRunningQueryBuilder = new SlowRunningQueryBuilder(searchTimeout.millis() * 5, remoteIndex);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(slowRunningQueryBuilder).timeout(searchTimeout);
        searchRequest.source(sourceBuilder);

        CCSTelemetrySnapshot telemetry = getTelemetryFromSearch(searchRequest);
        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(1L));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(1L));
        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(2));
        assertThat(perCluster.get(REMOTE1).getCount(), equalTo(0L));
        assertThat(perCluster.get(REMOTE1).getSkippedCount(), equalTo(1L));
        assertThat(perCluster.get(REMOTE1).getTook().count(), equalTo(0L));
        assertThat(perCluster.get(REMOTE2), equalTo(null));
    }

    /**
    * Test what happens if remote times out and there's no local - it should be skipped
    */
    public void testRemoteOnlyTimesOut() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchRequest searchRequest = makeSearchRequest(REMOTE1 + ":" + remoteIndex);
        // This works only with minimize_roundtrips enabled, since otherwise timed out shards will be counted as
        // partial failure, and we disable partial results...
        searchRequest.setCcsMinimizeRoundtrips(true);

        TimeValue searchTimeout = new TimeValue(100, TimeUnit.MILLISECONDS);
        // query builder that will sleep for the specified amount of time in the query phase
        SlowRunningQueryBuilder slowRunningQueryBuilder = new SlowRunningQueryBuilder(searchTimeout.millis() * 5, remoteIndex);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(slowRunningQueryBuilder).timeout(searchTimeout);
        searchRequest.source(sourceBuilder);

        CCSTelemetrySnapshot telemetry = getTelemetryFromSearch(searchRequest);
        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(1L));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(1L));
        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(1));
        assertThat(perCluster.get(REMOTE1).getCount(), equalTo(0L));
        assertThat(perCluster.get(REMOTE1).getSkippedCount(), equalTo(1L));
        assertThat(perCluster.get(REMOTE1).getTook().count(), equalTo(0L));
        assertThat(perCluster.get(REMOTE2), equalTo(null));
    }

    @SkipOverride(aliases = { REMOTE1 })
    public void testRemoteTimesOutFailure() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchRequest searchRequest = makeSearchRequest(REMOTE1 + ":" + remoteIndex);

        TimeValue searchTimeout = new TimeValue(100, TimeUnit.MILLISECONDS);
        // query builder that will sleep for the specified amount of time in the query phase
        SlowRunningQueryBuilder slowRunningQueryBuilder = new SlowRunningQueryBuilder(searchTimeout.millis() * 5, remoteIndex);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(slowRunningQueryBuilder).timeout(searchTimeout);
        searchRequest.source(sourceBuilder);

        CCSTelemetrySnapshot telemetry = getTelemetryFromFailedSearch(searchRequest);
        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        // Failure is not skipping
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        // Still count the remote that failed
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(1L));
        assertThat(telemetry.getTook().count(), equalTo(0L));
        Map<String, Long> expectedFailure = Map.of(Result.TIMEOUT.getName(), 1L);
        assertThat(telemetry.getFailureReasons(), equalTo(expectedFailure));
        // No per-cluster data on total failure
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));
    }

    /**
    * Search when all the remotes failed and not skipped
    */
    @SkipOverride(aliases = { REMOTE1, REMOTE2 })
    public void testFailedAllRemotesSearch() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchRequest searchRequest = makeSearchRequest(localIndex, "*:" + remoteIndex);
        // throw Exception on all shards of remoteIndex, but not against localIndex
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(
            randomLong(),
            new IllegalStateException("index corrupted"),
            remoteIndex
        );
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));

        CCSTelemetrySnapshot telemetry = getTelemetryFromFailedSearch(searchRequest);
        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        // Failure is not skipping
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        // Still count the remote that failed
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getTook().count(), equalTo(0L));
        Map<String, Long> expectedFailure = Map.of(Result.REMOTES_UNAVAILABLE.getName(), 1L);
        assertThat(telemetry.getFailureReasons(), equalTo(expectedFailure));
        // No per-cluster data on total failure
        assertThat(telemetry.getByRemoteCluster().size(), equalTo(0));
    }

    /**
     * Test that we're still counting remote search even if remote cluster has no such index
     */
    public void testRemoteHasNoIndex() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");

        CCSTelemetrySnapshot telemetry = getTelemetryFromSearch(localIndex, REMOTE1 + ":" + "no_such_index*");
        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(2));
        assertThat(perCluster.get(REMOTE1).getCount(), equalTo(1L));
        assertThat(perCluster.get(REMOTE1).getTook().count(), equalTo(1L));
        assertThat(perCluster.get(REMOTE2), equalTo(null));
    }

    /**
     * Test that we're still counting remote search even if remote cluster has no such index
     */
    @SkipOverride(aliases = { REMOTE1 })
    public void testRemoteHasNoIndexFailure() throws Exception {
        SearchRequest searchRequest = makeSearchRequest(REMOTE1 + ":no_such_index");
        CCSTelemetrySnapshot telemetry = getTelemetryFromFailedSearch(searchRequest);
        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(0));
        Map<String, Long> expectedFailure = Map.of(Result.NOT_FOUND.getName(), 1L);
        assertThat(telemetry.getFailureReasons(), equalTo(expectedFailure));
    }

    public void testPITSearch() throws ExecutionException, InterruptedException {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        OpenPointInTimeRequest openPITRequest = new OpenPointInTimeRequest(localIndex, "*:" + remoteIndex).keepAlive(
            TimeValue.timeValueMinutes(5)
        );
        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        var client = cluster(LOCAL_CLUSTER).client(nodeName);
        BytesReference pitID = client.execute(TransportOpenPointInTimeAction.TYPE, openPITRequest).actionGet().getPointInTimeId();
        SearchRequest searchRequest = new SearchRequest().source(
            new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitID).setKeepAlive(TimeValue.timeValueMinutes(5)))
                .sort("@timestamp")
                .size(10)
        );
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());

        assertResponse(client.search(searchRequest), Assert::assertNotNull);
        // do it again
        assertResponse(client.search(searchRequest), Assert::assertNotNull);
        client.execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitID)).actionGet();
        CCSTelemetrySnapshot telemetry = getTelemetrySnapshot(nodeName);

        assertThat(telemetry.getTotalCount(), equalTo(2L));
        assertThat(telemetry.getSuccessCount(), equalTo(2L));
    }

    private CCSTelemetrySnapshot getTelemetrySnapshot(String nodeName) {
        var usage = cluster(LOCAL_CLUSTER).getInstance(UsageService.class, nodeName);
        return usage.getCcsUsageHolder().getCCSTelemetrySnapshot();
    }

    private Map<String, Object> setupClusters() {
        String localIndex = "demo";
        int numShardsLocal = randomIntBetween(2, 10);
        Settings localSettings = indexSettings(numShardsLocal, randomIntBetween(0, 1)).build();
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate(localIndex)
                .setSettings(localSettings)
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        indexDocs(client(LOCAL_CLUSTER), localIndex);

        String remoteIndex = "prod";
        int numShardsRemote = randomIntBetween(2, 10);
        for (String clusterAlias : remoteClusterAlias()) {
            final InternalTestCluster remoteCluster = cluster(clusterAlias);
            remoteCluster.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
            assertAcked(
                client(clusterAlias).admin()
                    .indices()
                    .prepareCreate(remoteIndex)
                    .setSettings(indexSettings(numShardsRemote, randomIntBetween(0, 1)))
                    .setMapping("@timestamp", "type=date", "f", "type=text")
            );
            assertFalse(
                client(clusterAlias).admin()
                    .cluster()
                    .prepareHealth(remoteIndex)
                    .setWaitForYellowStatus()
                    .setTimeout(TimeValue.timeValueSeconds(10))
                    .get()
                    .isTimedOut()
            );
            indexDocs(client(clusterAlias), remoteIndex);
        }

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.index", localIndex);
        clusterInfo.put("remote.index", remoteIndex);
        return clusterInfo;
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(5, 20);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("f", "v", "@timestamp", randomNonNegativeLong()).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    /**
     * Annotation to mark specific cluster in a test as not to be skipped when unavailable
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface SkipOverride {
        String[] aliases();
    }

    /**
     * Test rule to process skip annotations
     */
    static class SkipUnavailableRule implements TestRule {
        private final Map<String, Boolean> skipMap;

        SkipUnavailableRule(String... clusterAliases) {
            this.skipMap = Arrays.stream(clusterAliases).collect(Collectors.toMap(Function.identity(), alias -> true));
        }

        public Map<String, Boolean> getMap() {
            return skipMap;
        }

        @Override
        public Statement apply(Statement base, Description description) {
            // Check for annotation named "SkipOverride" and set the overrides accordingly
            var aliases = description.getAnnotation(SkipOverride.class);
            if (aliases != null) {
                for (String alias : aliases.aliases()) {
                    skipMap.put(alias, false);
                }
            }
            return base;
        }

    }
}
