/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.admin.cluster.stats.CCSTelemetrySnapshot;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.usage.UsageService;
import org.junit.Assert;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.MRT_FEATURE;
import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.WILDCARD_FEATURE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class CCSUsageTelemetryIT extends AbstractMultiClustersTestCase {
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

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE1, true, REMOTE2, true);
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
     * Search on all remotes
     */
    public void testAllRemotesSearch() throws ExecutionException, InterruptedException {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchRequest searchRequest = makeSearchRequest(localIndex, "*:" + remoteIndex);
        boolean minimizeRoundtrips = searchRequest.isCcsMinimizeRoundtrips();

        String randomClient = randomAlphaOfLength(10);
        // We want to send search to a specific node (we don't care which one) so that we could
        // collect the CCS telemetry from it later
        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        // We don't care here too much about the response, we just want to trigger the telemetry collection.
        // So we check it's not null and leave the rest to other tests.
        assertResponse(
            cluster(LOCAL_CLUSTER).client(nodeName)
                .filterWithHeader(Map.of(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, randomClient))
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
        assertThat(telemetry.getSkippedRemotes(), equalTo(0L));
        assertThat(telemetry.getClientCounts().size(), equalTo(1));
        assertThat(telemetry.getClientCounts().get(randomClient), equalTo(1L));
        if (minimizeRoundtrips) {
            assertThat(telemetry.getFeatureCounts().get(MRT_FEATURE), equalTo(1L));
        } else {
            assertThat(telemetry.getFeatureCounts().get(MRT_FEATURE), equalTo(null));
        }
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
        assertThat(telemetry.getSkippedRemotes(), equalTo(0L));
        assertThat(telemetry.getClientCounts().size(), equalTo(1));
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
    public void testLocalSearch() throws ExecutionException, InterruptedException {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");

        SearchRequest searchRequest = makeSearchRequest(localIndex);
        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);
        CCSTelemetrySnapshot telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(0L));
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

        searchRequest = makeSearchRequest(localIndex, "*:" + remoteIndex);
        assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).search(searchRequest), Assert::assertNotNull);
        telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(4L));
        assertThat(telemetry.getFeatureCounts().get(WILDCARD_FEATURE), equalTo(2L));
    }

    // TODO: implement the following tests:
    // - failed search
    // - search with one remote skipped
    // - search with a remote failing
    // - async search
    // - various search failure reasons

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
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", localIndex);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", remoteIndex);
        clusterInfo.put("remote.skip_unavailable", true);
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
}
