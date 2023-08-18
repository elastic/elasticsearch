/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class CrossClusterSearchIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "cluster_a";
    private static long EARLIEST_TIMESTAMP = 1691348810000L;
    private static long LATEST_TIMESTAMP = 1691348820000L;

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, randomBoolean());
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugs = Arrays.asList(TestQueryBuilderPlugin.class);
        return Stream.concat(super.nodePlugins(clusterAlias).stream(), plugs.stream()).collect(Collectors.toList());
    }

    public static class TestQueryBuilderPlugin extends Plugin implements SearchPlugin {
        public TestQueryBuilderPlugin() {}

        @Override
        public List<QuerySpec<?>> getQueries() {
            QuerySpec<ThrowingQueryBuilder> throwingSpec = new QuerySpec<>(ThrowingQueryBuilder.NAME, ThrowingQueryBuilder::new, p -> {
                throw new IllegalStateException("not implemented");
            });

            return List.of(throwingSpec);
        }
    }

    public void testClusterDetailsAfterSuccessfulCCS() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();
        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(false);
        boolean minimizeRoundtrips = true;  // TODO: support MRT=false
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);

        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));
        client(LOCAL_CLUSTER).search(searchRequest, queryFuture);

        assertBusy(() -> assertTrue(queryFuture.isDone()));

        SearchResponse searchResponse = queryFuture.get();
        assertNotNull(searchResponse);

        SearchResponse.Clusters clusters = searchResponse.getClusters();
        assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
        assertThat(clusters.getTotal(), equalTo(2));
        assertThat(clusters.getSuccessful(), equalTo(2));
        assertThat(clusters.getSkipped(), equalTo(0));

        SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
        assertNotNull(localClusterSearchInfo);
        assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(localClusterSearchInfo.getIndexExpression(), equalTo(localIndex));
        assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
        assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
        assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
        assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
        assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
        assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

        SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
        assertNotNull(remoteClusterSearchInfo);
        assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(remoteClusterSearchInfo.getIndexExpression(), equalTo(remoteIndex));
        assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
    }

    // CCS with a search where the timestamp of the query cannot match so should be SUCCESSFUL with all shards skipped
    // during can-match
    public void testCCSClusterDetailsWhereAllShardsSkippedInCanMatch() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();
        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(false);
        boolean minimizeRoundtrips = true; // TODO support MRT=false
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("@timestamp").from(EARLIEST_TIMESTAMP - 2000)
            .to(EARLIEST_TIMESTAMP - 1000);

        searchRequest.source(new SearchSourceBuilder().query(rangeQueryBuilder).size(1000));
        client(LOCAL_CLUSTER).search(searchRequest, queryFuture);

        assertBusy(() -> assertTrue(queryFuture.isDone()));

        SearchResponse searchResponse = queryFuture.get();
        assertNotNull(searchResponse);

        SearchResponse.Clusters clusters = searchResponse.getClusters();
        assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
        assertThat(clusters.getTotal(), equalTo(2));
        assertThat(clusters.getSuccessful(), equalTo(2));
        assertThat(clusters.getSkipped(), equalTo(0));

        SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
        assertNotNull(localClusterSearchInfo);
        SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
        assertNotNull(remoteClusterSearchInfo);

        assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
        assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
        assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
        assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
        assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
        assertThat(localClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));

        assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));
    }

    public void testClusterDetailsAfterCCSWithFailuresOnOneShardOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();
        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(true);
        boolean minimizeRoundtrips = true; // TODO support MRT=false
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);

        // shardId 0 means to throw the Exception only on shard 0; all others should work
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), 0);
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));
        client(LOCAL_CLUSTER).search(searchRequest, queryFuture);

        assertBusy(() -> assertTrue(queryFuture.isDone()));

        SearchResponse searchResponse = queryFuture.get();
        assertNotNull(searchResponse);

        SearchResponse.Clusters clusters = searchResponse.getClusters();
        assertThat(clusters.getTotal(), equalTo(2));
        assertThat(clusters.getSuccessful(), equalTo(2));
        assertThat(clusters.getSkipped(), equalTo(0));

        SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
        assertNotNull(localClusterSearchInfo);
        assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
        assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
        assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards - 1));
        assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
        assertThat(localClusterSearchInfo.getFailedShards(), equalTo(1));
        assertThat(localClusterSearchInfo.getFailures().size(), equalTo(1));
        assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));
        ShardSearchFailure localShardSearchFailure = localClusterSearchInfo.getFailures().get(0);
        assertTrue("should have 'index corrupted' in reason", localShardSearchFailure.reason().contains("index corrupted"));

        SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
        assertNotNull(remoteClusterSearchInfo);
        assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
        assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards - 1));
        assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(1));
        assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
        assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
        assertTrue("should have 'index corrupted' in reason", remoteShardSearchFailure.reason().contains("index corrupted"));
    }

    public void testClusterDetailsAfterCCSWithFailuresOnRemoteClusterOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();
        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(true);
        boolean minimizeRoundtrips = true; // TODO support MRT=false
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);

        // throw Exception on all shards of remoteIndex, but not against localIndex
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(
            randomLong(),
            new IllegalStateException("index corrupted"),
            remoteIndex
        );
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));
        client(LOCAL_CLUSTER).search(searchRequest, queryFuture);

        assertBusy(() -> assertTrue(queryFuture.isDone()));

        if (skipUnavailable == false) {
            ExecutionException ee = expectThrows(ExecutionException.class, () -> queryFuture.get());
            assertNotNull(ee.getCause());
            assertThat(ee.getCause(), instanceOf(RemoteTransportException.class));
            Throwable rootCause = ExceptionsHelper.unwrap(ee.getCause(), IllegalStateException.class);
            assertThat(rootCause.getMessage(), containsString("index corrupted"));
        } else {
            SearchResponse searchResponse = queryFuture.get();
            assertNotNull(searchResponse);

            SearchResponse.Clusters clusters = searchResponse.getClusters();
            assertThat(clusters.isCcsMinimizeRoundtrips(), equalTo(minimizeRoundtrips));
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getSuccessful(), equalTo(1));
            assertThat(clusters.getSkipped(), equalTo(1));

            SearchResponse.Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).get();
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();

            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
            assertNull(remoteClusterSearchInfo.getTotalShards());
            assertNull(remoteClusterSearchInfo.getSuccessfulShards());
            assertNull(remoteClusterSearchInfo.getSkippedShards());
            assertNull(remoteClusterSearchInfo.getFailedShards());
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            assertNull(remoteClusterSearchInfo.getTook());
            assertFalse(remoteClusterSearchInfo.isTimedOut());
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue("should have 'index corrupted' in reason", remoteShardSearchFailure.reason().contains("index corrupted"));
        }
    }

    public void testRemoteClusterOnlyCCSSuccessfulResult() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();
        SearchRequest searchRequest = new SearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(false);
        boolean minimizeRoundtrips = true; // TODO support MRT=false
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));
        client(LOCAL_CLUSTER).search(searchRequest, queryFuture);

        assertBusy(() -> assertTrue(queryFuture.isDone()));

        SearchResponse searchResponse = queryFuture.get();
        assertNotNull(searchResponse);

        SearchResponse.Clusters clusters = searchResponse.getClusters();
        assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
        assertThat(clusters.getTotal(), equalTo(1));
        assertThat(clusters.getSuccessful(), equalTo(1));
        assertThat(clusters.getSkipped(), equalTo(0));

        assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

        SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
        assertNotNull(remoteClusterSearchInfo);
        assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
    }

    public void testRemoteClusterOnlyCCSWithFailuresOnOneShardOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();
        SearchRequest searchRequest = new SearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(true);
        boolean minimizeRoundtrips = true; // TODO support MRT=false
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);

        // shardId 0 means to throw the Exception only on shard 0; all others should work
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), 0);
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));
        client(LOCAL_CLUSTER).search(searchRequest, queryFuture);

        assertBusy(() -> assertTrue(queryFuture.isDone()));

        SearchResponse searchResponse = queryFuture.get();
        assertNotNull(searchResponse);

        SearchResponse.Clusters clusters = searchResponse.getClusters();
        assertThat(clusters.getTotal(), equalTo(1));
        assertThat(clusters.getSuccessful(), equalTo(1));
        assertThat(clusters.getSkipped(), equalTo(0));

        assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

        SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
        assertNotNull(remoteClusterSearchInfo);
        assertThat(remoteClusterSearchInfo.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
        assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
        assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards - 1));
        assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
        assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(1));
        assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
        assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
        assertTrue("should have 'index corrupted' in reason", remoteShardSearchFailure.reason().contains("index corrupted"));
    }

    public void testRemoteClusterOnlyCCSWithFailuresOnAllShards() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();
        SearchRequest searchRequest = new SearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(true);
        boolean minimizeRoundtrips = true; // TODO support MRT=false
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);

        // shardId -1 means to throw the Exception on all shards, so should result in complete search failure
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), -1);
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));
        client(LOCAL_CLUSTER).search(searchRequest, queryFuture);

        assertBusy(() -> assertTrue(queryFuture.isDone()));

        if (skipUnavailable == false) {
            ExecutionException ee = expectThrows(ExecutionException.class, () -> queryFuture.get());
            assertNotNull(ee.getCause());
            Throwable rootCause = ExceptionsHelper.unwrap(ee, IllegalStateException.class);
            assertThat(rootCause.getMessage(), containsString("index corrupted"));
        } else {
            SearchResponse searchResponse = queryFuture.get();
            assertNotNull(searchResponse);
            SearchResponse.Clusters clusters = searchResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getSuccessful(), equalTo(0));
            assertThat(clusters.getSkipped(), equalTo(1));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            SearchResponse.Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER).get();
            assertNotNull(remoteClusterSearchInfo);
            SearchResponse.Cluster.Status expectedStatus = skipUnavailable
                ? SearchResponse.Cluster.Status.SKIPPED
                : SearchResponse.Cluster.Status.FAILED;
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(expectedStatus));
            if (clusters.isCcsMinimizeRoundtrips()) {
                assertNull(remoteClusterSearchInfo.getTotalShards());
                assertNull(remoteClusterSearchInfo.getSuccessfulShards());
                assertNull(remoteClusterSearchInfo.getSkippedShards());
                assertNull(remoteClusterSearchInfo.getFailedShards());
                assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(1));
            } else {
                assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
                assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(0));
                assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
                assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(remoteNumShards));
                assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(remoteNumShards));
            }
            assertNull(remoteClusterSearchInfo.getTook());
            assertFalse(remoteClusterSearchInfo.isTimedOut());
            ShardSearchFailure remoteShardSearchFailure = remoteClusterSearchInfo.getFailures().get(0);
            assertTrue("should have 'index corrupted' in reason", remoteShardSearchFailure.reason().contains("index corrupted"));
        }
    }

    private Map<String, Object> setupTwoClusters() {
        String localIndex = "demo";
        int numShardsLocal = randomIntBetween(3, 6);
        Settings localSettings = indexSettings(numShardsLocal, 0).build();
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate(localIndex)
                .setSettings(localSettings)
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        indexDocs(client(LOCAL_CLUSTER), localIndex);

        String remoteIndex = "prod";
        int numShardsRemote = randomIntBetween(3, 6);
        final InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        final Settings.Builder remoteSettings = Settings.builder();
        remoteSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShardsRemote);

        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .indices()
                .prepareCreate(remoteIndex)
                .setSettings(Settings.builder().put(remoteSettings.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .setMapping("@timestamp", "type=date", "f", "type=text")
        );
        assertFalse(
            client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareHealth(remoteIndex)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexDocs(client(REMOTE_CLUSTER), remoteIndex);

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", localIndex);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", remoteIndex);
        clusterInfo.put("remote.skip_unavailable", skipUnavailable);
        return clusterInfo;
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(50, 100);
        for (int i = 0; i < numDocs; i++) {
            long ts = EARLIEST_TIMESTAMP + i;
            if (i == numDocs - 1) {
                ts = LATEST_TIMESTAMP;
            }
            client.prepareIndex(index).setSource("f", "v", "@timestamp", ts).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }
}
