/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Cluster;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.SlowRunningQueryBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.anyOf;
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
    protected List<String> remoteClusterAlias() {
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
        return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), CrossClusterSearchIT.TestQueryBuilderPlugin.class);
    }

    public static class TestQueryBuilderPlugin extends Plugin implements SearchPlugin {
        public TestQueryBuilderPlugin() {}

        @Override
        public List<QuerySpec<?>> getQueries() {
            QuerySpec<SlowRunningQueryBuilder> slowRunningSpec = new QuerySpec<>(
                SlowRunningQueryBuilder.NAME,
                SlowRunningQueryBuilder::new,
                p -> {
                    throw new IllegalStateException("not implemented");
                }
            );
            QuerySpec<ThrowingQueryBuilder> throwingSpec = new QuerySpec<>(ThrowingQueryBuilder.NAME, ThrowingQueryBuilder::new, p -> {
                throw new IllegalStateException("not implemented");
            });

            return List.of(slowRunningSpec, throwingSpec);
        }
    }

    public void testClusterDetailsAfterSuccessfulCCS() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        if (randomBoolean()) {
            searchRequest = searchRequest.scroll(TimeValue.timeValueMinutes(1));
        }
        searchRequest.allowPartialSearchResults(false);
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        }
        boolean minimizeRoundtrips = randomBoolean();
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);
        boolean dfs = randomBoolean();
        if (dfs) {
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);

            Clusters clusters = response.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.FAILED), equalTo(0));

            Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getIndexExpression(), equalTo(localIndex));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getIndexExpression(), equalTo(remoteIndex));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        });
    }

    // CCS with a search where the timestamp of the query cannot match so should be SUCCESSFUL with all shards skipped
    // during can-match
    public void testCCSClusterDetailsWhereAllShardsSkippedInCanMatch() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(false);
        boolean minimizeRoundtrips = randomBoolean();
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }
        searchRequest.setPreFilterShardSize(1);
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("@timestamp").from(EARLIEST_TIMESTAMP - 2000)
            .to(EARLIEST_TIMESTAMP - 1000);

        searchRequest.source(new SearchSourceBuilder().query(rangeQueryBuilder).size(10));

        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);

            Clusters clusters = response.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.FAILED), equalTo(0));

            Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);

            assertThat(localClusterSearchInfo.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            if (dfs) {
                // with DFS_QUERY_THEN_FETCH, the local shards are never skipped
                assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            } else {
                assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(localNumShards));
            }
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));

            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            if (clusters.isCcsMinimizeRoundtrips()) {
                assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(remoteNumShards));
            } else {
                assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(remoteNumShards));
            }
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));
        });

    }

    public void testClusterDetailsAfterCCSWithFailuresOnOneShardOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(true);
        boolean minimizeRoundtrips = randomBoolean();
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }
        // shardId 0 means to throw the Exception only on shard 0; all others should work
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), 0);
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));

        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);

            Clusters clusters = response.getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.PARTIAL), equalTo(2));
            assertThat(clusters.getClusterStateCount(Cluster.Status.FAILED), equalTo(0));

            Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertOneFailedShard(localClusterSearchInfo, localNumShards);

            Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertOneFailedShard(remoteClusterSearchInfo, remoteNumShards);
        });
    }

    // tests bug fix https://github.com/elastic/elasticsearch/issues/100350
    public void testClusterDetailsAfterCCSWhereRemoteClusterHasNoShardsToSearch() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");

        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + "no_such_index*");
        if (randomBoolean()) {
            searchRequest = searchRequest.scroll(TimeValue.timeValueMinutes(1));
        }
        searchRequest.allowPartialSearchResults(false);
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        }
        boolean minimizeRoundtrips = randomBoolean();
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);
        boolean dfs = randomBoolean();
        if (dfs) {
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);

            Clusters clusters = response.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.FAILED), equalTo(0));

            Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getIndexExpression(), equalTo(localIndex));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

            Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getIndexExpression(), equalTo("no_such_index*"));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(0)); // no shards since index does not exist
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertNotNull(remoteClusterSearchInfo.getTook());
        });
    }

    public void testClusterDetailsAfterCCSWithFailuresOnRemoteClusterOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();
        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(true);
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        }
        boolean dfs = randomBoolean();
        if (dfs) {
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }

        // throw Exception on all shards of remoteIndex, but not against localIndex
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(
            randomLong(),
            new IllegalStateException("index corrupted"),
            remoteIndex
        );
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));

        boolean minimizeRoundtrips = TransportSearchAction.shouldMinimizeRoundtrips(searchRequest);

        client(LOCAL_CLUSTER).search(searchRequest, queryFuture.delegateFailure((l, r) -> {
            r.incRef();
            l.onResponse(r);
        }));
        assertBusy(() -> assertTrue(queryFuture.isDone()));

        // dfs=true overrides the minimize_roundtrips=true setting and does not minimize roundtrips
        if (skipUnavailable == false && minimizeRoundtrips && dfs == false) {
            ExecutionException ee = expectThrows(ExecutionException.class, () -> queryFuture.get());
            assertNotNull(ee.getCause());
            assertThat(ee.getCause(), instanceOf(RemoteTransportException.class));
            Throwable rootCause = ExceptionsHelper.unwrap(ee.getCause(), IllegalStateException.class);
            assertThat(rootCause.getMessage(), containsString("index corrupted"));
        } else {
            assertResponse(queryFuture, response -> {
                assertNotNull(response);

                Clusters clusters = response.getClusters();
                if (dfs == false) {
                    assertThat(clusters.isCcsMinimizeRoundtrips(), equalTo(minimizeRoundtrips));
                }
                assertThat(clusters.getTotal(), equalTo(2));
                assertThat(clusters.getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(1));
                assertThat(clusters.getClusterStateCount(Cluster.Status.RUNNING), equalTo(0));
                assertThat(clusters.getClusterStateCount(Cluster.Status.PARTIAL), equalTo(0));
                if (skipUnavailable) {
                    assertThat(clusters.getClusterStateCount(Cluster.Status.SKIPPED), equalTo(1));
                    assertThat(clusters.getClusterStateCount(Cluster.Status.FAILED), equalTo(0));
                } else {
                    assertThat(clusters.getClusterStateCount(Cluster.Status.SKIPPED), equalTo(0));
                    assertThat(clusters.getClusterStateCount(Cluster.Status.FAILED), equalTo(1));
                }

                Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                assertNotNull(localClusterSearchInfo);
                assertThat(localClusterSearchInfo.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
                assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
                assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
                assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
                assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
                assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
                assertThat(localClusterSearchInfo.getTook().millis(), greaterThan(0L));

                Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);

                assertNotNull(remoteClusterSearchInfo);
                Cluster.Status expectedStatus = skipUnavailable ? Cluster.Status.SKIPPED : Cluster.Status.FAILED;
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
            });
        }
    }

    public void testCCSWithSearchTimeoutOnRemoteCluster() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(true);
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());

        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        }
        if (randomBoolean()) {
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }

        TimeValue searchTimeout = new TimeValue(100, TimeUnit.MILLISECONDS);
        // query builder that will sleep for the specified amount of time in the query phase
        SlowRunningQueryBuilder slowRunningQueryBuilder = new SlowRunningQueryBuilder(searchTimeout.millis() * 5);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(slowRunningQueryBuilder).timeout(searchTimeout);
        searchRequest.source(sourceBuilder);

        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);

            Clusters clusters = response.getClusters();
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.PARTIAL), equalTo(2));
            assertThat(clusters.getClusterStateCount(Cluster.Status.FAILED), equalTo(0));

            Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(Cluster.Status.PARTIAL));
            assertTrue(localClusterSearchInfo.isTimedOut());
            assertThat(localClusterSearchInfo.getIndexExpression(), equalTo(localIndex));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));

            Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(Cluster.Status.PARTIAL));
            assertTrue(remoteClusterSearchInfo.isTimedOut());
            assertThat(remoteClusterSearchInfo.getIndexExpression(), equalTo(remoteIndex));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThanOrEqualTo(0L));
        });
    }

    public void testRemoteClusterOnlyCCSSuccessfulResult() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchRequest searchRequest = new SearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(false);
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        }
        if (randomBoolean()) {
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));

        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);

            Clusters clusters = response.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(1));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.FAILED), equalTo(0));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        });
    }

    public void testRemoteClusterOnlyCCSWithFailuresOnOneShardOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchRequest searchRequest = new SearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(true);
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        }
        if (randomBoolean()) {
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }

        // shardId 0 means to throw the Exception only on shard 0; all others should work
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), 0);
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));

        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {

            Clusters clusters = response.getClusters();
            assertThat(clusters.getTotal(), equalTo(1));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.PARTIAL), equalTo(1));
            assertThat(clusters.getClusterStateCount(Cluster.Status.FAILED), equalTo(0));

            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

            Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertOneFailedShard(remoteClusterSearchInfo, remoteNumShards);
        });
    }

    public void testRemoteClusterOnlyCCSWithFailuresOnAllShards() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        boolean skipUnavailable = (Boolean) testClusterInfo.get("remote.skip_unavailable");

        PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();
        SearchRequest searchRequest = new SearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.allowPartialSearchResults(true);
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        }
        if (randomBoolean()) {
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }
        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }

        // shardId -1 means to throw the Exception on all shards, so should result in complete search failure
        ThrowingQueryBuilder queryBuilder = new ThrowingQueryBuilder(randomLong(), new IllegalStateException("index corrupted"), -1);
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));

        boolean minimizeRoundtrips = TransportSearchAction.shouldMinimizeRoundtrips(searchRequest);

        client(LOCAL_CLUSTER).search(searchRequest, queryFuture.delegateFailure((l, r) -> {
            r.incRef();
            l.onResponse(r);
        }));
        assertBusy(() -> assertTrue(queryFuture.isDone()));

        if (skipUnavailable == false || minimizeRoundtrips == false) {
            ExecutionException ee = expectThrows(ExecutionException.class, () -> queryFuture.get());
            assertNotNull(ee.getCause());
            Throwable rootCause = ExceptionsHelper.unwrap(ee, IllegalStateException.class);
            assertThat(rootCause.getMessage(), containsString("index corrupted"));
        } else {
            assertResponse(queryFuture, response -> {
                assertNotNull(response);
                Clusters clusters = response.getClusters();
                assertThat(clusters.getTotal(), equalTo(1));
                assertThat(clusters.getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(0));
                assertThat(clusters.getClusterStateCount(Cluster.Status.RUNNING), equalTo(0));
                assertThat(clusters.getClusterStateCount(Cluster.Status.PARTIAL), equalTo(0));
                if (skipUnavailable) {
                    assertThat(clusters.getClusterStateCount(Cluster.Status.SKIPPED), equalTo(1));
                    assertThat(clusters.getClusterStateCount(Cluster.Status.FAILED), equalTo(0));
                } else {
                    assertThat(clusters.getClusterStateCount(Cluster.Status.SKIPPED), equalTo(0));
                    assertThat(clusters.getClusterStateCount(Cluster.Status.FAILED), equalTo(1));
                }

                assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));

                Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
                assertNotNull(remoteClusterSearchInfo);
                Cluster.Status expectedStatus = skipUnavailable ? Cluster.Status.SKIPPED : Cluster.Status.FAILED;
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
            });
        }
    }

    public void testDateMathIndexes() throws ExecutionException, InterruptedException {
        Map<String, Object> testClusterInfo = setupTwoClusters(
            new String[] { "datemath-2001-01-01-14", "datemath-2001-01-01-15" },
            new String[] { "remotemath-2001-01-01-14", "remotemath-2001-01-01-15" }
        );
        SearchRequest searchRequest = new SearchRequest(
            REMOTE_CLUSTER + ":<remotemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}>",
            "<datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}>"
        );
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(5000));
        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);
            Clusters clusters = response.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(Objects.requireNonNull(response.getHits().getTotalHits()).value(), greaterThan(2L));
            for (var hit : response.getHits()) {
                assertThat(hit.getIndex(), anyOf(equalTo("datemath-2001-01-01-14"), equalTo("remotemath-2001-01-01-14")));
            }
        });
    }

    /**
     * Test for issue https://github.com/elastic/elasticsearch/issues/112243
     */
    public void testDateMathNegativeIndexesLocal() throws ExecutionException, InterruptedException {
        Map<String, Object> testClusterInfo = setupTwoClusters(
            new String[] { "datemath-2001-01-01-14", "datemath-2001-01-01-15" },
            new String[] { "datemath-2001-01-01-14", "datemath-2001-01-01-15" }
        );
        SearchRequest searchRequest = new SearchRequest("da*", "-<datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}>");
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(5000));
        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);
            Clusters clusters = response.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(0));
            for (var hit : response.getHits()) {
                assertThat(hit.getIndex(), equalTo("datemath-2001-01-01-15"));
            }
        });
    }

    /**
     * Test for issue https://github.com/elastic/elasticsearch/issues/112243
     */
    public void testDateMathNegativeIndexesRemote() throws ExecutionException, InterruptedException {
        Map<String, Object> testClusterInfo = setupTwoClusters(
            new String[] { "datemath-2001-01-01-14", "datemath-2001-01-01-15" },
            new String[] { "datemath-2001-01-01-14", "datemath-2001-01-01-15" }
        );
        SearchRequest searchRequest = new SearchRequest(
            REMOTE_CLUSTER + ":*",
            REMOTE_CLUSTER + ":-<datemath-{2001-01-01-13||+1h/h{yyyy-MM-dd-HH|-07:00}}>"
        );
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(5000));
        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);
            Clusters clusters = response.getClusters();
            assertFalse("search cluster results should NOT be marked as partial", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(1));
            Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNull(localClusterSearchInfo);
            Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            for (var hit : response.getHits()) {
                assertThat(hit.getIndex(), equalTo("datemath-2001-01-01-15"));
            }
        });
    }

    public void testNegativeRemoteIndexNameThrows() {
        SearchRequest searchRequest = new SearchRequest("*:*", "-" + REMOTE_CLUSTER + ":prod");
        searchRequest.setCcsMinimizeRoundtrips(true);
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(5000));
        var queryFuture = client(LOCAL_CLUSTER).search(searchRequest);
        // This should throw the wildcard error
        ExecutionException ee = expectThrows(ExecutionException.class, queryFuture::get);
        assertNotNull(ee.getCause());
    }

    public void testClusterDetailsWhenLocalClusterHasNoMatchingIndex() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        SearchRequest searchRequest = new SearchRequest("nomatch*", REMOTE_CLUSTER + ":" + remoteIndex);
        if (randomBoolean()) {
            searchRequest = searchRequest.scroll(TimeValue.timeValueMinutes(1));
        }

        searchRequest.allowPartialSearchResults(false);
        if (randomBoolean()) {
            searchRequest.setBatchedReduceSize(randomIntBetween(3, 20));
        }

        boolean minimizeRoundtrips = false;
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);

        boolean dfs = randomBoolean();
        if (dfs) {
            searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
        }

        if (randomBoolean()) {
            searchRequest.setPreFilterShardSize(1);
        }

        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));
        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);

            Clusters clusters = response.getClusters();
            assertFalse("search cluster results should BE successful", clusters.hasPartialResults());
            assertThat(clusters.getTotal(), equalTo(2));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(clusters.getClusterStateCount(Cluster.Status.SKIPPED), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.RUNNING), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.PARTIAL), equalTo(0));
            assertThat(clusters.getClusterStateCount(Cluster.Status.FAILED), equalTo(0));

            Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNotNull(localClusterSearchInfo);
            assertThat(localClusterSearchInfo.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(localClusterSearchInfo.getIndexExpression(), equalTo("nomatch*"));
            assertThat(localClusterSearchInfo.getTotalShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getSuccessfulShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(localClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(localClusterSearchInfo.getTook().millis(), equalTo(0L));

            Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getIndexExpression(), equalTo(remoteIndex));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailedShards(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getFailures().size(), equalTo(0));
            assertThat(remoteClusterSearchInfo.getTook().millis(), greaterThan(0L));
        });
    }

    private static void assertOneFailedShard(Cluster cluster, int totalShards) {
        assertNotNull(cluster);
        assertThat(cluster.getStatus(), equalTo(Cluster.Status.PARTIAL));
        assertThat(cluster.getTotalShards(), equalTo(totalShards));
        assertThat(cluster.getSuccessfulShards(), equalTo(totalShards - 1));
        assertThat(cluster.getSkippedShards(), equalTo(0));
        assertThat(cluster.getFailedShards(), equalTo(1));
        assertThat(cluster.getFailures().size(), equalTo(1));
        assertThat(cluster.getTook().millis(), greaterThan(0L));
        ShardSearchFailure remoteShardSearchFailure = cluster.getFailures().get(0);
        assertTrue("should have 'index corrupted' in reason", remoteShardSearchFailure.reason().contains("index corrupted"));
    }

    private Map<String, Object> setupTwoClusters(String[] localIndices, String[] remoteIndices) {
        int numShardsLocal = randomIntBetween(2, 10);
        Settings localSettings = indexSettings(numShardsLocal, randomIntBetween(0, 1)).build();
        for (String localIndex : localIndices) {
            assertAcked(
                client(LOCAL_CLUSTER).admin()
                    .indices()
                    .prepareCreate(localIndex)
                    .setSettings(localSettings)
                    .setMapping("@timestamp", "type=date", "f", "type=text")
            );
            indexDocs(client(LOCAL_CLUSTER), localIndex);
        }

        int numShardsRemote = randomIntBetween(2, 10);
        final InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        for (String remoteIndex : remoteIndices) {
            assertAcked(
                client(REMOTE_CLUSTER).admin()
                    .indices()
                    .prepareCreate(remoteIndex)
                    .setSettings(indexSettings(numShardsRemote, randomIntBetween(0, 1)))
                    .setMapping("@timestamp", "type=date", "f", "type=text")
            );
            assertFalse(
                client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .prepareHealth(TEST_REQUEST_TIMEOUT, remoteIndex)
                    .setWaitForYellowStatus()
                    .setTimeout(TimeValue.timeValueSeconds(10))
                    .get()
                    .isTimedOut()
            );
            indexDocs(client(REMOTE_CLUSTER), remoteIndex);
        }

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.skip_unavailable", skipUnavailable);
        return clusterInfo;
    }

    private Map<String, Object> setupTwoClusters() {
        var clusterInfo = setupTwoClusters(new String[] { "demo" }, new String[] { "prod" });
        clusterInfo.put("local.index", "demo");
        clusterInfo.put("remote.index", "prod");
        return clusterInfo;
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(500, 1200);
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
