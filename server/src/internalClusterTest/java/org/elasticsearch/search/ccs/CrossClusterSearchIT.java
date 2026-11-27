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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponse.Cluster;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.AllocatingQueryPlugin;
import org.elasticsearch.search.AllocatingQueryPlugin.AllocatingQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.SlowRunningQueryBuilder;
import org.elasticsearch.search.query.ThrowingQueryBuilder;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

public class CrossClusterSearchIT extends AbstractCrossClusterSearchTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), TestQueryBuilderPlugin.class),
            AllocatingQueryPlugin.class
        );
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

    /**
     * With minimize_roundtrips, the coordinating node charges the local and remote responses while it merges them, and the hits the merged
     * response keeps until it is released. The local response arrives already charged, so charging it twice or releasing it twice would
     * leave the breaker away from zero.
     */
    public void testMinimizeRoundtripsResponseChargedUntilReleased() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        // Test clusters sometimes give a node a no-op request breaker, so coordinate on one with a real breaker.
        List<String> coordinators = Arrays.stream(cluster(LOCAL_CLUSTER).getNodeNames())
            .filter(node -> requestBreaker(node) instanceof NoopCircuitBreaker == false)
            .toList();
        assumeFalse("every local node has a no-op request breaker", coordinators.isEmpty());
        String coordinator = randomFrom(coordinators);
        CircuitBreaker breaker = requestBreaker(coordinator);

        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.setCcsMinimizeRoundtrips(true);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));
        SearchResponse response = cluster(LOCAL_CLUSTER).client(coordinator).search(searchRequest).actionGet();
        try {
            assertThat(response.getClusters().getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(2));
            assertThat(response.getHits().getHits().length, greaterThan(0));
            assertThat(breaker.getUsed(), greaterThan(0L));
        } finally {
            response.decRef();
        }
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(0L)));
    }

    /**
     * A remote-only minimize-roundtrips search has no local shard reduce. The hits it holds are the query's, so the query stays
     * tracked after its task is gone and is forgotten once the response is released.
     */
    public void testMinimizeRoundtripsChargesTheQuery() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        String coordinator = coordinatorWithRequestBreaker();
        SearchService searchService = cluster(LOCAL_CLUSTER).getInstance(SearchService.class, coordinator);

        SearchRequest searchRequest = new SearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.setCcsMinimizeRoundtrips(true);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));
        SearchResponse response = cluster(LOCAL_CLUSTER).client(coordinator).search(searchRequest).actionGet();
        try {
            assertThat(response.getHits().getHits().length, greaterThan(0));
            assertBusy(() -> {
                assertThat(searchTasks(coordinator), equalTo(0));
                assertThat(searchService.queryScheduler().queries(), equalTo(1));
            });
        } finally {
            response.decRef();
        }
        assertBusy(() -> assertThat(searchService.queryScheduler().queries(), equalTo(0)));
    }

    /**
     * A local and remote minimize-roundtrips search merges on this node. Only the remote index matches, so the hits the merged response
     * keeps come from the remote cluster and are charged by the merger. The merger releases the rest of its reservation after the client
     * listener returns. Once that reservation is gone, the hits keep the query tracked until the response is released.
     */
    public void testMinimizeRoundtripsMergerChargesTheQuery() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        client(REMOTE_CLUSTER).prepareIndex(remoteIndex).setSource("f", "remoteonly", "@timestamp", LATEST_TIMESTAMP).get();
        client(REMOTE_CLUSTER).admin().indices().prepareRefresh(remoteIndex).get();
        String coordinator = coordinatorWithRequestBreaker();
        SearchService searchService = cluster(LOCAL_CLUSTER).getInstance(SearchService.class, coordinator);
        CircuitBreaker breaker = requestBreaker(coordinator);

        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.setCcsMinimizeRoundtrips(true);
        searchRequest.source(new SearchSourceBuilder().query(new TermQueryBuilder("f", "remoteonly")).size(10));
        // The task is unregistered before this listener runs, and the merger releases its temporary reservation only after the listener
        // returns. Hold the listener so that reservation is still charged, then let the listener return.
        CountDownLatch responseHeld = new CountDownLatch(1);
        CountDownLatch releaseListener = new CountDownLatch(1);
        AtomicReference<SearchResponse> responseRef = new AtomicReference<>();
        AtomicReference<Exception> searchFailure = new AtomicReference<>();
        cluster(LOCAL_CLUSTER).client(coordinator).execute(TransportSearchAction.TYPE, searchRequest, ActionListener.wrap(response -> {
            response.incRef();
            responseRef.set(response);
            responseHeld.countDown();
            try {
                releaseListener.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, e -> {
            searchFailure.set(e);
            responseHeld.countDown();
        }));
        safeAwait(responseHeld);
        if (searchFailure.get() != null) {
            throw searchFailure.get();
        }
        SearchResponse response = responseRef.get();
        try {
            assertThat(response.getClusters().getClusterStateCount(Cluster.Status.SUCCESSFUL), equalTo(2));
            SearchHit[] hits = response.getHits().getHits();
            assertThat(hits.length, greaterThan(0));
            for (SearchHit hit : hits) {
                assertThat(hit.getClusterAlias(), equalTo(REMOTE_CLUSTER));
            }
            assertThat(searchTasks(coordinator), equalTo(0));
            assertThat(searchService.queryScheduler().queries(), equalTo(1));
            long reserved = breaker.getUsed();
            assertThat(reserved, greaterThan(0L));
            releaseListener.countDown();
            assertBusy(() -> {
                // The reservation has been released. The hits the response keeps are still charged, so the query stays tracked.
                assertThat(breaker.getUsed(), lessThan(reserved));
                assertThat(breaker.getUsed(), greaterThan(0L));
                assertThat(searchService.queryScheduler().queries(), equalTo(1));
            });
        } finally {
            releaseListener.countDown();
            response.decRef();
        }
        assertBusy(() -> {
            assertThat(breaker.getUsed(), equalTo(0L));
            assertThat(searchService.queryScheduler().queries(), equalTo(0));
        });
    }

    /**
     * A multi-search buffers a finished child until the whole request returns. That child's response stays on the node's breaker, so its
     * query is not kept tracked while a later child is still running.
     */
    public void testMultiSearchMinimizeRoundtripsReleasesTheChildQuery() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");
        String coordinator = coordinatorWithRequestBreaker();
        SearchService searchService = cluster(LOCAL_CLUSTER).getInstance(SearchService.class, coordinator);

        SearchRequest buffered = new SearchRequest(REMOTE_CLUSTER + ":" + remoteIndex);
        buffered.setCcsMinimizeRoundtrips(true);
        buffered.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));
        SearchRequest held = new SearchRequest(localIndex);
        held.source(new SearchSourceBuilder().query(new AllocatingQueryBuilder(64)).size(0));
        MultiSearchRequest multiSearch = new MultiSearchRequest().maxConcurrentSearchRequests(1);
        multiSearch.add(buffered);
        multiSearch.add(held);

        // Read the child response here. The transport releases it when this listener returns, including when the test fails while the
        // later child is still paused.
        AtomicInteger bufferedHits = new AtomicInteger();
        AtomicReference<Exception> itemFailure = new AtomicReference<>();
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        try (var pause = AllocatingQueryPlugin.pause()) {
            cluster(LOCAL_CLUSTER).client(coordinator).execute(TransportMultiSearchAction.TYPE, multiSearch, ActionListener.wrap(r -> {
                MultiSearchResponse.Item bufferedItem = r.getResponses()[0];
                if (bufferedItem.isFailure()) {
                    itemFailure.compareAndSet(null, bufferedItem.getFailure());
                } else {
                    bufferedHits.set(bufferedItem.getResponse().getHits().getHits().length);
                }
                MultiSearchResponse.Item heldItem = r.getResponses()[1];
                if (heldItem.isFailure()) {
                    itemFailure.compareAndSet(null, heldItem.getFailure());
                }
                done.onResponse(null);
            }, done::onFailure));
            pause.awaitReached();
            assertBusy(() -> assertThat(searchService.queryScheduler().queries(), equalTo(1)));
        }
        done.actionGet();
        assertNull(itemFailure.get());
        assertThat(bufferedHits.get(), greaterThan(0));
        assertBusy(() -> assertThat(searchService.queryScheduler().queries(), equalTo(0)));
    }

    private String coordinatorWithRequestBreaker() {
        List<String> coordinators = Arrays.stream(cluster(LOCAL_CLUSTER).getNodeNames())
            .filter(node -> requestBreaker(node) instanceof NoopCircuitBreaker == false)
            .toList();
        assumeFalse("every local node has a no-op request breaker", coordinators.isEmpty());
        return randomFrom(coordinators);
    }

    private int searchTasks(String coordinator) {
        return cluster(LOCAL_CLUSTER).client(coordinator)
            .admin()
            .cluster()
            .prepareListTasks()
            .setNodesIds(coordinator)
            .setActions(TransportSearchAction.NAME)
            .get()
            .getTasks()
            .size();
    }

    private CircuitBreaker requestBreaker(String localNode) {
        return cluster(LOCAL_CLUSTER).getInstance(CircuitBreakerService.class, localNode).getBreaker(CircuitBreaker.REQUEST);
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
            if (clusters.hasPartialResults()) {
                logger.info("Unexpected partial results for search: {}", response);
            }
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
            assertThat(remoteClusterSearchInfo.getSkippedShards(), equalTo(remoteNumShards));
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
        // dfs=true overrides the minimize_roundtrips=true setting and does not minimize roundtrips
        if (skipUnavailable == false && minimizeRoundtrips && dfs == false) {
            ExecutionException ee = expectThrows(ExecutionException.class, queryFuture::get);
            assertNotNull(ee.getCause());
            assertThat(ee.getCause(), instanceOf(RemoteTransportException.class));
            Throwable rootCause = ExceptionsHelper.unwrap(ee.getCause(), IllegalStateException.class);
            assertNotNull(ExceptionsHelper.stackTrace(ee), rootCause);
            assertThat(ExceptionsHelper.stackTrace(ee), rootCause.getMessage(), containsString("index corrupted"));
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
                assertThat(
                    failureReasonMessage(remoteClusterSearchInfo, remoteShardSearchFailure),
                    remoteShardSearchFailure.reason(),
                    containsString("index corrupted")
                );
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
        if (skipUnavailable == false || minimizeRoundtrips == false) {
            ExecutionException ee = expectThrows(ExecutionException.class, queryFuture::get);
            assertNotNull(ee.getCause());
            Throwable rootCause = ExceptionsHelper.unwrap(ee, IllegalStateException.class);
            assertNotNull(ExceptionsHelper.stackTrace(ee), rootCause);
            assertThat(ExceptionsHelper.stackTrace(ee), rootCause.getMessage(), containsString("index corrupted"));
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
                assertThat(
                    failureReasonMessage(remoteClusterSearchInfo, remoteShardSearchFailure),
                    remoteShardSearchFailure.reason(),
                    containsString("index corrupted")
                );
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

    public void testNegativeRemoteIndexNameEquivalentToRemoteNegativeIndexSyntax() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SearchRequest clusterPrefixExclusion = new SearchRequest(REMOTE_CLUSTER + ":*", "-" + REMOTE_CLUSTER + ":prod");
        clusterPrefixExclusion.setCcsMinimizeRoundtrips(randomBoolean());
        clusterPrefixExclusion.allowPartialSearchResults(false);
        clusterPrefixExclusion.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(5000));

        SearchRequest indexPrefixExclusion = new SearchRequest(REMOTE_CLUSTER + ":*", REMOTE_CLUSTER + ":-prod");
        indexPrefixExclusion.setCcsMinimizeRoundtrips(randomBoolean());
        indexPrefixExclusion.allowPartialSearchResults(false);
        indexPrefixExclusion.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(5000));

        final long[] clusterPrefixTotalHits = new long[1];
        assertResponse(client(LOCAL_CLUSTER).search(clusterPrefixExclusion), clusterPrefixResponse -> {
            assertNotNull(clusterPrefixResponse);
            Clusters clusters = clusterPrefixResponse.getClusters();
            assertThat(clusters.getTotal(), equalTo(1));
            assertNull(clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY));
            Cluster clusterPrefixCluster = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(clusterPrefixCluster);
            assertThat(clusterPrefixCluster.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(clusterPrefixCluster.getIndexExpression(), equalTo("*,-prod"));
            for (var hit : clusterPrefixResponse.getHits()) {
                assertThat(hit.getIndex(), equalTo(remoteIndex));
            }
            clusterPrefixTotalHits[0] = Objects.requireNonNull(clusterPrefixResponse.getHits().getTotalHits()).value();
        });
        assertResponse(client(LOCAL_CLUSTER).search(indexPrefixExclusion), indexPrefixResponse -> {
            assertNotNull(indexPrefixResponse);
            assertThat(Objects.requireNonNull(indexPrefixResponse.getHits().getTotalHits()).value(), equalTo(clusterPrefixTotalHits[0]));
            Cluster indexPrefixCluster = indexPrefixResponse.getClusters().getCluster(REMOTE_CLUSTER);
            assertNotNull(indexPrefixCluster);
            assertThat(indexPrefixCluster.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(indexPrefixCluster.getIndexExpression(), equalTo("*,-prod"));
        });
    }

    public void testNegativeRemoteIndexNameBeforeInclusionIsAccepted() throws Exception {
        String remoteIndex = (String) setupTwoClusters().get("remote.index");
        boolean minimizeRoundtrips = randomBoolean();

        SearchRequest controlRequest = new SearchRequest(REMOTE_CLUSTER + ":*");
        controlRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);
        controlRequest.allowPartialSearchResults(false);
        controlRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(5000));

        SearchRequest searchRequest = new SearchRequest("-" + REMOTE_CLUSTER + ":prod", REMOTE_CLUSTER + ":*");
        searchRequest.setCcsMinimizeRoundtrips(minimizeRoundtrips);
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(5000));

        final long[] controlTotalHits = new long[1];
        assertResponse(client(LOCAL_CLUSTER).search(controlRequest), controlResponse -> {
            controlTotalHits[0] = Objects.requireNonNull(controlResponse.getHits().getTotalHits()).value();
        });
        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);
            Clusters clusters = response.getClusters();
            assertThat(clusters.getTotal(), equalTo(1));
            Cluster localClusterSearchInfo = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            assertNull(localClusterSearchInfo);
            Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getIndexExpression(), equalTo("-prod,*"));
            assertThat(Objects.requireNonNull(response.getHits().getTotalHits()).value(), equalTo(controlTotalHits[0]));
            for (var hit : response.getHits()) {
                assertThat(hit.getIndex(), equalTo(remoteIndex));
            }
        });
    }

    public void testMixedRemoteIndexAndClusterExclusionsClusterExclusionLast() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        SearchRequest searchRequest = new SearchRequest(
            localIndex,
            REMOTE_CLUSTER + ":*",
            "-" + REMOTE_CLUSTER + ":prod",
            "-" + REMOTE_CLUSTER + ":*"
        );
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(5000));
        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);
            Clusters clusters = response.getClusters();
            assertNull(clusters.getCluster(REMOTE_CLUSTER));
            assertThat(Objects.requireNonNull(response.getHits().getTotalHits()).value(), greaterThan(0L));
            for (var hit : response.getHits()) {
                assertThat(hit.getIndex(), equalTo(localIndex));
            }
        });
    }

    public void testMixedRemoteIndexAndClusterExclusionsClusterExclusionFirst() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        SearchRequest searchRequest = new SearchRequest(
            localIndex,
            REMOTE_CLUSTER + ":*",
            "-" + REMOTE_CLUSTER + ":*",
            "-" + REMOTE_CLUSTER + ":prod"
        );
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        searchRequest.allowPartialSearchResults(false);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(5000));
        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);
            Clusters clusters = response.getClusters();
            Cluster remoteClusterSearchInfo = clusters.getCluster(REMOTE_CLUSTER);
            assertNotNull(remoteClusterSearchInfo);
            assertThat(remoteClusterSearchInfo.getStatus(), equalTo(Cluster.Status.SUCCESSFUL));
            assertThat(remoteClusterSearchInfo.getIndexExpression(), equalTo("-prod"));
            assertThat(remoteClusterSearchInfo.getTotalShards(), equalTo(0));
            assertThat(Objects.requireNonNull(response.getHits().getTotalHits()).value(), greaterThan(0L));
            for (var hit : response.getHits()) {
                assertThat(hit.getIndex(), equalTo(localIndex));
            }
        });
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
        assertThat(
            failureReasonMessage(cluster, remoteShardSearchFailure),
            remoteShardSearchFailure.reason(),
            containsString("index corrupted")
        );
    }

    /**
     * Describes the failure being asserted and its siblings. {@link Cluster#getFailures()} is ordered by arrival, not
     * by shard id, so the asserted entry is not necessarily shard 0.
     */
    private static String failureReasonMessage(Cluster cluster, ShardSearchFailure failure) {
        return Strings.format(
            "unexpected reason for failure on shard [%s] of cluster [%s]; all %d failure(s): %s",
            failure.shard(),
            cluster.getClusterAlias(),
            cluster.getFailures().size(),
            cluster.getFailures()
        );
    }

}
