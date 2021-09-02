/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.JoinHitLookupBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.LegacyReaderContext;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class CrossClusterSearchIT extends AbstractMultiClustersTestCase {

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of("cluster_a");
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("f", "v").get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    public void testRemoteClusterClientRole() throws Exception {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("demo"));
        final int demoDocs = indexDocs(client(LOCAL_CLUSTER), "demo");
        assertAcked(client("cluster_a").admin().indices().prepareCreate("prod"));
        final int prodDocs = indexDocs(client("cluster_a"), "prod");
        final InternalTestCluster localCluster = cluster(LOCAL_CLUSTER);
        final String pureDataNode = randomBoolean() ? localCluster.startDataOnlyNode() : null;
        final String nodeWithoutRemoteClusterClientRole = localCluster.startNode(NodeRoles.onlyRole(DiscoveryNodeRole.DATA_ROLE));
        ElasticsearchAssertions.assertFutureThrows(
            localCluster.client(nodeWithoutRemoteClusterClientRole)
                .prepareSearch("demo", "cluster_a:prod")
                .setQuery(new MatchAllQueryBuilder())
                .setAllowPartialSearchResults(false)
                .setSize(1000)
                .execute(),
            IllegalArgumentException.class,
            RestStatus.BAD_REQUEST,
            "node [" + nodeWithoutRemoteClusterClientRole + "] does not have the remote cluster client role enabled"
        );

        final String nodeWithRemoteClusterClientRole = randomFrom(
            StreamSupport.stream(localCluster.clusterService().state().nodes().spliterator(), false)
                .map(DiscoveryNode::getName)
                .filter(nodeName -> nodeWithoutRemoteClusterClientRole.equals(nodeName) == false)
                .filter(nodeName -> nodeName.equals(pureDataNode) == false)
                .collect(Collectors.toList()));

        final SearchResponse resp = localCluster.client(nodeWithRemoteClusterClientRole)
            .prepareSearch("demo", "cluster_a:prod")
            .setQuery(new MatchAllQueryBuilder())
            .setAllowPartialSearchResults(false)
            .setSize(1000)
            .get();
        assertHitCount(resp, demoDocs + prodDocs);
    }

    public void testProxyConnectionDisconnect() throws Exception {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("demo"));
        indexDocs(client(LOCAL_CLUSTER), "demo");
        final String remoteNode = cluster("cluster_a").startDataOnlyNode();
        assertAcked(client("cluster_a").admin().indices().prepareCreate("prod")
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", remoteNode)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()));
        indexDocs(client("cluster_a"), "prod");
        SearchListenerPlugin.blockQueryPhase();
        try {
            PlainActionFuture<SearchResponse> future = new PlainActionFuture<>();
            SearchRequest searchRequest = new SearchRequest("demo", "cluster_a:prod");
            searchRequest.allowPartialSearchResults(false);
            searchRequest.setCcsMinimizeRoundtrips(false);
            searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));
            client(LOCAL_CLUSTER).search(searchRequest, future);
            SearchListenerPlugin.waitSearchStarted();
            disconnectFromRemoteClusters();
            // Cancellable tasks on the remote cluster should be cancelled
            assertBusy(() -> {
                final Iterable<TransportService> transportServices = cluster("cluster_a").getInstances(TransportService.class);
                for (TransportService transportService : transportServices) {
                    Collection<CancellableTask> cancellableTasks = transportService.getTaskManager().getCancellableTasks().values();
                    for (CancellableTask cancellableTask : cancellableTasks) {
                        assertTrue(cancellableTask.getDescription(), cancellableTask.isCancelled());
                    }
                }
            });
            assertBusy(() -> assertTrue(future.isDone()));
            configureAndConnectsToRemoteClusters();
        } finally {
            SearchListenerPlugin.allowQueryPhase();
        }
    }

    public void testCancel() throws Exception {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("demo"));
        indexDocs(client(LOCAL_CLUSTER), "demo");
        final InternalTestCluster remoteCluster = cluster("cluster_a");
        remoteCluster.ensureAtLeastNumDataNodes(1);
        final Settings.Builder allocationFilter = Settings.builder();
        if (randomBoolean()) {
            remoteCluster.ensureAtLeastNumDataNodes(3);
            List<String> remoteDataNodes = StreamSupport.stream(remoteCluster.clusterService().state().nodes().spliterator(), false)
                .filter(DiscoveryNode::canContainData)
                .map(DiscoveryNode::getName)
                .collect(Collectors.toList());
            assertThat(remoteDataNodes.size(), Matchers.greaterThanOrEqualTo(3));
            List<String> seedNodes = randomSubsetOf(between(1, remoteDataNodes.size() - 1), remoteDataNodes);
            disconnectFromRemoteClusters();
            configureRemoteCluster("cluster_a", seedNodes);
            if (randomBoolean()) {
                // Using proxy connections
                allocationFilter.put("index.routing.allocation.exclude._name", String.join(",", seedNodes));
            } else {
                allocationFilter.put("index.routing.allocation.include._name", String.join(",", seedNodes));
            }
        }
        assertAcked(client("cluster_a").admin().indices().prepareCreate("prod")
            .setSettings(Settings.builder().put(allocationFilter.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)));
        assertFalse(client("cluster_a").admin().cluster().prepareHealth("prod")
            .setWaitForYellowStatus().setTimeout(TimeValue.timeValueSeconds(10)).get().isTimedOut());
        indexDocs(client("cluster_a"), "prod");
        SearchListenerPlugin.blockQueryPhase();
        PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();
        SearchRequest searchRequest = new SearchRequest("demo", "cluster_a:prod");
        searchRequest.allowPartialSearchResults(false);
        searchRequest.setCcsMinimizeRoundtrips(false);
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));
        client(LOCAL_CLUSTER).search(searchRequest, queryFuture);
        SearchListenerPlugin.waitSearchStarted();
        // Get the search task and cancelled
        final TaskInfo rootTask = client().admin().cluster().prepareListTasks()
            .setActions(SearchAction.INSTANCE.name())
            .get().getTasks().stream().filter(t -> t.getParentTaskId().isSet() == false)
            .findFirst().get();
        final CancelTasksRequest cancelRequest = new CancelTasksRequest().setTaskId(rootTask.getTaskId());
        cancelRequest.setWaitForCompletion(randomBoolean());
        final ActionFuture<CancelTasksResponse> cancelFuture = client().admin().cluster().cancelTasks(cancelRequest);
        assertBusy(() -> {
            final Iterable<TransportService> transportServices = cluster("cluster_a").getInstances(TransportService.class);
            for (TransportService transportService : transportServices) {
                Collection<CancellableTask> cancellableTasks = transportService.getTaskManager().getCancellableTasks().values();
                for (CancellableTask cancellableTask : cancellableTasks) {
                    assertTrue(cancellableTask.getDescription(), cancellableTask.isCancelled());
                }
            }
        });
        SearchListenerPlugin.allowQueryPhase();
        assertBusy(() -> assertTrue(queryFuture.isDone()));
        assertBusy(() -> assertTrue(cancelFuture.isDone()));
        assertBusy(() -> {
            final Iterable<TransportService> transportServices = cluster("cluster_a").getInstances(TransportService.class);
            for (TransportService transportService : transportServices) {
                assertThat(transportService.getTaskManager().getBannedTaskIds(), Matchers.empty());
            }
        });
    }

    /**
     * Makes sure the join hit lookup is executed on the local cluster
     */
    public void testLookupJoinHits() throws Exception {
        // Sometimes create the `user` index on the remote cluster
        if (randomBoolean()) {
            cluster("cluster_a").ensureAtMostNumDataNodes(1);
        }
        if (randomBoolean()) {
            cluster(LOCAL_CLUSTER).ensureAtMostNumDataNodes(1);
        }
        if (randomBoolean()) {
            cluster("cluster_a").ensureAtMostNumDataNodes(1);
        }
        if (randomBoolean()) {
            cluster("cluster_a").client()
                .admin().indices().prepareCreate("user")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
                .get();
            cluster("cluster_a").client().prepareBulk("user")
                .add(new IndexRequest().id("u1").source("device", "ios", "name", "Remote Kim"))
                .add(new IndexRequest().id("u2").source("device", "android", "name", "Remote Joe"))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
        client()
            .admin().indices().prepareCreate("user")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();
        client().prepareBulk("user")
            .add(new IndexRequest().id("u1").source("device", "ios", "name", "John New York"))
            .add(new IndexRequest().id("u2").source("device", "android", "name", "Mike Boston"))
            .add(new IndexRequest().id("u3").source("device", "linux", "name", "Jack Austin"))
            .add(new IndexRequest().id("u4").source("device", "macos", "name", "Tony London"))
            .add(new IndexRequest().id("u5").source("device", "macos", "name", "Tim Tokyo"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Calls on the remote cluster
        client().admin().indices().prepareCreate("call")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .setMapping("from_user", "type=keyword", "to_user", "type=keyword")
            .get();
        client().prepareBulk("call")
            .add(new IndexRequest().source("from_user", "u1", "to_user", List.of("u2", "u4"), "duration", 20))
            .add(new IndexRequest().source("from_user", "u2", "to_user", "u1", "duration", 25))
            .add(new IndexRequest().source("from_user", "u3", "to_user", "u5", "duration", 30))
            .add(new IndexRequest().source("from_user", "u4", "to_user", List.of("u1"), "duration", 35))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Calls on the remote cluster
        cluster("cluster_a").client().admin().indices().prepareCreate("call")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .setMapping("from_user", "type=keyword", "to_user", "type=keyword")
            .get();
        cluster("cluster_a").client().prepareBulk("call")
            .add(new IndexRequest().source("from_user", "u1", "to_user", "u2", "duration", 45))
            .add(new IndexRequest().source("from_user", "u2", "to_user", "u1", "duration", 50))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Search the remote index only
        {
            final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(new TermQueryBuilder("from_user", "u1"))
                .sort(new FieldSortBuilder("duration"))
                .fetchField("from_user")
                .lookupJoinHitBuilder(new JoinHitLookupBuilder("from_user", "user", "from_user"));
            final SearchRequest request = new SearchRequest("cluster_a:call").source(searchSourceBuilder);
            request.setCcsMinimizeRoundtrips(randomBoolean());
            final SearchResponse response = client().search(request).actionGet();
            final SearchHit[] hits = response.getHits().getHits();
            assertThat(hits, arrayWithSize(1));
            final List<SearchHit.JoinHit> fromUsers = hits[0].getJoinHits().get("from_user");
            assertThat(fromUsers, hasSize(1));
            assertThat(fromUsers.get(0).getId(), equalTo("u1"));
            assertThat(fromUsers.get(0).getFailure(), nullValue());
            assertThat(SourceLookup.sourceAsMap(fromUsers.get(0).getSource()).get("name"), equalTo("John New York"));
            assertThat(SourceLookup.sourceAsMap(fromUsers.get(0).getSource()).get("device"), equalTo("ios"));
        }

        // Search both local and remote indices
        {
            final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(new TermQueryBuilder("from_user", "u1"))
                .sort(new FieldSortBuilder("duration"))
                .lookupJoinHitBuilder(new JoinHitLookupBuilder("to_user", "user", "to_user"))
                .fetchField("to_user");
            final SearchRequest request = new SearchRequest("call", "cluster_a:call").source(searchSourceBuilder);
            request.setCcsMinimizeRoundtrips(randomBoolean());
            final SearchResponse response = client().search(request).actionGet();
            final SearchHit[] hits = response.getHits().getHits();
            assertThat(hits, arrayWithSize(2));

            List<SearchHit.JoinHit> toUsers = hits[0].getJoinHits().get("to_user");
            assertThat(toUsers, hasSize(2));
            assertThat(toUsers.get(0).getId(), equalTo("u2"));
            assertThat(toUsers.get(0).getFailure(), nullValue());
            assertThat(SourceLookup.sourceAsMap(toUsers.get(0).getSource()).get("name"), equalTo("Mike Boston"));
            assertThat(SourceLookup.sourceAsMap(toUsers.get(0).getSource()).get("device"), equalTo("android"));
            assertThat(toUsers.get(1).getId(), equalTo("u4"));
            assertThat(toUsers.get(1).getFailure(), nullValue());
            assertThat(SourceLookup.sourceAsMap(toUsers.get(1).getSource()).get("name"), equalTo("Tony London"));
            assertThat(SourceLookup.sourceAsMap(toUsers.get(1).getSource()).get("device"), equalTo("macos"));

            toUsers = hits[1].getJoinHits().get("to_user");
            assertThat(toUsers, hasSize(1));
            assertThat(toUsers.get(0).getId(), equalTo("u2"));
            assertThat(toUsers.get(0).getFailure(), nullValue());
            assertThat(SourceLookup.sourceAsMap(toUsers.get(0).getSource()).get("name"), equalTo("Mike Boston"));
            assertThat(SourceLookup.sourceAsMap(toUsers.get(0).getSource()).get("device"), equalTo("android"));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        if (clusterAlias.equals(LOCAL_CLUSTER)) {
            return super.nodePlugins(clusterAlias);
        } else {
            return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), SearchListenerPlugin.class);
        }
    }

    @Before
    public void resetSearchListenerPlugin() throws Exception {
        SearchListenerPlugin.reset();
    }

    public static class SearchListenerPlugin extends Plugin {
        private static final AtomicReference<CountDownLatch> startedLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> queryLatch = new AtomicReference<>();

        static void reset() {
            startedLatch.set(new CountDownLatch(1));
        }

        static void blockQueryPhase() {
            queryLatch.set(new CountDownLatch(1));
        }

        static void allowQueryPhase() {
            final CountDownLatch latch = queryLatch.get();
            if (latch != null) {
                latch.countDown();
            }
        }

        static void waitSearchStarted() throws InterruptedException {
            assertTrue(startedLatch.get().await(60, TimeUnit.SECONDS));
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onNewReaderContext(ReaderContext readerContext) {
                    assertThat(readerContext, not(instanceOf(LegacyReaderContext.class)));
                }

                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    startedLatch.get().countDown();
                    final CountDownLatch latch = queryLatch.get();
                    if (latch != null) {
                        try {
                            assertTrue(latch.await(60, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    }
                }
            });
            super.onIndexModule(indexModule);
        }
    }
}
