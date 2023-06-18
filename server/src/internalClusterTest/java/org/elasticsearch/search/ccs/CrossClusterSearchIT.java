/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardsAction;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.LegacyReaderContext;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
            localCluster.clusterService()
                .state()
                .nodes()
                .stream()
                .map(DiscoveryNode::getName)
                .filter(nodeName -> nodeWithoutRemoteClusterClientRole.equals(nodeName) == false)
                .filter(nodeName -> nodeName.equals(pureDataNode) == false)
                .toList()
        );

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
        assertAcked(
            client("cluster_a").admin()
                .indices()
                .prepareCreate("prod")
                .setSettings(
                    Settings.builder()
                        .put("index.routing.allocation.require._name", remoteNode)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
        );
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
                        if (TransportActionProxy.isProxyAction(cancellableTask.getAction())) {
                            assertTrue(cancellableTask.getDescription(), cancellableTask.isCancelled());
                        }
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
            List<String> remoteDataNodes = remoteCluster.clusterService()
                .state()
                .nodes()
                .stream()
                .filter(DiscoveryNode::canContainData)
                .map(DiscoveryNode::getName)
                .toList();
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
        assertAcked(
            client("cluster_a").admin()
                .indices()
                .prepareCreate("prod")
                .setSettings(Settings.builder().put(allocationFilter.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        assertFalse(
            client("cluster_a").admin()
                .cluster()
                .prepareHealth("prod")
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexDocs(client("cluster_a"), "prod");
        SearchListenerPlugin.blockQueryPhase();
        PlainActionFuture<SearchResponse> queryFuture = new PlainActionFuture<>();
        SearchRequest searchRequest = new SearchRequest("demo", "cluster_a:prod");
        searchRequest.allowPartialSearchResults(false);
        searchRequest.setCcsMinimizeRoundtrips(randomBoolean());
        searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));
        client(LOCAL_CLUSTER).search(searchRequest, queryFuture);
        SearchListenerPlugin.waitSearchStarted();
        // Get the search task and cancelled
        final TaskInfo rootTask = client().admin()
            .cluster()
            .prepareListTasks()
            .setActions(SearchAction.INSTANCE.name())
            .get()
            .getTasks()
            .stream()
            .filter(t -> t.parentTaskId().isSet() == false)
            .findFirst()
            .get();

        AtomicReference<List<TaskInfo>> remoteClusterSearchTasks = new AtomicReference<>();
        assertBusy(() -> {
            List<TaskInfo> remoteSearchTasks = client("cluster_a").admin()
                .cluster()
                .prepareListTasks()
                .get()
                .getTasks()
                .stream()
                .filter(t -> t.action().startsWith("indices:data/read/search"))
                .collect(Collectors.toList());
            assertThat(remoteSearchTasks.size(), greaterThan(0));
            remoteClusterSearchTasks.set(remoteSearchTasks);
        });

        for (TaskInfo taskInfo : remoteClusterSearchTasks.get()) {
            assertFalse("taskInfo is cancelled: " + taskInfo, taskInfo.cancelled());
        }

        final CancelTasksRequest cancelRequest = new CancelTasksRequest().setTargetTaskId(rootTask.taskId());
        cancelRequest.setWaitForCompletion(randomBoolean());
        final ActionFuture<CancelTasksResponse> cancelFuture = client().admin().cluster().cancelTasks(cancelRequest);
        assertBusy(() -> {
            final Iterable<TransportService> transportServices = cluster("cluster_a").getInstances(TransportService.class);
            for (TransportService transportService : transportServices) {
                Collection<CancellableTask> cancellableTasks = transportService.getTaskManager().getCancellableTasks().values();
                for (CancellableTask cancellableTask : cancellableTasks) {
                    if (cancellableTask.getAction().contains(SearchAction.INSTANCE.name())) {
                        assertTrue(cancellableTask.getDescription(), cancellableTask.isCancelled());
                    }
                }
            }
        });

        List<TaskInfo> remoteSearchTasksAfterCancellation = client("cluster_a").admin()
            .cluster()
            .prepareListTasks()
            .get()
            .getTasks()
            .stream()
            .filter(t -> t.action().startsWith("indices:data/read/search"))
            .collect(Collectors.toList());
        for (TaskInfo taskInfo : remoteSearchTasksAfterCancellation) {
            assertTrue(taskInfo.description(), taskInfo.cancelled());
        }

        SearchListenerPlugin.allowQueryPhase();
        assertBusy(() -> assertTrue(queryFuture.isDone()));
        assertBusy(() -> assertTrue(cancelFuture.isDone()));
        assertBusy(() -> {
            final Iterable<TransportService> transportServices = cluster("cluster_a").getInstances(TransportService.class);
            for (TransportService transportService : transportServices) {
                assertThat(transportService.getTaskManager().getBannedTaskIds(), Matchers.empty());
            }
        });

        try {
            queryFuture.result();
            fail("Exception should have been thrown since the search task was cancelled before the search completed");
        } catch (Exception e) {
            // should have been stopped due to TaskCancelledException
            Throwable t = ExceptionsHelper.unwrap(e, TaskCancelledException.class);
            assertNotNull(t);
        }
    }

    /**
     * Makes sure that lookup fields are resolved using the lookup index on each cluster.
     */
    public void testLookupFields() throws Exception {
        cluster("cluster_a").client()
            .admin()
            .indices()
            .prepareCreate("users")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();
        cluster("cluster_a").client()
            .prepareBulk("users")
            .add(new IndexRequest().id("a").source("name", "Remote A"))
            .add(new IndexRequest().id("b").source("name", "Remote B"))
            .add(new IndexRequest().id("c").source("name", "Remote C"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        client().admin()
            .indices()
            .prepareCreate("users")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();
        client().prepareBulk("users")
            .add(new IndexRequest().id("a").source("name", "Local A"))
            .add(new IndexRequest().id("b").source("name", "Local B"))
            .add(new IndexRequest().id("c").source("name", "Local C"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Setup calls on the local cluster
        client().admin()
            .indices()
            .prepareCreate("local_calls")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .setMapping("from_user", "type=keyword", "to_user", "type=keyword")
            .get();
        client().prepareBulk("local_calls")
            .add(new IndexRequest().source("from_user", "a", "to_user", List.of("b", "c"), "duration", 95))
            .add(new IndexRequest().source("from_user", "a", "to_user", "b", "duration", 25))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Setup calls on the remote cluster
        cluster("cluster_a").client()
            .admin()
            .indices()
            .prepareCreate("remote_calls")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .setMapping("from_user", "type=keyword", "to_user", "type=keyword")
            .get();
        cluster("cluster_a").client()
            .prepareBulk("remote_calls")
            .add(new IndexRequest().source("from_user", "a", "to_user", "b", "duration", 45))
            .add(new IndexRequest().source("from_user", "unknown_caller", "to_user", "c", "duration", 50))
            .add(new IndexRequest().source("from_user", List.of("a", "b"), "to_user", "c", "duration", 60))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        final String runtimeMappingSource = """
            {
                "from": {
                    "type": "lookup",
                    "target_index": "users",
                    "input_field": "from_user",
                    "target_field": "_id",
                    "fetch_fields": ["name"]
                },
                "to": {
                    "type": "lookup",
                    "target_index": "users",
                    "input_field": "to_user",
                    "target_field": "_id",
                    "fetch_fields": ["name"]
                }
            }
            """;
        final Map<String, Object> runtimeMappings;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, runtimeMappingSource)) {
            runtimeMappings = parser.map();
        }
        // Search on the remote cluster only
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(new TermQueryBuilder("to_user", "c"))
                .runtimeMappings(runtimeMappings)
                .sort(new FieldSortBuilder("duration"))
                .fetchField("from")
                .fetchField("to");
            SearchRequest request = new SearchRequest("cluster_a:remote_calls").source(searchSourceBuilder);
            request.setCcsMinimizeRoundtrips(randomBoolean());
            SearchResponse searchResponse = client().search(request).actionGet();
            ElasticsearchAssertions.assertHitCount(searchResponse, 2);
            SearchHit hit0 = searchResponse.getHits().getHits()[0];
            assertThat(hit0.getIndex(), equalTo("remote_calls"));
            assertThat(hit0.field("from"), nullValue());
            assertThat(hit0.field("to").getValues(), contains(Map.of("name", List.of("Remote C"))));

            SearchHit hit1 = searchResponse.getHits().getHits()[1];
            assertThat(hit1.getIndex(), equalTo("remote_calls"));
            assertThat(hit1.field("from").getValues(), contains(Map.of("name", List.of("Remote A")), Map.of("name", List.of("Remote B"))));
            assertThat(hit1.field("to").getValues(), contains(Map.of("name", List.of("Remote C"))));
        }
        // Search on both clusters
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(new TermQueryBuilder("to_user", "c"))
                .runtimeMappings(runtimeMappings)
                .sort(new FieldSortBuilder("duration"))
                .fetchField("from")
                .fetchField("to");
            SearchRequest request = new SearchRequest("local_calls", "cluster_a:remote_calls").source(searchSourceBuilder);
            request.setCcsMinimizeRoundtrips(randomBoolean());
            SearchResponse searchResponse = client().search(request).actionGet();
            ElasticsearchAssertions.assertHitCount(searchResponse, 3);
            SearchHit hit0 = searchResponse.getHits().getHits()[0];
            assertThat(hit0.getIndex(), equalTo("remote_calls"));
            assertThat(hit0.field("from"), nullValue());
            assertThat(hit0.field("to").getValues(), contains(Map.of("name", List.of("Remote C"))));

            SearchHit hit1 = searchResponse.getHits().getHits()[1];
            assertThat(hit1.getIndex(), equalTo("remote_calls"));
            assertThat(hit1.field("from").getValues(), contains(Map.of("name", List.of("Remote A")), Map.of("name", List.of("Remote B"))));
            assertThat(hit1.field("to").getValues(), contains(Map.of("name", List.of("Remote C"))));

            SearchHit hit2 = searchResponse.getHits().getHits()[2];
            assertThat(hit2.getIndex(), equalTo("local_calls"));
            assertThat(hit2.field("from").getValues(), contains(Map.of("name", List.of("Local A"))));
            assertThat(hit2.field("to").getValues(), contains(Map.of("name", List.of("Local B")), Map.of("name", List.of("Local C"))));
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

    public void testSearchShardsWithIndexNameQuery() {
        int numShards = randomIntBetween(1, 10);
        Client remoteClient = client("cluster_a");
        remoteClient.admin()
            .indices()
            .prepareCreate("my_index")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards))
            .get();
        int numDocs = randomIntBetween(100, 500);
        for (int i = 0; i < numDocs; i++) {
            remoteClient.prepareIndex("my_index").setSource("f", "v").get();
        }
        remoteClient.admin().indices().prepareRefresh("my_index").get();
        String[] indices = new String[] { "my_index" };
        IndicesOptions indicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        {
            QueryBuilder query = new TermQueryBuilder("_index", "cluster_a:my_index");
            SearchShardsRequest request = new SearchShardsRequest(indices, indicesOptions, query, null, null, randomBoolean(), "cluster_a");
            SearchShardsResponse resp = remoteClient.execute(SearchShardsAction.INSTANCE, request).actionGet();
            assertThat(resp.getGroups(), hasSize(numShards));
            for (SearchShardsGroup group : resp.getGroups()) {
                assertFalse(group.skipped());
            }
        }
        {
            QueryBuilder query = new TermQueryBuilder("_index", "cluster_a:my_index");
            SearchShardsRequest request = new SearchShardsRequest(
                indices,
                indicesOptions,
                query,
                null,
                null,
                randomBoolean(),
                randomFrom("cluster_b", null)
            );
            SearchShardsResponse resp = remoteClient.execute(SearchShardsAction.INSTANCE, request).actionGet();
            assertThat(resp.getGroups(), hasSize(numShards));
            for (SearchShardsGroup group : resp.getGroups()) {
                assertTrue(group.skipped());
            }
        }
        {
            QueryBuilder query = new TermQueryBuilder("_index", "cluster_a:not_my_index");
            SearchShardsRequest request = new SearchShardsRequest(
                indices,
                indicesOptions,
                query,
                null,
                null,
                randomBoolean(),
                randomFrom("cluster_a", "cluster_b", null)
            );
            SearchShardsResponse resp = remoteClient.execute(SearchShardsAction.INSTANCE, request).actionGet();
            assertThat(resp.getGroups(), hasSize(numShards));
            for (SearchShardsGroup group : resp.getGroups()) {
                assertTrue(group.skipped());
            }
        }
    }
}
