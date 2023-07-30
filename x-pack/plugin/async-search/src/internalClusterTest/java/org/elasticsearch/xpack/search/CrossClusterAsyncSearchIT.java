/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.LegacyReaderContext;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.async.AsyncResultsIndexPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultAction;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * This IT test copies the setup and general approach that the {@code CrossClusterSearchIT} test
 * used for testing synchronous CCS.
 */
public class CrossClusterAsyncSearchIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "cluster_a";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugs = Arrays.asList(
            SearchListenerPlugin.class,
            AsyncSearch.class,
            AsyncResultsIndexPlugin.class,
            LocalStateCompositeXPackPlugin.class,
            TestQueryBuilderPlugin.class
        );
        return Stream.concat(super.nodePlugins(clusterAlias).stream(), plugs.stream()).collect(Collectors.toList());
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

            return List.of(slowRunningSpec);
        }
    }

    public void testCancelViaTasksAPI() throws Exception {
        setupTwoClusters();

        SearchListenerPlugin.blockQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest("demo", REMOTE_CLUSTER + ":prod");
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        request.getSearchRequest().allowPartialSearchResults(false);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());

        SearchListenerPlugin.waitSearchStarted();

        ActionFuture<CancelTasksResponse> cancelFuture;
        try {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(SearchAction.INSTANCE.name())
                .get();
            List<TaskInfo> tasks = listTasksResponse.getTasks();
            assertThat(tasks.size(), equalTo(1));
            final TaskInfo rootTask = tasks.get(0);

            AtomicReference<List<TaskInfo>> remoteClusterSearchTasks = new AtomicReference<>();
            assertBusy(() -> {
                List<TaskInfo> remoteSearchTasks = client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .prepareListTasks()
                    .get()
                    .getTasks()
                    .stream()
                    .filter(t -> t.action().contains(SearchAction.NAME))
                    .collect(Collectors.toList());
                assertThat(remoteSearchTasks.size(), greaterThan(0));
                remoteClusterSearchTasks.set(remoteSearchTasks);
            });

            for (TaskInfo taskInfo : remoteClusterSearchTasks.get()) {
                assertFalse("taskInfo on remote cluster should not be cancelled yet: " + taskInfo, taskInfo.cancelled());
            }

            final CancelTasksRequest cancelRequest = new CancelTasksRequest().setTargetTaskId(rootTask.taskId());
            cancelRequest.setWaitForCompletion(randomBoolean());
            cancelFuture = client().admin().cluster().cancelTasks(cancelRequest);
            assertBusy(() -> {
                final Iterable<TransportService> transportServices = cluster(REMOTE_CLUSTER).getInstances(TransportService.class);
                for (TransportService transportService : transportServices) {
                    Collection<CancellableTask> cancellableTasks = transportService.getTaskManager().getCancellableTasks().values();
                    for (CancellableTask cancellableTask : cancellableTasks) {
                        if (cancellableTask.getAction().contains(SearchAction.INSTANCE.name())) {
                            assertTrue(cancellableTask.getDescription(), cancellableTask.isCancelled());
                        }
                    }
                }
            });

            List<TaskInfo> remoteSearchTasksAfterCancellation = client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .get()
                .getTasks()
                .stream()
                .filter(t -> t.action().contains(SearchAction.INSTANCE.name()))
                .toList();
            for (TaskInfo taskInfo : remoteSearchTasksAfterCancellation) {
                assertTrue(taskInfo.description(), taskInfo.cancelled());
            }

            // check async search status before allowing query to continue but after cancellation
            AsyncSearchResponse searchResponseAfterCancellation = getAsyncSearch(response.getId());
            assertTrue(searchResponseAfterCancellation.isPartial());
            assertTrue(searchResponseAfterCancellation.isRunning());
            assertFalse(searchResponseAfterCancellation.getSearchResponse().isTimedOut());
            assertThat(searchResponseAfterCancellation.getSearchResponse().getClusters().getTotal(), equalTo(2));
            assertThat(searchResponseAfterCancellation.getSearchResponse().getFailedShards(), equalTo(0));

            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());
            assertTrue(statusResponse.isRunning());
            assertThat(statusResponse.getClusters().getTotal(), equalTo(2));
            assertThat(statusResponse.getFailedShards(), equalTo(0));
            assertNull(statusResponse.getCompletionStatus());

        } finally {
            SearchListenerPlugin.allowQueryPhase();
        }

        assertBusy(() -> assertTrue(cancelFuture.isDone()));
        assertBusy(() -> {
            final Iterable<TransportService> transportServices = cluster(REMOTE_CLUSTER).getInstances(TransportService.class);
            for (TransportService transportService : transportServices) {
                assertThat(transportService.getTaskManager().getBannedTaskIds(), Matchers.empty());
            }
        });

        // wait until search status endpoint reports it as completed
        assertBusy(() -> {
            AsyncStatusResponse statusResponseAfterCompletion = getAsyncStatus(response.getId());
            assertNotNull(statusResponseAfterCompletion.getCompletionStatus());
        });

        AsyncStatusResponse statusResponseAfterCompletion = getAsyncStatus(response.getId());
        assertTrue(statusResponseAfterCompletion.isPartial());
        assertFalse(statusResponseAfterCompletion.isRunning());
        assertThat(statusResponseAfterCompletion.getClusters().getTotal(), equalTo(2));
        assertThat(statusResponseAfterCompletion.getFailedShards(), greaterThan(0));
        assertThat(statusResponseAfterCompletion.getCompletionStatus(), equalTo(RestStatus.BAD_REQUEST));

        AsyncSearchResponse searchResponseAfterCompletion = getAsyncSearch(response.getId());
        assertTrue(searchResponseAfterCompletion.isPartial());
        assertFalse(searchResponseAfterCompletion.isRunning());
        assertFalse(searchResponseAfterCompletion.getSearchResponse().isTimedOut());
        assertThat(searchResponseAfterCompletion.getSearchResponse().getClusters().getTotal(), equalTo(2));
        assertThat(searchResponseAfterCompletion.getSearchResponse().getFailedShards(), greaterThan(0));
        Throwable cause = ExceptionsHelper.unwrap(searchResponseAfterCompletion.getFailure(), TaskCancelledException.class);
        assertNotNull("TaskCancelledException should be in the causal chain", cause);
        ShardSearchFailure[] shardFailures = searchResponseAfterCompletion.getSearchResponse().getShardFailures();
        assertThat(shardFailures.length, greaterThan(0));
        String json = Strings.toString(searchResponseAfterCompletion.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        assertThat(json, containsString("task cancelled [by user request]"));
    }

    public void testCancelViaAsyncSearchDelete() throws Exception {
        setupTwoClusters();

        SearchListenerPlugin.blockQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest("demo", REMOTE_CLUSTER + ":prod");
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        request.getSearchRequest().allowPartialSearchResults(false);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());

        SearchListenerPlugin.waitSearchStarted();

        try {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(SearchAction.INSTANCE.name())
                .get();
            List<TaskInfo> tasks = listTasksResponse.getTasks();
            assertThat(tasks.size(), equalTo(1));

            AtomicReference<List<TaskInfo>> remoteClusterSearchTasks = new AtomicReference<>();
            assertBusy(() -> {
                List<TaskInfo> remoteSearchTasks = client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .prepareListTasks()
                    .get()
                    .getTasks()
                    .stream()
                    .filter(t -> t.action().contains(SearchAction.NAME))
                    .collect(Collectors.toList());
                assertThat(remoteSearchTasks.size(), greaterThan(0));
                remoteClusterSearchTasks.set(remoteSearchTasks);
            });

            for (TaskInfo taskInfo : remoteClusterSearchTasks.get()) {
                assertFalse("taskInfo on remote cluster should not be cancelled yet: " + taskInfo, taskInfo.cancelled());
            }

            AcknowledgedResponse ack = deleteAsyncSearch(response.getId());
            assertTrue(ack.isAcknowledged());

            assertBusy(() -> {
                final Iterable<TransportService> transportServices = cluster(REMOTE_CLUSTER).getInstances(TransportService.class);
                for (TransportService transportService : transportServices) {
                    Collection<CancellableTask> cancellableTasks = transportService.getTaskManager().getCancellableTasks().values();
                    for (CancellableTask cancellableTask : cancellableTasks) {
                        if (cancellableTask.getAction().contains(SearchAction.INSTANCE.name())) {
                            assertTrue(cancellableTask.getDescription(), cancellableTask.isCancelled());
                        }
                    }
                }
            });

            List<TaskInfo> remoteSearchTasksAfterCancellation = client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .get()
                .getTasks()
                .stream()
                .filter(t -> t.action().contains(SearchAction.INSTANCE.name()))
                .toList();
            for (TaskInfo taskInfo : remoteSearchTasksAfterCancellation) {
                assertTrue(taskInfo.description(), taskInfo.cancelled());
            }

            ExecutionException e = expectThrows(ExecutionException.class, () -> getAsyncSearch(response.getId()));
            assertNotNull(e);
            assertNotNull(e.getCause());
            Throwable t = ExceptionsHelper.unwrap(e, ResourceNotFoundException.class);
            assertNotNull("after deletion, getAsyncSearch should throw an Exception with ResourceNotFoundException in the causal chain", t);

            AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
            assertTrue(statusResponse.isPartial());
            assertTrue(statusResponse.isRunning());
            assertThat(statusResponse.getClusters().getTotal(), equalTo(2));
            assertThat(statusResponse.getFailedShards(), equalTo(0));
            assertNull(statusResponse.getCompletionStatus());
        } finally {
            SearchListenerPlugin.allowQueryPhase();
        }

        assertBusy(() -> expectThrows(ExecutionException.class, () -> getAsyncStatus(response.getId())));
        assertBusy(() -> {
            final Iterable<TransportService> transportServices = cluster(REMOTE_CLUSTER).getInstances(TransportService.class);
            for (TransportService transportService : transportServices) {
                assertThat(transportService.getTaskManager().getBannedTaskIds(), Matchers.empty());
            }
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/97286")
    public void testCancellationViaTimeoutWithAllowPartialResultsSetToFalse() throws Exception {
        setupTwoClusters();

        SearchListenerPlugin.blockQueryPhase();

        TimeValue searchTimeout = new TimeValue(100, TimeUnit.MILLISECONDS);
        // query builder that will sleep for the specified amount of time in the query phase
        SlowRunningQueryBuilder slowRunningQueryBuilder = new SlowRunningQueryBuilder(searchTimeout.millis() * 5);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(slowRunningQueryBuilder).timeout(searchTimeout);

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest("demo", REMOTE_CLUSTER + ":prod");
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.getSearchRequest().source(sourceBuilder);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.getSearchRequest().allowPartialSearchResults(false);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);

        AsyncSearchResponse response = submitAsyncSearch(request);
        assertNotNull(response.getSearchResponse());
        assertTrue(response.isRunning());

        SearchListenerPlugin.waitSearchStarted();

        // ensure tasks are present on both clusters and not cancelled
        try {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(SearchAction.INSTANCE.name())
                .get();
            List<TaskInfo> tasks = listTasksResponse.getTasks();
            assertThat(tasks.size(), equalTo(1));

            AtomicReference<List<TaskInfo>> remoteClusterSearchTasks = new AtomicReference<>();
            assertBusy(() -> {
                List<TaskInfo> remoteSearchTasks = client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .prepareListTasks()
                    .get()
                    .getTasks()
                    .stream()
                    .filter(t -> t.action().contains(SearchAction.NAME))
                    .collect(Collectors.toList());
                assertThat(remoteSearchTasks.size(), greaterThan(0));
                remoteClusterSearchTasks.set(remoteSearchTasks);
            });

            for (TaskInfo taskInfo : remoteClusterSearchTasks.get()) {
                assertFalse("taskInfo on remote cluster should not be cancelled yet: " + taskInfo, taskInfo.cancelled());
            }

        } finally {
            SearchListenerPlugin.allowQueryPhase();
        }

        // query phase has begun, so wait for query failure (due to timeout)
        SearchListenerPlugin.waitQueryFailure();

        // wait for the async_search task to be cancelled or unregistered
        assertBusy(() -> {
            ListTasksResponse taskResponses = client().admin().cluster().prepareListTasks().setDetailed(true).get();
            List<TaskInfo> asyncSearchTaskInfos = new ArrayList<>();
            for (TaskInfo task : taskResponses.getTasks()) {
                if (task.action().contains("search")) {
                    if (task.description().contains("async_search{indices[")) {
                        asyncSearchTaskInfos.add(task);
                    }
                }
            }

            if (asyncSearchTaskInfos.size() > 0) {
                // if still present, and it is cancelled, then we can proceed with the test
                assertTrue(asyncSearchTaskInfos.get(0).cancelled());
            }
            // if not present, then it has been unregistered and the async search should no longer be running, so can proceed
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> { assertFalse(getAsyncStatus(response.getId()).isRunning()); });

        AsyncStatusResponse statusResponse = getAsyncStatus(response.getId());
        assertFalse(statusResponse.isRunning());
        assertEquals(0, statusResponse.getSuccessfulShards());
        assertEquals(0, statusResponse.getSkippedShards());
        assertThat(statusResponse.getFailedShards(), greaterThanOrEqualTo(1));

        assertBusy(() -> {
            final Iterable<TransportService> transportServices = cluster(REMOTE_CLUSTER).getInstances(TransportService.class);
            for (TransportService transportService : transportServices) {
                assertThat(transportService.getTaskManager().getBannedTaskIds(), Matchers.empty());
            }
        });
    }

    protected AsyncSearchResponse submitAsyncSearch(SubmitAsyncSearchRequest request) throws ExecutionException, InterruptedException {
        return client(LOCAL_CLUSTER).execute(SubmitAsyncSearchAction.INSTANCE, request).get();
    }

    protected AsyncSearchResponse getAsyncSearch(String id) throws ExecutionException, InterruptedException {
        return client(LOCAL_CLUSTER).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).get();
    }

    protected AsyncStatusResponse getAsyncStatus(String id) throws ExecutionException, InterruptedException {
        return client(LOCAL_CLUSTER).execute(GetAsyncStatusAction.INSTANCE, new GetAsyncStatusRequest(id)).get();
    }

    protected AcknowledgedResponse deleteAsyncSearch(String id) throws ExecutionException, InterruptedException {
        return client().execute(DeleteAsyncResultAction.INSTANCE, new DeleteAsyncResultRequest(id)).get();
    }

    private void setupTwoClusters() throws Exception {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("demo"));
        indexDocs(client(LOCAL_CLUSTER), "demo");
        final InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
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
            configureRemoteCluster(REMOTE_CLUSTER, seedNodes);
            if (randomBoolean()) {
                // Using proxy connections
                allocationFilter.put("index.routing.allocation.exclude._name", String.join(",", seedNodes));
            } else {
                allocationFilter.put("index.routing.allocation.include._name", String.join(",", seedNodes));
            }
        }
        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .indices()
                .prepareCreate("prod")
                .setSettings(Settings.builder().put(allocationFilter.build()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        assertFalse(
            client(REMOTE_CLUSTER).admin()
                .cluster()
                .prepareHealth("prod")
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );
        indexDocs(client(REMOTE_CLUSTER), "prod");
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("f", "v").get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    @Before
    public void resetSearchListenerPlugin() throws Exception {
        SearchListenerPlugin.reset();
    }

    public static class SearchListenerPlugin extends Plugin {
        private static final AtomicReference<CountDownLatch> startedLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> queryLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> failedQueryLatch = new AtomicReference<>();

        static void reset() {
            startedLatch.set(new CountDownLatch(1));
            failedQueryLatch.set(new CountDownLatch(1));
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

        static void waitQueryFailure() throws Exception {
            assertTrue(failedQueryLatch.get().await(60, TimeUnit.SECONDS));
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

                @Override
                public void onFailedQueryPhase(SearchContext searchContext) {
                    // only count failed queries that have a timeout set (to be sure we are listening for our test query)
                    if (searchContext.timeout().millis() > -1) {
                        if (failedQueryLatch.get().getCount() > 0) {
                            failedQueryLatch.get().countDown();
                        }
                    }
                }
            });
            super.onIndexModule(indexModule);
        }
    }
}
