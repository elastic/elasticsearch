/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.stats.CCSTelemetrySnapshot;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xpack.async.AsyncResultsIndexPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.ASYNC_FEATURE;
import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.MRT_FEATURE;
import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.Result.CANCELED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class CCSUsageTelemetryAsyncSearchIT extends AbstractMultiClustersTestCase {
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugs = Arrays.asList(
            CrossClusterAsyncSearchIT.SearchListenerPlugin.class,
            AsyncSearch.class,
            AsyncResultsIndexPlugin.class,
            LocalStateCompositeXPackPlugin.class,
            CrossClusterAsyncSearchIT.TestQueryBuilderPlugin.class
        );
        return Stream.concat(super.nodePlugins(clusterAlias).stream(), plugs.stream()).collect(Collectors.toList());
    }

    @Before
    public void resetSearchListenerPlugin() {
        CrossClusterAsyncSearchIT.SearchListenerPlugin.reset();
    }

    private SubmitAsyncSearchRequest makeSearchRequest(String... indices) {
        CrossClusterAsyncSearchIT.SearchListenerPlugin.blockLocalQueryPhase();

        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(indices);
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        request.getSearchRequest().allowPartialSearchResults(false);
        request.getSearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(10));
        if (randomBoolean()) {
            request.setBatchedReduceSize(randomIntBetween(2, 256));
        }

        return request;
    }

    /**
    * Run async search request and get telemetry from it
    */
    private CCSTelemetrySnapshot getTelemetryFromSearch(SubmitAsyncSearchRequest searchRequest) throws Exception {
        // We want to send search to a specific node (we don't care which one) so that we could
        // collect the CCS telemetry from it later
        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        final AsyncSearchResponse response = cluster(LOCAL_CLUSTER).client(nodeName)
            .execute(SubmitAsyncSearchAction.INSTANCE, searchRequest)
            .get();
        // We don't care here too much about the response, we just want to trigger the telemetry collection.
        // So we check it's not null and leave the rest to other tests.
        final String responseId;
        try {
            assertNotNull(response.getSearchResponse());
            responseId = response.getId();
        } finally {
            response.decRef();
        }
        waitForSearchTasksToFinish();
        final AsyncSearchResponse finishedResponse = cluster(LOCAL_CLUSTER).client(nodeName)
            .execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(responseId))
            .get();
        try {
            assertNotNull(finishedResponse.getSearchResponse());
        } finally {
            finishedResponse.decRef();
        }
        return getTelemetrySnapshot(nodeName);

    }

    private void waitForSearchTasksToFinish() throws Exception {
        assertBusy(() -> {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(TransportSearchAction.TYPE.name())
                .get();
            List<TaskInfo> tasks = listTasksResponse.getTasks();
            assertThat(tasks.size(), equalTo(0));

            for (String clusterAlias : remoteClusterAlias()) {
                ListTasksResponse remoteTasksResponse = client(clusterAlias).admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(TransportSearchAction.TYPE.name())
                    .get();
                List<TaskInfo> remoteTasks = remoteTasksResponse.getTasks();
                assertThat(remoteTasks.size(), equalTo(0));
            }
        });

        assertBusy(() -> {
            for (String clusterAlias : remoteClusterAlias()) {
                final Iterable<TransportService> transportServices = cluster(clusterAlias).getInstances(TransportService.class);
                for (TransportService transportService : transportServices) {
                    assertThat(transportService.getTaskManager().getBannedTaskIds(), Matchers.empty());
                }
            }
        });
    }

    /**
     * Create search request for indices and get telemetry from it
     */
    private CCSTelemetrySnapshot getTelemetryFromSearch(String... indices) throws Exception {
        return getTelemetryFromSearch(makeSearchRequest(indices));
    }

    /**
     * Async search on all remotes
     */
    public void testAllRemotesSearch() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SubmitAsyncSearchRequest searchRequest = makeSearchRequest(localIndex, "*:" + remoteIndex);
        boolean minimizeRoundtrips = TransportSearchAction.shouldMinimizeRoundtrips(searchRequest.getSearchRequest());
        CrossClusterAsyncSearchIT.SearchListenerPlugin.negate();

        CCSTelemetrySnapshot telemetry = getTelemetryFromSearch(searchRequest);

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        assertThat(telemetry.getFailureReasons().size(), equalTo(0));
        assertThat(telemetry.getTook().count(), equalTo(1L));
        assertThat(telemetry.getTookMrtTrue().count(), equalTo(minimizeRoundtrips ? 1L : 0L));
        assertThat(telemetry.getTookMrtFalse().count(), equalTo(minimizeRoundtrips ? 0L : 1L));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        assertThat(telemetry.getFeatureCounts().get(ASYNC_FEATURE), equalTo(1L));
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
    }

    /**
     * Search that is cancelled
     */
    public void testCancelledSearch() throws Exception {
        Map<String, Object> testClusterInfo = setupClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SubmitAsyncSearchRequest searchRequest = makeSearchRequest(localIndex, REMOTE1 + ":" + remoteIndex);
        CrossClusterAsyncSearchIT.SearchListenerPlugin.blockLocalQueryPhase();
        CrossClusterAsyncSearchIT.SearchListenerPlugin.blockRemoteQueryPhase();

        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        final AsyncSearchResponse response = cluster(LOCAL_CLUSTER).client(nodeName)
            .execute(SubmitAsyncSearchAction.INSTANCE, searchRequest)
            .get();
        try {
            assertNotNull(response.getSearchResponse());
        } finally {
            response.decRef();
            assertTrue(response.isRunning());
        }
        CrossClusterAsyncSearchIT.SearchListenerPlugin.waitLocalSearchStarted();
        CrossClusterAsyncSearchIT.SearchListenerPlugin.waitRemoteSearchStarted();

        ActionFuture<ListTasksResponse> cancelFuture;
        try {
            ListTasksResponse listTasksResponse = client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareListTasks()
                .setActions(TransportSearchAction.TYPE.name())
                .get();
            List<TaskInfo> tasks = listTasksResponse.getTasks();
            assertThat(tasks.size(), equalTo(1));
            final TaskInfo rootTask = tasks.get(0);

            AtomicReference<List<TaskInfo>> remoteClusterSearchTasks = new AtomicReference<>();
            assertBusy(() -> {
                List<TaskInfo> remoteSearchTasks = client(REMOTE1).admin()
                    .cluster()
                    .prepareListTasks()
                    .get()
                    .getTasks()
                    .stream()
                    .filter(t -> t.action().contains(TransportSearchAction.TYPE.name()))
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
                final Iterable<TransportService> transportServices = cluster(REMOTE1).getInstances(TransportService.class);
                for (TransportService transportService : transportServices) {
                    Collection<CancellableTask> cancellableTasks = transportService.getTaskManager().getCancellableTasks().values();
                    for (CancellableTask cancellableTask : cancellableTasks) {
                        if (cancellableTask.getAction().contains(TransportSearchAction.TYPE.name())) {
                            assertTrue(cancellableTask.getDescription(), cancellableTask.isCancelled());
                        }
                    }
                }
            });

            List<TaskInfo> remoteSearchTasksAfterCancellation = client(REMOTE1).admin()
                .cluster()
                .prepareListTasks()
                .get()
                .getTasks()
                .stream()
                .filter(t -> t.action().contains(TransportSearchAction.TYPE.name()))
                .toList();
            for (TaskInfo taskInfo : remoteSearchTasksAfterCancellation) {
                assertTrue(taskInfo.description(), taskInfo.cancelled());
            }
        } finally {
            CrossClusterAsyncSearchIT.SearchListenerPlugin.allowLocalQueryPhase();
            CrossClusterAsyncSearchIT.SearchListenerPlugin.allowRemoteQueryPhase();
        }

        assertBusy(() -> assertTrue(cancelFuture.isDone()));
        waitForSearchTasksToFinish();

        CCSTelemetrySnapshot telemetry = getTelemetrySnapshot(nodeName);
        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(0L));
        assertThat(telemetry.getFailureReasons().size(), equalTo(1));
        assertThat(telemetry.getFailureReasons().get(CANCELED.getName()), equalTo(1L));
        assertThat(telemetry.getTook().count(), equalTo(0L));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(1.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(1L));
        // Still counts as async search
        assertThat(telemetry.getFeatureCounts().get(ASYNC_FEATURE), equalTo(1L));
    }

    private CCSTelemetrySnapshot getTelemetrySnapshot(String nodeName) {
        var usage = cluster(LOCAL_CLUSTER).getInstance(UsageService.class, nodeName);
        return usage.getCcsUsageHolder().getCCSTelemetrySnapshot();
    }

    private Map<String, Object> setupClusters() {
        String localIndex = "local";
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

        String remoteIndex = "remote";
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
                    .prepareHealth(TEST_REQUEST_TIMEOUT, remoteIndex)
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
