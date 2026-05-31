/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.RethrottleRequestBuilder;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.node.ShutdownPrepareService.MAXIMUM_REINDEXING_TIMEOUT_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/// Remote-path counterpart of [ReindexRelocationOnShutdownIT]: end-to-end test that a reindex task with `RemoteInfo`
/// relocates from a coordinating-only node to a data node when the coordinator shuts down.
@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class RemoteReindexRelocationOnShutdownIT extends ESIntegTestCase {

    private static final String SOURCE = "remote-reindex-relocation-source";
    private static final String DEST = "remote-reindex-relocation-dest";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // MainRestPlugin: remote client probes `GET /` for the version.
        return List.of(ReindexPlugin.class, MainRestPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        // RemoteInfo needs a real HTTP endpoint on the data node.
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "*:*")
            .build();
    }

    public void testRemoteReindexTaskRelocatesOnNodeShutdown() throws Exception {
        assumeTrue("pit relocation must be enabled", SearchService.PIT_RELOCATION_FEATURE_FLAG.isEnabled());

        internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();

        final Settings coordSettings = Settings.builder()
            .put(MAXIMUM_REINDEXING_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(30))
            .build();
        final String coordNodeName = internalCluster().startCoordinatingOnlyNode(coordSettings);

        ensureStableCluster(3);

        // Exceed the default cap so the cache substitution is meaningfully exercised across the relocation boundary.
        final int numDocs = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO + randomIntBetween(
            1,
            SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO
        );
        createIndex(SOURCE);
        indexRandom(true, SOURCE, numDocs);

        // Use the data node's HTTP endpoint; the coord node's goes away on shutdown.
        final InetSocketAddress dataNodeHttp = internalCluster().getInstance(HttpServerTransport.class, dataNodeName)
            .boundAddress()
            .publishAddress()
            .address();

        // Heavily throttle so the task can't complete before shutdown.
        final ReindexRequest request = new ReindexRequest().setSourceIndices(SOURCE)
            .setDestIndex(DEST)
            .setRefresh(true)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setRequestsPerSecond(0.000001f)
            .setRemoteInfo(
                new RemoteInfo(
                    "http",
                    dataNodeHttp.getHostString(),
                    dataNodeHttp.getPort(),
                    null,
                    new BytesArray("{\"match_all\":{}}"),
                    null,
                    null,
                    Map.of(),
                    RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                    RemoteInfo.DEFAULT_CONNECT_TIMEOUT
                )
            );
        request.getSearchRequest().source().size(1000);

        final PlainActionFuture<BulkByPaginatedSearchResponse> reindexFuture = new PlainActionFuture<>();
        internalCluster().client(coordNodeName).execute(ReindexAction.INSTANCE, request, reindexFuture);

        final TaskId reindexTaskId = waitForRootReindexTask(coordNodeName);

        final ShutdownPrepareService shutdownPrepareService = internalCluster().getInstance(ShutdownPrepareService.class, coordNodeName);
        // Mark the task for relocation, then unblock the throttled task so it hits its next batch boundary
        // and throws TaskRelocatedException.
        shutdownPrepareService.prepareForShutdown();
        unthrottleReindex(reindexTaskId);
        internalCluster().stopNode(coordNodeName);

        // The original reindex on the coord throws TaskRelocatedException
        expectThrows(TaskRelocatedException.class, () -> reindexFuture.actionGet(TimeValue.timeValueSeconds(30)));

        // GetTask follows the relocation chain (default) so the original task id resolves to the resumed task.
        final GetTaskResponse relocatedTaskFinished = clusterAdmin().prepareGetTask(reindexTaskId)
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(60))
            .get();
        assertTrue("relocated remote reindex should complete", relocatedTaskFinished.getTask().isCompleted());

        // Status#total must survive the relocation boundary; a regression in cache substitution would clamp it to 0.
        final Map<String, Object> responseMap = relocatedTaskFinished.getTask().getResponseAsMap();
        final var reportedTotal = (Integer) responseMap.get(BulkByPaginatedSearchTask.Status.TOTAL_FIELD);
        assertNotNull("relocated remote reindex response must include Status#total", reportedTotal);
        assertEquals("relocated remote reindex Status#total must equal numDocs", numDocs, reportedTotal.intValue());
        assertHitCount(prepareSearch(DEST).setSize(0).setTrackTotalHits(true), numDocs);
    }

    /// Waits until a top-level reindex task is visible on the given node and returns its id.
    private TaskId waitForRootReindexTask(String nodeName) throws Exception {
        SetOnce<TaskId> capturedId = new SetOnce<>();
        assertBusy(() -> {
            ListTasksResponse tasks = clusterAdmin().prepareListTasks(nodeName)
                .setActions(ReindexAction.INSTANCE.name())
                .setDetailed(true)
                .get();
            tasks.rethrowFailures("find reindex task");
            for (TaskInfo taskInfo : tasks.getTasks()) {
                if (taskInfo.parentTaskId().isSet() == false) {
                    capturedId.set(taskInfo.taskId());
                    return;
                }
            }
            fail("no root reindex task on [" + nodeName + "]: " + tasks.getTasks());
        }, 30, TimeUnit.SECONDS);
        return capturedId.get();
    }

    /// Removes the throttle so the task wakes up and reaches its next batch-boundary relocation check.
    /// `setFollowRelocations(true)` lets the rethrottle target the original id even after the task has relocated.
    /// Wrapped in `assertBusy` to retry on any transient failure (e.g. the task hasn't initialized to leader/worker yet).
    private void unthrottleReindex(TaskId reindexTaskId) throws Exception {
        assertBusy(() -> {
            try {
                new RethrottleRequestBuilder(client()).setTargetTaskId(reindexTaskId)
                    .setRequestsPerSecond(Float.POSITIVE_INFINITY)
                    .setFollowRelocations(true)
                    .get()
                    .rethrowFailures("rethrottle after relocation");
            } catch (Exception e) {
                throw new AssertionError("rethrottle failed", e);
            }
        }, 30, TimeUnit.SECONDS);
    }
}
