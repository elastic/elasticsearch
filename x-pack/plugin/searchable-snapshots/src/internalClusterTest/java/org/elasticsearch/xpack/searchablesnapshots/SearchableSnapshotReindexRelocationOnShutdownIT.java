/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.TaskRelocatedException;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.RethrottleRequestBuilder;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.reindex.management.ReindexManagementPlugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.node.ShutdownPrepareService.MAXIMUM_REINDEXING_TIMEOUT_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class SearchableSnapshotReindexRelocationOnShutdownIT extends BaseSearchableSnapshotsIntegTestCase {

    private static final String DEST = "reindex-ss-relocation-dest";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ReindexPlugin.class);
        plugins.add(ReindexManagementPlugin.class);
        return Collections.unmodifiableList(plugins);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "*:*")
            .build();
    }

    /**
     * Tests reindex relocation when the source index is a searchable snapshot. Reindex resilience is built upon point-in-time
     * search relocation, which we guarantee to succeed by shutting down the coordinating node containing the reindexing task
     * and not the data node holding the search shards needed by PIT.
     */
    public void testReindexRelocatesWithSearchableSnapshotSource() throws Exception {
        disableRepoConsistencyCheck("When the assumeTrue below fails for REINDEX_RESILIENCE_ENABLED, no node has been started");
        assumeTrue("reindex resilience must be enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        assumeTrue("pit relocation must be enabled", SearchService.PIT_RELOCATION_FEATURE_FLAG.isEnabled());

        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final Settings coordSettings = Settings.builder()
            .put(MAXIMUM_REINDEXING_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(30))
            .build();
        final String coordNodeName = internalCluster().startCoordinatingOnlyNode(coordSettings);

        ensureStableCluster(internalCluster().getNodeNames().length);

        final String backingIndex = randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        final String mountedSource = randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        final String repoName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepository(repoName, "fs");

        assertAcked(prepareCreate(backingIndex, indexSettings(1, 0).put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));
        ensureGreen(backingIndex);

        final int numDocs = randomIntBetween(10, 40);
        indexRandom(
            true,
            false,
            true,
            IntStream.range(0, numDocs)
                .mapToObj(i -> prepareIndex(backingIndex).setId(String.valueOf(i)).setSource("n", i))
                .collect(Collectors.toList())
        );
        assertHitCount(prepareSearch(backingIndex).setSize(0).setTrackTotalHits(true), numDocs);

        createSnapshot(repoName, snapshotName, List.of(backingIndex));
        assertAcked(indicesAdmin().prepareDelete(backingIndex));

        // Do not pass index.number_of_shards (or full indexSettings) here since restore/mount forbids changing shard count.
        // Shard counts comes from the snapshot.
        mountSnapshot(repoName, snapshotName, backingIndex, mountedSource, Settings.EMPTY, Storage.FULL_COPY);
        ensureGreen(mountedSource);
        assertHitCount(prepareSearch(mountedSource).setSize(0).setTrackTotalHits(true), numDocs);

        createIndex(DEST, indexSettings(1, 0).build());

        final ReindexRequest request = new ReindexRequest().setSourceIndices(mountedSource)
            .setDestIndex(DEST)
            .setRefresh(true)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setRequestsPerSecond(0.000001f);
        request.getSearchRequest().source().size(1);

        final CountDownLatch listenerDone = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<BulkByScrollResponse> success = new AtomicReference<>();
        internalCluster().client(coordNodeName).execute(ReindexAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                success.set(bulkByScrollResponse);
                listenerDone.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                listenerDone.countDown();
            }
        });

        waitForRootReindexTask(coordNodeName);

        final ShutdownPrepareService shutdownPrepareService = internalCluster().getInstance(ShutdownPrepareService.class, coordNodeName);
        shutdownPrepareService.prepareForShutdown();
        rethrottleRunningRootReindex(numDocs);
        internalCluster().stopNode(coordNodeName);

        assertTrue("reindex listener should complete", listenerDone.await(30, TimeUnit.SECONDS));

        final Throwable error = failure.get();
        final BulkByScrollResponse response = success.get();
        assertThat(ExceptionsHelper.unwrapCause(error), instanceOf(TaskRelocatedException.class));
        final TaskRelocatedException relocated = (TaskRelocatedException) ExceptionsHelper.unwrapCause(error);
        final String relocatedTaskIdString = relocated.getRelocatedTaskId().orElseThrow();
        assertNull(response);

        // Wait until the relocated task finishes (including persisting to `.tasks` when shouldStoreResult is true)
        // so tear-down does not race with async task-result indexing.
        final GetTaskResponse relocatedTaskFinished = clusterAdmin().prepareGetTask(new TaskId(relocatedTaskIdString))
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(60))
            .get();
        assertTrue("relocated reindex should complete", relocatedTaskFinished.getTask().isCompleted());

        // Asserts that the reindexing task is relocated to another node and succeeds
        assertBusy(() -> {
            assertTrue(indexExists(DEST));
            flushAndRefresh(DEST);
            assertHitCount(prepareSearch(DEST).setSize(0).setTrackTotalHits(true), numDocs);
        }, 30, TimeUnit.SECONDS);
    }

    private void waitForRootReindexTask(String nodeName) throws Exception {
        assertBusy(() -> {
            ListTasksResponse tasks = clusterAdmin().prepareListTasks(nodeName)
                .setActions(ReindexAction.INSTANCE.name())
                .setDetailed(true)
                .get();
            tasks.rethrowFailures("find reindex task");
            for (TaskInfo taskInfo : tasks.getTasks()) {
                if (taskInfo.parentTaskId().isSet() == false) {
                    return;
                }
            }
            fail("no root reindex task on [" + nodeName + "]: " + tasks.getTasks());
        }, 30, TimeUnit.SECONDS);
    }

    private void rethrottleRunningRootReindex(int numDocs) throws Exception {
        assertBusy(() -> {
            ListTasksResponse tasks = clusterAdmin().prepareListTasks().setActions(ReindexAction.INSTANCE.name()).setDetailed(true).get();
            tasks.rethrowFailures("list reindex tasks for rethrottle");
            for (TaskInfo taskInfo : tasks.getTasks()) {
                if (taskInfo.parentTaskId().isSet()) {
                    continue;
                }
                try {
                    ListTasksResponse rethrottleResponse = new RethrottleRequestBuilder(client()).setTargetTaskId(taskInfo.taskId())
                        // Forces the reindexing task to still take 2 seconds, giving enough time for the node to shut down
                        .setRequestsPerSecond((float) numDocs / 2)
                        .get();
                    rethrottleResponse.rethrowFailures("rethrottle after relocation");
                    return;
                } catch (ElasticsearchException e) {
                    Throwable unwrapped = ExceptionsHelper.unwrap(e, IllegalArgumentException.class);
                    if (unwrapped == null) {
                        throw e;
                    }
                    assertThat(
                        unwrapped.getMessage(),
                        equalTo(
                            "task ["
                                + taskInfo.taskId().getId()
                                + "] has not yet been initialized to the point where it knows how to rethrottle itself"
                        )
                    );
                    throw new AssertionError("rethrottle before task ready", e);
                }
            }
            throw new AssertionError("no root reindex task found for rethrottle after relocation");
        }, 30, TimeUnit.SECONDS);
    }
}
