/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.node.ShutdownPrepareService.MAXIMUM_REINDEXING_TIMEOUT_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.is;

/**
 * Test that a wait added during shutdown is necessary for a large reindexing task to complete.
 * The test works as follows:
 * 1. Start a large (reasonably long running) reindexing request on the coordinator-only node.
 * 2. Check that the reindexing task appears on the coordinating node
 * 3. With a 60s timeout value for MAXIMUM_REINDEXING_TIMEOUT_SETTING,
 *    wait for the reindexing task to complete before closing the node
 * 4. Confirm that the reindexing task succeeds with the wait (it will fail without it)
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class ReindexNodeShutdownIT extends ESIntegTestCase {

    protected static final String INDEX = "reindex-shutdown-index";
    protected static final String DEST_INDEX = "dest-index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class);
    }

    protected ReindexRequestBuilder reindex(String nodeName) {
        return new ReindexRequestBuilder(internalCluster().client(nodeName));
    }

    public void testReindexWithShutdown() throws Exception {
        final String masterNodeName = internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();

        /* Maximum time to wait for reindexing tasks to complete before shutdown */
        final Settings coordSettings = Settings.builder()
            .put(MAXIMUM_REINDEXING_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(60))
            .build();
        final String coordNodeName = internalCluster().startCoordinatingOnlyNode(coordSettings);

        ensureStableCluster(3);

        int numDocs = 20000;
        createIndex(numDocs);
        BulkByScrollTask task = createReindexTaskAndShutdown(coordNodeName);

        // Assert that relocation is requested on the reindex task...
        assertBusy(() -> assertThat(task.isRelocationRequested(), is(true)), 1, TimeUnit.SECONDS);
        // ...and that the reindex is given time to complete:
        checkDestinationIndex(dataNodeName, numDocs);
    }

    private void createIndex(int numDocs) {
        // INDEX will be created on the dataNode
        createIndex(INDEX);

        logger.debug("setting up [{}] docs", numDocs);
        indexRandom(
            true,
            false,
            true,
            IntStream.range(0, numDocs)
                .mapToObj(i -> prepareIndex(INDEX).setId(String.valueOf(i)).setSource("n", i))
                .collect(Collectors.toList())
        );

        // Checks that the all documents have been indexed and correctly counted
        assertHitCount(prepareSearch(INDEX).setSize(0).setTrackTotalHits(true), numDocs);
    }

    private BulkByScrollTask createReindexTaskAndShutdown(String coordNodeName) throws Exception {
        AbstractBulkByScrollRequestBuilder<?, ?> builder = reindex(coordNodeName).source(INDEX).destination(DEST_INDEX);
        AbstractBulkByScrollRequest<?> reindexRequest = builder.request();
        reindexRequest.setEligibleForRelocationOnShutdown(true);
        ShutdownPrepareService shutdownPrepareService = internalCluster().getInstance(ShutdownPrepareService.class, coordNodeName);

        // Now execute the reindex action...
        ActionListener<BulkByScrollResponse> reindexListener = new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                assertNull(bulkByScrollResponse.getReasonCancelled());
                logger.debug(bulkByScrollResponse.toString());
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Encountered " + e.toString());
                fail(e, "Encountered " + e.toString());
            }
        };
        internalCluster().client(coordNodeName).execute(ReindexAction.INSTANCE, reindexRequest, reindexListener);

        // Check for reindex task to appear in the tasks list and Immediately stop coordinating node
        BulkByScrollTask task = asInstanceOf(BulkByScrollTask.class, waitForTask(ReindexAction.INSTANCE.name(), coordNodeName));
        assertThat(task.isRelocationRequested(), is(false));
        shutdownPrepareService.prepareForShutdown();
        internalCluster().stopNode(coordNodeName);
        return task;
    }

    // Make sure all documents from the source index have been re-indexed into the destination index
    private void checkDestinationIndex(String dataNodeName, int numDocs) throws Exception {
        assertTrue(indexExists(DEST_INDEX));
        flushAndRefresh(DEST_INDEX);
        assertBusy(() -> { assertHitCount(prepareSearch(DEST_INDEX).setSize(0).setTrackTotalHits(true), numDocs); });
    }

    private static Task waitForTask(String actionName, String nodeName) throws Exception {
        AtomicReference<Task> reindexTask = new AtomicReference<>();
        assertBusy(() -> {
            Map<Long, Task> tasks = internalCluster().getInstance(TransportService.class, nodeName).getTaskManager().getTasks();
            tasks.values()
                .stream()
                .filter(task -> task.getAction().equals(actionName))
                // Skip tasks with a parent because those are children of the task we want
                .filter(task -> task.getParentTaskId().isSet() == false)
                .findAny()
                .ifPresentOrElse(reindexTask::set, () -> fail("Couldn't find task after waiting, tasks=" + tasks));
        }, 10, TimeUnit.SECONDS);
        return reindexTask.get();
    }
}
