/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.shutdown.GetShutdownStatusAction;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.node.Node.MAXIMUM_REINDEXING_TIMEOUT_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Test that a large reindexing task completes even after the node coordinating the reindexing task
 * is shutdown, due to the wait for reindexing tasks added during shutdown.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class ReindexNodeShutdownIT extends ESIntegTestCase {

    protected static final String INDEX = "reindex-shutdown-index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, ShutdownPlugin.class);
    }

    protected ReindexRequestBuilder reindex(String nodeName) {
        return new ReindexRequestBuilder(internalCluster().client(nodeName));
    }

    public void testReindexWithShutdown() throws Exception {
        final String masterNodeName = internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        final String coordNodeName = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        /*
        final Settings COORD_SETTINGS =
            Settings.builder().put(MAXIMUM_REINDEXING_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build();
        // final String coordNodeName = internalCluster().startCoordinatingOnlyNode(COORD_SETTINGS);
        */

        ensureStableCluster(3);

        createReindexTask(dataNodeName, coordNodeName, masterNodeName);
    }

    private void createReindexTask(final String dataNodeName, final String coordNodeName, final String masterNodeName) throws Exception {

        // INDEX will be created on the datanode
        createIndex(INDEX);

        AbstractBulkByScrollRequestBuilder<?, ?> builder = reindex(coordNodeName).source(INDEX).destination("dest");
        AbstractBulkByScrollRequest<?> reindexRequest = builder.request();

        TaskManager taskManager = internalCluster().getInstance(TransportService.class).getTaskManager();
        // int numDocs = getNumShards(INDEX).numPrimaries * 10 * reindexRequest.getSlices();
        int numDocs = 10000;
        System.out.println("numDocs =" + numDocs + "\n");

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
        assertHitCount(prepareSearch(INDEX).setSize(0), numDocs);

        // Now execute the reindex action...
        ActionListener<BulkByScrollResponse> reindexListener = new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                assertNull(bulkByScrollResponse.getReasonCancelled());
                logger.debug(bulkByScrollResponse.toString());
                System.out.println("Got Response!");
                System.out.println(bulkByScrollResponse.toString());
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Encounterd " + e.toString());
                // System.out.println("Encounterd " + e.toString());
            }
        };
        internalCluster().client(coordNodeName).execute(ReindexAction.INSTANCE, reindexRequest, reindexListener);

        // Check for reindex task to appear in the tasks list and Immediately stop coordinating node
        TaskInfo mainTask = findTask(ReindexAction.INSTANCE.name(), reindexRequest.getSlices());
        putShutdown(coordNodeName);
        GetShutdownStatusAction.Response getResp = client().execute(
            GetShutdownStatusAction.INSTANCE,
            new GetShutdownStatusAction.Request(TEST_REQUEST_TIMEOUT, coordNodeName)
        ).get();
        assertThat(getResp.getShutdownStatuses().get(0).pluginsStatus().getStatus(), equalTo(SingleNodeShutdownMetadata.Status.COMPLETE));
        // System.out.println("Found task and initiated shutdown");
        System.out.println("Shutdown complete");

        assertBusy(() -> assertTrue(indexExists("dest")));
        flushAndRefresh("dest");
        assertTrue("Number of documents in source and dest indexes does not match", waitUntil(() -> {
            final TotalHits totalHits =
                SearchResponseUtils.getTotalHits(client(dataNodeName).prepareSearch("dest").setSize(0).setTrackTotalHits(true));
            return totalHits.relation == TotalHits.Relation.EQUAL_TO && totalHits.value == numDocs;
        }, 10, TimeUnit.SECONDS));
        System.out.println("End of test");
    }

    private void putShutdown(String shutdownNode) throws InterruptedException, ExecutionException {
        PutShutdownNodeAction.Request putShutdownRequest = new PutShutdownNodeAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            shutdownNode,
            SingleNodeShutdownMetadata.Type.REMOVE,
            this.getTestName(),
            null,
            null,
            null
        );
        assertTrue(client().execute(PutShutdownNodeAction.INSTANCE, putShutdownRequest).get().isAcknowledged());
    }

    private static TaskInfo findTask(String actionName, int workerCount) {
        ListTasksResponse tasks;
        long start = System.nanoTime();
        do {
            tasks = clusterAdmin().prepareListTasks().setActions(actionName).setDetailed(true).get();
            tasks.rethrowFailures("Find my task");
            for (TaskInfo taskInfo : tasks.getTasks()) {
                // Skip tasks with a parent because those are children of the task we want to cancel
                if (false == taskInfo.parentTaskId().isSet()) {
                    return taskInfo;
                }
            }
        } while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
        throw new AssertionError("Couldn't find task after waiting tasks=" + tasks.getTasks());
    }
}
