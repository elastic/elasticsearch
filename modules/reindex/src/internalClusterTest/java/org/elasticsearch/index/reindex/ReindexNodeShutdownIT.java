/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class ReindexNodeShutdownIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class);
    }

    protected ReindexRequestBuilder reindex() {
        return new ReindexRequestBuilder(client());
    }

    protected static final String INDEX = "reindex-shutdown-index";

    private void createReindexTask(final String dataNodeName)
    {
        createIndex(INDEX); // Number of shards ?
        AbstractBulkByScrollRequest<ReindexRequest> reindexRequest = reindex().source(INDEX).destination("dest").request();
        TaskManager taskManager = internalCluster().getInstance(TransportService.class).getTaskManager();
        int numDocs = getNumShards(INDEX).numPrimaries * 10 * reindexRequest.getSlices();

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

        // Create shutdown request
        Request putShutdown = createShutdownRequest(dataNodeName);

        // Now execute the reindex action...
        ActionFuture<? extends BulkByScrollResponse> future = client().execute(ReindexAction.INSTANCE, reindexRequest);

        // Check for reindex task to appear in the tasks list and Immediately stop node
        TaskInfo mainTask = findTaskAndShutdown(ReindexAction.INSTANCE.name(), reindexRequest.getSlices());
        //assertOK(client().performRequest(putShutdown));

        // Status should show the task running. Need a way to check the task completed.
        BulkByScrollTask.Status status = (BulkByScrollTask.Status) mainTask.status();
        assertNull(status.getReasonCancelled());
    }

    private static Request createShutdownRequest(final String dataNodeName) throws IOException {
        Request putShutdown = new Request("PUT", "_nodes/" + dataNodeName + "/shutdown");
        String reason = this.getTestName();
        try (XContentBuilder putBody = JsonXContent.contentBuilder()) {
            putBody.startObject();
            {
                putBody.field("reason", reason);
            }
            putBody.endObject();
            putShutdown.setJsonEntity(Strings.toString(putBody));
        }
        return(putShutdown);
    }
    private static TaskInfo findTaskAndShutdown(String actionName, int workerCount) {
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
