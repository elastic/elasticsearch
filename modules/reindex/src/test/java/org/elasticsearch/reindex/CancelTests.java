/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.Operation.Origin;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequestBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.ingest.IngestTestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Test that you can actually cancel a reindex/update-by-query/delete-by-query request and all the plumbing works. Doesn't test all of the
 * different cancellation places - that is the responsibility of AsyncBulkByScrollActionTests which have more precise control to
 * simulate failures but does not exercise important portion of the stack like transport and task management.
 */
public class CancelTests extends ReindexTestCase {

    protected static final String INDEX = "reindex-cancel-index";

    // Semaphore used to allow & block indexing operations during the test
    private static final Semaphore ALLOWED_OPERATIONS = new Semaphore(0);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(IngestTestPlugin.class);
        plugins.add(ReindexCancellationPlugin.class);
        return plugins;
    }

    @Before
    public void clearAllowedOperations() {
        ALLOWED_OPERATIONS.drainPermits();
    }

    /**
     * Executes the cancellation test
     */
    private void testCancel(
        ActionType<BulkByScrollResponse> action,
        AbstractBulkByScrollRequestBuilder<?, ?> builder,
        CancelAssertion assertion,
        Matcher<String> taskDescriptionMatcher
    ) throws Exception {
        createIndex(INDEX);
        // Scroll by 1 so that cancellation is easier to control
        builder.source().setSize(1);
        AbstractBulkByScrollRequest<?> request = builder.request();
        // Total number of documents created for this test (~10 per primary shard per slice)
        int numDocs = getNumShards(INDEX).numPrimaries * 10 * request.getSlices();
        ALLOWED_OPERATIONS.release(numDocs);

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
        assertThat(ALLOWED_OPERATIONS.drainPermits(), equalTo(0));

        /* Allow a random number of the documents less the number of workers
         * to be modified by the reindex action. That way at least one worker
         * is blocked. */
        int numModifiedDocs = randomIntBetween(request.getSlices() * 2, numDocs);
        logger.debug("chose to modify [{}] out of [{}] docs", numModifiedDocs, numDocs);
        ALLOWED_OPERATIONS.release(numModifiedDocs - request.getSlices());

        // Now execute the reindex action...
        ActionFuture<? extends BulkByScrollResponse> future = client().execute(action, request);

        /* ... and wait for the indexing operation listeners to block. It
         * is important to realize that some of the workers might have
         * exhausted their slice while others might have quite a bit left
         * to work on. We can't control that. */
        logger.debug("waiting for updates to be blocked");
        assertBusy(
            () -> assertTrue("updates blocked", ALLOWED_OPERATIONS.hasQueuedThreads() && ALLOWED_OPERATIONS.availablePermits() == 0),
            1,
            TimeUnit.MINUTES
        ); // 10 seconds is usually fine but on heavily loaded machines this can take a while

        // Status should show the task running
        TaskInfo mainTask = findTaskToCancel(action.name(), request.getSlices());
        BulkByScrollTask.Status status = (BulkByScrollTask.Status) mainTask.status();
        assertNull(status.getReasonCancelled());

        // Description shouldn't be empty
        assertThat(mainTask.description(), taskDescriptionMatcher);

        // Cancel the request while the action is blocked by the indexing operation listeners.
        // This will prevent further requests from being sent.
        ListTasksResponse cancelTasksResponse = clusterAdmin().prepareCancelTasks().setTargetTaskId(mainTask.taskId()).get();
        cancelTasksResponse.rethrowFailures("Cancel");
        assertThat(cancelTasksResponse.getTasks(), hasSize(1));

        /* The status should now show canceled. The request will still be in the
         * list because it is (or its children are) still blocked. */
        mainTask = clusterAdmin().prepareGetTask(mainTask.taskId()).get().getTask().getTask();
        status = (BulkByScrollTask.Status) mainTask.status();
        logger.debug("asserting that parent is marked canceled {}", status);
        assertEquals(CancelTasksRequest.DEFAULT_REASON, status.getReasonCancelled());

        if (request.getSlices() > 1) {
            boolean foundCancelled = false;
            ListTasksResponse sliceList = clusterAdmin().prepareListTasks()
                .setTargetParentTaskId(mainTask.taskId())
                .setDetailed(true)
                .get();
            sliceList.rethrowFailures("Fetch slice tasks");
            logger.debug("finding at least one canceled child among {}", sliceList.getTasks());
            for (TaskInfo slice : sliceList.getTasks()) {
                BulkByScrollTask.Status sliceStatus = (BulkByScrollTask.Status) slice.status();
                if (sliceStatus.getReasonCancelled() == null) continue;
                assertEquals(CancelTasksRequest.DEFAULT_REASON, sliceStatus.getReasonCancelled());
                foundCancelled = true;
            }
            assertTrue("Didn't find at least one sub task that was cancelled", foundCancelled);
        }

        logger.debug("unblocking the blocked update");
        ALLOWED_OPERATIONS.release(request.getSlices());

        // Checks that no more operations are executed
        assertBusy(() -> {
            if (request.getSlices() == 1) {
                /* We can only be sure that we've drained all the permits if we only use a single worker. Otherwise some worker may have
                 * exhausted all of its documents before we blocked. */
                assertEquals(0, ALLOWED_OPERATIONS.availablePermits());
            }
            assertEquals(0, ALLOWED_OPERATIONS.getQueueLength());
        });

        // And check the status of the response
        BulkByScrollResponse response;
        try {
            response = future.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof TaskCancelledException).isPresent()) {
                return; // the scroll request was cancelled
            }
            String tasks = clusterAdmin().prepareListTasks().setTargetParentTaskId(mainTask.taskId()).setDetailed(true).get().toString();
            throw new RuntimeException("Exception while waiting for the response. Running tasks: " + tasks, e);
        } finally {
            if (request.getSlices() >= 1) {
                // If we have more than one worker we might not have made all the modifications
                numModifiedDocs -= ALLOWED_OPERATIONS.availablePermits();
            }
        }
        assertThat(response.getReasonCancelled(), equalTo("by user request"));
        assertThat(response.getBulkFailures(), emptyIterable());
        assertThat(response.getSearchFailures(), emptyIterable());

        flushAndRefresh(INDEX);
        assertion.assertThat(response, numDocs, numModifiedDocs);
    }

    public static TaskInfo findTaskToCancel(String actionName, int workerCount) {
        ListTasksResponse tasks;
        long start = System.nanoTime();
        do {
            tasks = clusterAdmin().prepareListTasks().setActions(actionName).setDetailed(true).get();
            tasks.rethrowFailures("Find tasks to cancel");
            for (TaskInfo taskInfo : tasks.getTasks()) {
                // Skip tasks with a parent because those are children of the task we want to cancel
                if (false == taskInfo.parentTaskId().isSet()) {
                    return taskInfo;
                }
            }
        } while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
        throw new AssertionError("Couldn't find task to rethrottle after waiting tasks=" + tasks.getTasks());
    }

    public void testReindexCancel() throws Exception {
        testCancel(ReindexAction.INSTANCE, reindex().source(INDEX).destination("dest"), (response, total, modified) -> {
            assertThat(response, matcher().created(modified).reasonCancelled(equalTo("by user request")));

            refresh("dest");
            assertHitCount(prepareSearch("dest").setSize(0), modified);
        }, equalTo("reindex from [" + INDEX + "] to [dest]"));
    }

    public void testUpdateByQueryCancel() throws Exception {
        putJsonPipeline("set-processed", """
            {
              "description" : "sets processed to true",
              "processors" : [ {
                  "test" : {}
              } ]
            }""");

        testCancel(
            UpdateByQueryAction.INSTANCE,
            updateByQuery().setPipeline("set-processed").source(INDEX),
            (response, total, modified) -> {
                assertThat(response, matcher().updated(modified).reasonCancelled(equalTo("by user request")));
                assertHitCount(prepareSearch(INDEX).setSize(0).setQuery(termQuery("processed", true)), modified);
            },
            equalTo("update-by-query [" + INDEX + "]")
        );

        deletePipeline("set-processed");
    }

    public void testDeleteByQueryCancel() throws Exception {
        testCancel(
            DeleteByQueryAction.INSTANCE,
            deleteByQuery().source(INDEX).filter(QueryBuilders.matchAllQuery()),
            (response, total, modified) -> {
                assertThat(response, matcher().deleted(modified).reasonCancelled(equalTo("by user request")));
                assertHitCount(prepareSearch(INDEX).setSize(0), total - modified);
            },
            equalTo("delete-by-query [" + INDEX + "]")
        );
    }

    public void testReindexCancelWithWorkers() throws Exception {
        testCancel(
            ReindexAction.INSTANCE,
            reindex().source(INDEX).filter(QueryBuilders.matchAllQuery()).destination("dest").setSlices(5),
            (response, total, modified) -> {
                assertThat(response, matcher().created(modified).reasonCancelled(equalTo("by user request")).slices(hasSize(5)));
                refresh("dest");
                assertHitCount(prepareSearch("dest").setSize(0), modified);
            },
            equalTo("reindex from [" + INDEX + "] to [dest]")
        );
    }

    public void testUpdateByQueryCancelWithWorkers() throws Exception {
        putJsonPipeline("set-processed", """
            {
              "description" : "sets processed to true",
              "processors" : [ {
                  "test" : {}
              } ]
            }""");

        testCancel(
            UpdateByQueryAction.INSTANCE,
            updateByQuery().setPipeline("set-processed").source(INDEX).setSlices(5),
            (response, total, modified) -> {
                assertThat(response, matcher().updated(modified).reasonCancelled(equalTo("by user request")).slices(hasSize(5)));
                assertHitCount(prepareSearch(INDEX).setSize(0).setQuery(termQuery("processed", true)), modified);
            },
            equalTo("update-by-query [" + INDEX + "]")
        );

        deletePipeline("set-processed");
    }

    public void testDeleteByQueryCancelWithWorkers() throws Exception {
        testCancel(
            DeleteByQueryAction.INSTANCE,
            deleteByQuery().source(INDEX).filter(QueryBuilders.matchAllQuery()).setSlices(5),
            (response, total, modified) -> {
                assertThat(response, matcher().deleted(modified).reasonCancelled(equalTo("by user request")).slices(hasSize(5)));
                assertHitCount(prepareSearch(INDEX).setSize(0), total - modified);
            },
            equalTo("delete-by-query [" + INDEX + "]")
        );
    }

    /**
     * Used to check the result of the cancel test.
     */
    private interface CancelAssertion {
        void assertThat(BulkByScrollResponse response, int total, int modified);
    }

    public static class ReindexCancellationPlugin extends Plugin {

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexOperationListener(new BlockingOperationListener());
        }
    }

    public static class BlockingOperationListener implements IndexingOperationListener {
        private static final Logger log = LogManager.getLogger(CancelTests.class);

        @Override
        public Engine.Index preIndex(ShardId shardId, Engine.Index index) {
            return preCheck(index);
        }

        @Override
        public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
            return preCheck(delete);
        }

        private <T extends Engine.Operation> T preCheck(T operation) {
            if ((operation.origin() != Origin.PRIMARY)) {
                return operation;
            }

            try {
                log.debug("checking");
                if (ALLOWED_OPERATIONS.tryAcquire(30, TimeUnit.SECONDS)) {
                    log.debug("passed");
                    return operation;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            throw new IllegalStateException("Something went wrong");
        }
    }
}
