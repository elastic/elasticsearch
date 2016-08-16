/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Engine.Operation.Origin;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.ingest.IngestTestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskInfo;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Test that you can actually cancel a reindex/update-by-query/delete-by-query request and all the plumbing works. Doesn't test all of the
 * different cancellation places - that is the responsibility of {@link AsyncBulkByScrollActionTests} which have more precise control to
 * simulate failures but do not exercise important portion of the stack like transport and task management.
 */
public class CancelTests extends ReindexTestCase {

    protected static final String INDEX = "reindex-cancel-index";
    protected static final String TYPE = "reindex-cancel-type";

    private static final int MIN_OPERATIONS = 2;
    private static final int BLOCKING_OPERATIONS = 1;

    // Semaphore used to allow & block indexing operations during the test
    private static final Semaphore ALLOWED_OPERATIONS = new Semaphore(0);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(IngestTestPlugin.class);
        plugins.add(ReindexCancellationPlugin.class);
        return plugins;
    }

    @BeforeClass
    public static void clearAllowedOperations() {
        ALLOWED_OPERATIONS.drainPermits();
    }

    /**
     * Executes the cancellation test
     */
    private void testCancel(String action, AbstractBulkByScrollRequestBuilder<?, ?> builder, CancelAssertion assertion) throws Exception {
        createIndex(INDEX);

        // Total number of documents created for this test (~10 per primary shard)
        int numDocs = getNumShards(INDEX).numPrimaries * 10;
        ALLOWED_OPERATIONS.release(numDocs);

        indexRandom(true, false, true, IntStream.range(0, numDocs)
                .mapToObj(i -> client().prepareIndex(INDEX, TYPE, String.valueOf(i)).setSource("n", i))
                .collect(Collectors.toList()));

        // Checks that the all documents have been indexed and correctly counted
        assertHitCount(client().prepareSearch(INDEX).setSize(0).get(), numDocs);
        assertThat(ALLOWED_OPERATIONS.drainPermits(), equalTo(0));

        // Scroll by 1 so that cancellation is easier to control
        builder.source().setSize(1);

        // Allow a random number of the documents minus 1
        // to be modified by the reindex action
        int numModifiedDocs = randomIntBetween(MIN_OPERATIONS, numDocs);
        ALLOWED_OPERATIONS.release(numModifiedDocs - BLOCKING_OPERATIONS);

        // Now execute the reindex action...
        ListenableActionFuture<? extends BulkIndexByScrollResponse> future = builder.execute();

        // ... and waits for the indexing operation listeners to block
        awaitBusy(() -> ALLOWED_OPERATIONS.hasQueuedThreads() && ALLOWED_OPERATIONS.availablePermits() == 0);

        // Status should show the task running
        ListTasksResponse tasksList = client().admin().cluster().prepareListTasks().setActions(action).setDetailed(true).get();
        assertThat(tasksList.getNodeFailures(), empty());
        assertThat(tasksList.getTaskFailures(), empty());
        assertThat(tasksList.getTasks(), hasSize(1));
        BulkByScrollTask.Status status = (BulkByScrollTask.Status) tasksList.getTasks().get(0).getStatus();
        assertNull(status.getReasonCancelled());

        // Cancel the request while the reindex action is blocked by the indexing operation listeners.
        // This will prevent further requests from being sent.
        List<TaskInfo> cancelledTasks = client().admin().cluster().prepareCancelTasks().setActions(action).get().getTasks();
        assertThat(cancelledTasks, hasSize(1));

        // The status should now show canceled. The request will still be in the list because it is still blocked.
        tasksList = client().admin().cluster().prepareListTasks().setActions(action).setDetailed(true).get();
        assertThat(tasksList.getNodeFailures(), empty());
        assertThat(tasksList.getTaskFailures(), empty());
        assertThat(tasksList.getTasks(), hasSize(1));
        status = (BulkByScrollTask.Status) tasksList.getTasks().get(0).getStatus();
        assertEquals(CancelTasksRequest.DEFAULT_REASON, status.getReasonCancelled());

        // Unblock the last operation
        ALLOWED_OPERATIONS.release(BLOCKING_OPERATIONS);

        // Checks that no more operations are executed
        assertBusy(() -> assertTrue(ALLOWED_OPERATIONS.availablePermits() == 0 && ALLOWED_OPERATIONS.getQueueLength() == 0));

        // And check the status of the response
        BulkIndexByScrollResponse response = future.get();
        assertThat(response.getReasonCancelled(), equalTo("by user request"));
        assertThat(response.getBulkFailures(), emptyIterable());
        assertThat(response.getSearchFailures(), emptyIterable());

        flushAndRefresh(INDEX);
        assertion.assertThat(response, numDocs, numModifiedDocs);
    }

    public void testReindexCancel() throws Exception {
        testCancel(ReindexAction.NAME, reindex().source(INDEX).destination("dest", TYPE), (response, total, modified) -> {
            assertThat(response, matcher().created(modified).reasonCancelled(equalTo("by user request")));

            refresh("dest");
            assertHitCount(client().prepareSearch("dest").setTypes(TYPE).setSize(0).get(), modified);
        });
    }

    public void testUpdateByQueryCancel() throws Exception {
        BytesReference pipeline = new BytesArray("{\n" +
                "  \"description\" : \"sets processed to true\",\n" +
                "  \"processors\" : [ {\n" +
                "      \"test\" : {}\n" +
                "  } ]\n" +
                "}");
        assertAcked(client().admin().cluster().preparePutPipeline("set-foo", pipeline).get());

        testCancel(UpdateByQueryAction.NAME, updateByQuery().setPipeline("set-foo").source(INDEX), (response, total, modified) -> {
            assertThat(response, matcher().updated(modified).reasonCancelled(equalTo("by user request")));
            assertHitCount(client().prepareSearch(INDEX).setSize(0).setQuery(termQuery("processed", true)).get(), modified);
        });

        assertAcked(client().admin().cluster().deletePipeline(new DeletePipelineRequest("set-foo")).get());
    }

    public void testDeleteByQueryCancel() throws Exception {
        testCancel(DeleteByQueryAction.NAME, deleteByQuery().source(INDEX), (response, total, modified) -> {
            assertThat(response, matcher().deleted(modified).reasonCancelled(equalTo("by user request")));
            assertHitCount(client().prepareSearch(INDEX).setSize(0).get(), total - modified);
        });
    }

    /**
     * {@link CancelAssertion} is used to check the result of the cancel test.
     */
    private interface CancelAssertion {
        void assertThat(BulkIndexByScrollResponse response, int total, int modified);
    }

    public static class ReindexCancellationPlugin extends Plugin {

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexOperationListener(new BlockingOperationListener());
        }
    }

    public static class BlockingOperationListener implements IndexingOperationListener {

        @Override
        public Engine.Index preIndex(Engine.Index index) {
            return preCheck(index, index.type());
        }

        @Override
        public Engine.Delete preDelete(Engine.Delete delete) {
            return preCheck(delete, delete.type());
        }

        private <T extends Engine.Operation> T preCheck(T operation, String type) {
            if ((TYPE.equals(type) == false) || (operation.origin() != Origin.PRIMARY)) {
                return operation;
            }

            try {
                if (ALLOWED_OPERATIONS.tryAcquire(30, TimeUnit.SECONDS)) {
                    return operation;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            throw new IllegalStateException("Something went wrong");
        }
    }
}
