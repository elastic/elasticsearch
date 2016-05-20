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
import org.elasticsearch.action.admin.cluster.node.tasks.list.TaskInfo;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests that you can actually cancel a delete-by-query request and all the plumbing works. Doesn't test all of the different cancellation
 * places - that is the responsibility of {@link AsyncBulkByScrollActionTests} which have more precise control to simulate failures but do
 * not exercise important portion of the stack like transport and task management.
 */
public class DeleteByQueryCancelTests extends ReindexTestCase {

    private static final String INDEX = "test-delete-by-query";
    private static final String TYPE = "test";

    private static final int MAX_DELETIONS = 10;
    private static final CyclicBarrier barrier = new CyclicBarrier(2);

    @Override
    protected int numberOfShards() {
        // Only 1 shard and no replica so that test execution
        // can be easily controlled within a {@link IndexingOperationListener#preDelete}
        return 1;
    }

    @Override
    protected int numberOfReplicas() {
        // Only 1 shard and no replica so that test execution
        // can be easily controlled within a {@link IndexingOperationListener#preDelete}
        return 0;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DeleteByQueryCancellationPlugin.class);
        return plugins;
    }

    public void testCancel() throws Exception {
        createIndex(INDEX);

        int totalNumShards = getNumShards(INDEX).totalNumShards;

        // Number of documents to be deleted in this test
        final int nbDocsToDelete = totalNumShards * MAX_DELETIONS;

        // Total number of documents that will be created in this test
        final int nbDocs = nbDocsToDelete * randomIntBetween(1, 5);
        for (int i = 0; i < nbDocs; i++) {
            indexRandom(false, client().prepareIndex(INDEX, TYPE, String.valueOf(i)).setSource("n", i));
        }

        refresh(INDEX);
        assertHitCount(client().prepareSearch(INDEX).setSize(0).get(), nbDocs);

        // Executes the delete by query; each shard will block after MAX_DELETIONS
        DeleteByQueryRequestBuilder deleteByQuery = deleteByQuery().source("_all");
        deleteByQuery.source().setSize(1);

        ListenableActionFuture<BulkIndexByScrollResponse> future = deleteByQuery.execute();

        // Waits for the indexing operation listener to block
        barrier.await(30, TimeUnit.SECONDS);

        // Status should show running
        ListTasksResponse tasksList = client().admin().cluster().prepareListTasks()
                .setActions(DeleteByQueryAction.NAME).setDetailed(true).get();
        assertThat(tasksList.getNodeFailures(), empty());
        assertThat(tasksList.getTaskFailures(), empty());
        assertThat(tasksList.getTasks(), hasSize(1));
        BulkByScrollTask.Status status = (BulkByScrollTask.Status) tasksList.getTasks().get(0).getStatus();
        assertNull(status.getReasonCancelled());

        // Cancel the request while the deletions are blocked. This will prevent further deletions requests from being sent.
        List<TaskInfo> cancelledTasks = client().admin().cluster().prepareCancelTasks()
                .setActions(DeleteByQueryAction.NAME).get().getTasks();
        assertThat(cancelledTasks, hasSize(1));

        // The status should now show canceled. The request will still be in the list because the script is still blocked.
        tasksList = client().admin().cluster().prepareListTasks().setActions(DeleteByQueryAction.NAME).setDetailed(true).get();
        assertThat(tasksList.getNodeFailures(), empty());
        assertThat(tasksList.getTaskFailures(), empty());
        assertThat(tasksList.getTasks(), hasSize(1));
        status = (BulkByScrollTask.Status) tasksList.getTasks().get(0).getStatus();
        assertEquals(CancelTasksRequest.DEFAULT_REASON, status.getReasonCancelled());

        // Now unblock the listener so that it can proceed
        barrier.await();

        // And check the status of the response
        BulkIndexByScrollResponse response = future.get();
        assertThat(response, matcher()
                .deleted(lessThanOrEqualTo((long) MAX_DELETIONS)).batches(MAX_DELETIONS).reasonCancelled(equalTo("by user request")));
    }


    public static class DeleteByQueryCancellationPlugin extends Plugin {

        @Override
        public String name() {
            return "delete-by-query-cancellation";
        }

        @Override
        public String description() {
            return "See " + DeleteByQueryCancellationPlugin.class.getName();
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexOperationListener(new BlockingDeleteListener());
        }
    }

    /**
     * A {@link IndexingOperationListener} that allows a given number of documents to be deleted
     * and then blocks until it is notified to proceed.
     */
    public static class BlockingDeleteListener implements IndexingOperationListener {

        private final CountDown blockAfter = new CountDown(MAX_DELETIONS);

        @Override
        public Engine.Delete preDelete(Engine.Delete delete) {
            if (blockAfter.isCountedDown() || (TYPE.equals(delete.type()) == false)) {
                return delete;
            }

            if (blockAfter.countDown()) {
                try {
                    // Tell the test we've deleted enough documents.
                    barrier.await(30, TimeUnit.SECONDS);

                    // Wait for the test to tell us to proceed.
                    barrier.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }
            return delete;
        }
    }
}
