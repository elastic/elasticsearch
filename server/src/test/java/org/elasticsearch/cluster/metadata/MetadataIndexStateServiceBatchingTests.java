/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.PendingClusterTask;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_BLOCKS_WRITE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class MetadataIndexStateServiceBatchingTests extends ESSingleNodeTestCase {

    /*
     * This class is painfully clever and knows all sorts of implementation details about MetadataIndexStateService. It's hard to get
     * around that, as we want to make sure that *batching* of opens and closes and adding index blocks will work as expected.
     *
     * In the case of opens that's easy enough, we just need to get hands-on enough to make sure that the open tasks are executed in a
     * single batch -- we do that by blocking up the master service's tasks queue, then running the opens (so they are added to the queue),
     * and then unblocking the master service again so everything processes in a single batch.
     *
     * But closing and adding blocks are both two-step processes where the second step is itself another cluster state update task, and we
     * want to make sure that those batch up correctly, too. So there we run the same trick as with opens, but twice: we block the
     * master service in order to batch up the initial tasks, then block it again, so that when the first tasks enqueue the second tasks
     * they're doing so into a blocked master service, finally the master service is unblocked once more so that the second tasks process
     * together in a batch.
     */

    public void testBatchOpenIndices() throws Exception {
        final var clusterService = getInstanceFromNode(ClusterService.class);
        final var masterService = clusterService.getMasterService();

        // create some indices, and randomly close some of them
        createIndex("test-1", indicesAdmin().prepareCreate("test-1"));
        createIndex("test-2", indicesAdmin().prepareCreate("test-2"));
        createIndex("test-3", indicesAdmin().prepareCreate("test-3"));
        String[] closedIndices = randomSubsetOf(between(1, 3), "test-1", "test-2", "test-3").toArray(Strings.EMPTY_ARRAY);
        assertAcked(indicesAdmin().prepareClose(closedIndices));
        ensureGreen("test-1", "test-2", "test-3");

        final var assertingListener = closedIndexCountListener(closedIndices.length);
        clusterService.addListener(assertingListener);

        final var block1 = blockMasterService(masterService);
        block1.run(); // wait for block

        // fire off some opens
        final var future1 = indicesAdmin().prepareOpen("test-1").execute();
        final var future2 = indicesAdmin().prepareOpen("test-2", "test-3").execute();

        // check the queue for the open-indices tasks
        assertThat(findPendingTasks(masterService, "open-indices"), hasSize(2));

        block1.run(); // release block

        // assert that the requests were acknowledged
        assertAcked(future1, future2);

        // and assert that all the indices are open
        for (String index : List.of("test-1", "test-2", "test-3")) {
            final var indexMetadata = clusterService.state().metadata().getProject().indices().get(index);
            assertThat(indexMetadata.getState(), is(State.OPEN));
        }

        clusterService.removeListener(assertingListener);
    }

    public void testBatchCloseIndices() throws Exception {
        final var clusterService = getInstanceFromNode(ClusterService.class);
        final var masterService = clusterService.getMasterService();

        // create some indices
        createIndex("test-1", indicesAdmin().prepareCreate("test-1"));
        createIndex("test-2", indicesAdmin().prepareCreate("test-2"));
        createIndex("test-3", indicesAdmin().prepareCreate("test-3"));
        ensureGreen("test-1", "test-2", "test-3");

        final List<String[]> observedClosedIndices = Collections.synchronizedList(new ArrayList<>());
        final ClusterStateListener closedIndicesStateListener = event -> observedClosedIndices.add(
            event.state().metadata().getProject().getConcreteAllClosedIndices()
        );
        clusterService.addListener(closedIndicesStateListener);

        final var block1 = blockMasterService(masterService);
        block1.run(); // wait for block

        // fire off some closes
        final var future1 = indicesAdmin().prepareClose("test-1").execute();
        final var future2 = indicesAdmin().prepareClose("test-2", "test-3").execute();

        // check the queue for the first close tasks (the add-block-index-to-close tasks)
        assertThat(findPendingTasks(masterService, "add-block-index-to-close"), hasSize(2));

        // add *another* block to the end of the pending tasks, then unblock the current block so we can progress,
        // then immediately block again on that new block
        final var block2 = blockMasterService(masterService);
        block1.run(); // release block
        block2.run(); // wait for block

        // wait for the queue to have the second close tasks (the close-indices tasks)
        assertBusy(() -> assertThat(findPendingTasks(masterService, "close-indices"), hasSize(2)));

        // wait for all ongoing tasks to complete on GENERIC to ensure that the batch is fully-formed (see #109187)
        flushThreadPoolExecutor(getInstanceFromNode(ThreadPool.class), ThreadPool.Names.GENERIC);

        block2.run(); // release block

        // assert that the requests were acknowledged
        final var resp1 = safeGet(future1);
        assertAcked(resp1);
        assertThat(resp1.getIndices(), hasSize(1));
        assertThat(resp1.getIndices().get(0).getIndex().getName(), is("test-1"));

        final var resp2 = safeGet(future2);
        assertAcked(resp2);
        assertThat(resp2.getIndices(), hasSize(2));
        assertThat(resp2.getIndices().stream().map(r -> r.getIndex().getName()).toList(), containsInAnyOrder("test-2", "test-3"));

        // and assert that all the indices are closed
        for (String index : List.of("test-1", "test-2", "test-3")) {
            final var indexMetadata = clusterService.state().metadata().getProject().indices().get(index);
            assertThat(indexMetadata.getState(), is(State.CLOSE));
        }

        clusterService.removeListener(closedIndicesStateListener);
        observedClosedIndices.forEach(
            indices -> assertThat("unexpected closed indices: " + Arrays.toString(indices), indices.length, oneOf(0, 3))
        );
    }

    public void testBatchBlockIndices() throws Exception {
        final var clusterService = getInstanceFromNode(ClusterService.class);
        final var masterService = clusterService.getMasterService();

        // create some indices
        createIndex("test-1", indicesAdmin().prepareCreate("test-1"));
        createIndex("test-2", indicesAdmin().prepareCreate("test-2"));
        createIndex("test-3", indicesAdmin().prepareCreate("test-3"));
        ensureGreen("test-1", "test-2", "test-3");

        final var assertingListener = blockedIndexCountListener();
        clusterService.addListener(assertingListener);

        final var block1 = blockMasterService(masterService);
        block1.run(); // wait for block

        // fire off some closes
        final var future1 = indicesAdmin().prepareAddBlock(APIBlock.WRITE, "test-1").execute();
        final var future2 = indicesAdmin().prepareAddBlock(APIBlock.WRITE, "test-2", "test-3").execute();

        // check the queue for the first add-block tasks (the add-index-block tasks)
        assertThat(findPendingTasks(masterService, "add-index-block-[write]"), hasSize(2));

        // add *another* block to the end of the pending tasks, then unblock the current block so we can progress,
        // then immediately block again on that new block
        final var block2 = blockMasterService(masterService);
        block1.run(); // release block
        block2.run(); // wait for block

        // wait for the queue to have the second add-block tasks (the finalize-index-block tasks)
        assertBusy(() -> assertThat(findPendingTasks(masterService, "finalize-index-block-[write]"), hasSize(2)));

        block2.run(); // release block

        // assert that the requests were acknowledged
        final var resp1 = future1.get();
        assertAcked(resp1);
        assertThat(resp1.getIndices(), hasSize(1));
        assertThat(resp1.getIndices().get(0).getIndex().getName(), is("test-1"));

        final var resp2 = future2.get();
        assertAcked(resp2);
        assertThat(resp2.getIndices(), hasSize(2));
        assertThat(resp2.getIndices().stream().map(r -> r.getIndex().getName()).toList(), containsInAnyOrder("test-2", "test-3"));

        // and assert that all the indices are blocked
        for (String index : List.of("test-1", "test-2", "test-3")) {
            final var indexMetadata = clusterService.state().metadata().getProject().indices().get(index);
            assertThat(INDEX_BLOCKS_WRITE_SETTING.get(indexMetadata.getSettings()), is(true));
        }

        clusterService.removeListener(assertingListener);
    }

    public void testBatchRemoveBlocks() throws Exception {
        final var clusterService = getInstanceFromNode(ClusterService.class);
        final var masterService = clusterService.getMasterService();

        // create some indices and add blocks
        createIndex("test-1", indicesAdmin().prepareCreate("test-1"));
        createIndex("test-2", indicesAdmin().prepareCreate("test-2"));
        createIndex("test-3", indicesAdmin().prepareCreate("test-3"));
        assertAcked(indicesAdmin().prepareAddBlock(APIBlock.WRITE, "test-1", "test-2", "test-3"));
        ensureGreen("test-1", "test-2", "test-3");

        final var assertingListener = unblockedIndexCountListener();
        clusterService.addListener(assertingListener);

        final var block1 = blockMasterService(masterService);
        block1.run(); // wait for block

        // fire off some remove blocks
        final var future1 = indicesAdmin().prepareRemoveBlock(APIBlock.WRITE, "test-1").execute();
        final var future2 = indicesAdmin().prepareRemoveBlock(APIBlock.WRITE, "test-2", "test-3").execute();

        // check the queue for the remove-block tasks
        assertThat(findPendingTasks(masterService, "remove-index-block-[write]"), hasSize(2));

        block1.run(); // release block

        // assert that the requests were acknowledged
        final var resp1 = future1.get();
        assertAcked(resp1);
        assertThat(resp1.getIndices(), hasSize(1));
        assertThat(resp1.getIndices().get(0).getIndex().getName(), is("test-1"));

        final var resp2 = future2.get();
        assertAcked(resp2);
        assertThat(resp2.getIndices(), hasSize(2));
        assertThat(resp2.getIndices().stream().map(r -> r.getIndex().getName()).toList(), containsInAnyOrder("test-2", "test-3"));

        // and assert that all the blocks are removed
        for (String index : List.of("test-1", "test-2", "test-3")) {
            final var indexMetadata = clusterService.state().metadata().getProject().indices().get(index);
            assertThat(INDEX_BLOCKS_WRITE_SETTING.get(indexMetadata.getSettings()), is(false));
        }

        clusterService.removeListener(assertingListener);
    }

    private static CheckedRunnable<Exception> blockMasterService(MasterService masterService) {
        final var executionBarrier = new CyclicBarrier(2);
        masterService.createTaskQueue("block", Priority.URGENT, batchExecutionContext -> {
            safeAwait(executionBarrier); // notify test thread that the master service is blocked
            safeAwait(executionBarrier); // wait for test thread to release us
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                taskContext.success(() -> {});
            }
            return batchExecutionContext.initialState();
        }).submitTask("block", new ExpectSuccessTask(), null);
        return () -> safeAwait(executionBarrier);
    }

    private static ClusterStateListener closedIndexCountListener(int closedIndices) {
        return event -> assertThat(event.state().metadata().getProject().getConcreteAllClosedIndices().length, oneOf(0, closedIndices));
    }

    private static ClusterStateListener blockedIndexCountListener() {
        return event -> assertThat(
            event.state()
                .metadata()
                .getProject()
                .stream()
                .filter(indexMetadata -> INDEX_BLOCKS_WRITE_SETTING.get(indexMetadata.getSettings()))
                .count(),
            oneOf(0L, 1L, 2L, 3L)  // Allow intermediate states during batched processing
        );
    }

    private static ClusterStateListener unblockedIndexCountListener() {
        return event -> assertThat(
            event.state()
                .metadata()
                .getProject()
                .stream()
                .filter(indexMetadata -> INDEX_BLOCKS_WRITE_SETTING.get(indexMetadata.getSettings()))
                .count(),
            oneOf(0L, 1L, 2L, 3L)  // Allow intermediate states during batched processing
        );
    }

    private static List<PendingClusterTask> findPendingTasks(MasterService masterService, String taskSourcePrefix) {
        return masterService.pendingTasks().stream().filter(task -> task.getSource().string().startsWith(taskSourcePrefix)).toList();
    }

    /**
     * Task that asserts it does not fail.
     */
    private static class ExpectSuccessTask implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            throw new AssertionError("should not be called", e);
        }
    }
}
