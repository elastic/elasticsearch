/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.action.ShardFollowTasksExecutor.ChunkWorker;
import org.elasticsearch.xpack.ccr.action.ShardFollowTasksExecutor.ChunksCoordinator;
import org.elasticsearch.xpack.ccr.action.ShardFollowTasksExecutor.IndexMetadataVersionChecker;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsAction;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsRequest;
import org.elasticsearch.xpack.ccr.action.bulk.BulkShardOperationsResponse;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static org.elasticsearch.xpack.ccr.action.ShardFollowTasksExecutor.PROCESSOR_RETRY_LIMIT;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ChunksCoordinatorTests extends ESTestCase {

    public void testCreateChunks() {
        Client client = mock(Client.class);
        ThreadPool threadPool = createMockThreadPool(false);
        
        ShardId leaderShardId = new ShardId("index1", "index1", 0);
        ShardId followShardId = new ShardId("index2", "index1", 0);

        IndexMetadataVersionChecker checker = new IndexMetadataVersionChecker(leaderShardId.getIndex(),
                followShardId.getIndex(), client, client);
        ChunksCoordinator coordinator = new ChunksCoordinator(client, client, threadPool, checker, 1024, 1,
                Long.MAX_VALUE,
            leaderShardId, followShardId, e -> {}, () -> true, value -> {});
        coordinator.createChucks(0, 1023);
        List<long[]> result = new ArrayList<>(coordinator.getChunkWorkerQueue());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0)[0], equalTo(0L));
        assertThat(result.get(0)[1], equalTo(1023L));

        coordinator.getChunkWorkerQueue().clear();
        coordinator.createChucks(0, 2047);
        result = new ArrayList<>(coordinator.getChunkWorkerQueue());
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0)[0], equalTo(0L));
        assertThat(result.get(0)[1], equalTo(1023L));
        assertThat(result.get(1)[0], equalTo(1024L));
        assertThat(result.get(1)[1], equalTo(2047L));

        coordinator.getChunkWorkerQueue().clear();
        coordinator.createChucks(0, 4095);
        result = new ArrayList<>(coordinator.getChunkWorkerQueue());
        assertThat(result.size(), equalTo(4));
        assertThat(result.get(0)[0], equalTo(0L));
        assertThat(result.get(0)[1], equalTo(1023L));
        assertThat(result.get(1)[0], equalTo(1024L));
        assertThat(result.get(1)[1], equalTo(2047L));
        assertThat(result.get(2)[0], equalTo(2048L));
        assertThat(result.get(2)[1], equalTo(3071L));
        assertThat(result.get(3)[0], equalTo(3072L));
        assertThat(result.get(3)[1], equalTo(4095L));

        coordinator.getChunkWorkerQueue().clear();
        coordinator.createChucks(4096, 8196);
        result = new ArrayList<>(coordinator.getChunkWorkerQueue());
        assertThat(result.size(), equalTo(5));
        assertThat(result.get(0)[0], equalTo(4096L));
        assertThat(result.get(0)[1], equalTo(5119L));
        assertThat(result.get(1)[0], equalTo(5120L));
        assertThat(result.get(1)[1], equalTo(6143L));
        assertThat(result.get(2)[0], equalTo(6144L));
        assertThat(result.get(2)[1], equalTo(7167L));
        assertThat(result.get(3)[0], equalTo(7168L));
        assertThat(result.get(3)[1], equalTo(8191L));
        assertThat(result.get(4)[0], equalTo(8192L));
        assertThat(result.get(4)[1], equalTo(8196L));
    }

    public void testCoordinator() throws Exception {
        Client client = createClientMock();
        ThreadPool threadPool = createMockThreadPool(false);
        mockShardChangesApiCall(client);
        mockBulkShardOperationsApiCall(client);
        ShardId leaderShardId = new ShardId("index1", "index1", 0);
        ShardId followShardId = new ShardId("index2", "index1", 0);

        Consumer<Exception> handler = e -> assertThat(e, nullValue());
        int concurrentProcessors = randomIntBetween(1, 4);
        int batchSize = randomIntBetween(1, 1000);
        IndexMetadataVersionChecker checker = new IndexMetadataVersionChecker(leaderShardId.getIndex(),
                followShardId.getIndex(), client, client);
        ChunksCoordinator coordinator = new ChunksCoordinator(client, client, threadPool, checker, batchSize,
            concurrentProcessors, Long.MAX_VALUE, leaderShardId, followShardId, handler,
            () -> true, value -> {});

        int numberOfOps = randomIntBetween(batchSize, batchSize * 20);
        long from = randomInt(1000);
        long to = from + numberOfOps - 1;
        int expectedNumberOfChunks = numberOfOps / batchSize;
        if (numberOfOps % batchSize > 0) {
            expectedNumberOfChunks++;
        }
        coordinator.start(from, to);
        assertThat(coordinator.getChunkWorkerQueue().size(), equalTo(0));
        verify(client, times(expectedNumberOfChunks)).execute(same(ShardChangesAction.INSTANCE),
                any(ShardChangesAction.Request.class), any());
        verify(client, times(expectedNumberOfChunks)).execute(same(BulkShardOperationsAction.INSTANCE),
                any(BulkShardOperationsRequest.class), any());
    }

    public void testCoordinator_failure() throws Exception {
        Exception expectedException = new RuntimeException("throw me");
        Client client = createClientMock();
        ThreadPool threadPool = createMockThreadPool(false);
        boolean shardChangesActionApiCallFailed;
        if (randomBoolean()) {
            shardChangesActionApiCallFailed = true;
            doThrow(expectedException).when(client).execute(same(ShardChangesAction.INSTANCE),
                    any(ShardChangesAction.Request.class), any());
        } else {
            shardChangesActionApiCallFailed = false;
            mockShardChangesApiCall(client);
            doThrow(expectedException).when(client).execute(same(BulkShardOperationsAction.INSTANCE),
                    any(BulkShardOperationsRequest.class), any());
        }
        Executor ccrExecutor = Runnable::run;
        ShardId leaderShardId = new ShardId("index1", "index1", 0);
        ShardId followShardId = new ShardId("index2", "index1", 0);

        Consumer<Exception> handler = e -> {
            assertThat(e, notNullValue());
            assertThat(e, sameInstance(expectedException));
        };
        IndexMetadataVersionChecker checker = new IndexMetadataVersionChecker(leaderShardId.getIndex(),
                followShardId.getIndex(), client, client);
        ChunksCoordinator coordinator = new ChunksCoordinator(client, client, threadPool, checker,10, 1, Long.MAX_VALUE,
            leaderShardId, followShardId, handler, () -> true, value -> {});
        coordinator.start(0, 19);
        
        assertThat(coordinator.getChunkWorkerQueue().size(), equalTo(1));
        verify(client, times(1)).execute(same(ShardChangesAction.INSTANCE), any(ShardChangesAction.Request.class),
                any());
        verify(client, times(shardChangesActionApiCallFailed ? 0 : 1)).execute(same(BulkShardOperationsAction.INSTANCE),
                any(BulkShardOperationsRequest.class), any());
    }

    public void testCoordinator_concurrent() throws Exception {
        Client client = createClientMock();
        ThreadPool threadPool = createMockThreadPool(true);
        mockShardChangesApiCall(client);
        mockBulkShardOperationsApiCall(client);
        ShardId leaderShardId = new ShardId("index1", "index1", 0);
        ShardId followShardId = new ShardId("index2", "index1", 0);

        AtomicBoolean calledOnceChecker = new AtomicBoolean(false);
        AtomicReference<Exception> failureHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        Consumer<Exception> handler = e -> {
            if (failureHolder.compareAndSet(null, e) == false) {
                // This handler should only be called once, irregardless of the number of concurrent processors
                calledOnceChecker.set(true);
            }
            latch.countDown();
        };
        IndexMetadataVersionChecker checker = new IndexMetadataVersionChecker(leaderShardId.getIndex(),
            followShardId.getIndex(), client, client);
        LongConsumer processedGlobalCheckpointHandler = value -> {
            if (value == 999999) {
                latch.countDown();
            }
        };
        ChunksCoordinator coordinator = new ChunksCoordinator(client, client, threadPool, checker, 1000, 4, Long.MAX_VALUE,
            leaderShardId, followShardId, handler, () -> true, processedGlobalCheckpointHandler);
        coordinator.start(0, 999999);
        latch.await();
        assertThat(coordinator.getChunkWorkerQueue().size(), equalTo(0));
        verify(client, times(1000)).execute(same(ShardChangesAction.INSTANCE), any(ShardChangesAction.Request.class), any());
        verify(client, times(1000)).execute(same(BulkShardOperationsAction.INSTANCE), any(BulkShardOperationsRequest.class), any());
        assertThat(calledOnceChecker.get(), is(false));
    }

    public void testChunkWorker() {
        Client client = createClientMock();
        Queue<long[]> chunks = new LinkedList<>();
        mockShardChangesApiCall(client);
        mockBulkShardOperationsApiCall(client);
        Executor ccrExecutor = Runnable::run;
        ShardId leaderShardId = new ShardId("index1", "index1", 0);
        ShardId followShardId = new ShardId("index2", "index1", 0);
        IndexMetadataVersionChecker checker = new IndexMetadataVersionChecker(leaderShardId.getIndex(),
                followShardId.getIndex(), client, client);

        boolean[] invoked = new boolean[1];
        Exception[] exception = new Exception[1];
        Consumer<Exception> handler = e -> {invoked[0] = true;exception[0] = e;};
        ChunkWorker chunkWorker = new ChunkWorker(client, client, chunks, ccrExecutor, checker, leaderShardId,
                followShardId, handler);
        chunkWorker.start(0, 10, Long.MAX_VALUE);
        assertThat(invoked[0], is(true));
        assertThat(exception[0], nullValue());
    }

    public void testChunkWorkerRetry() {
        Client client = createClientMock();
        Queue<long[]> chunks = new LinkedList<>();
        mockBulkShardOperationsApiCall(client);
        int testRetryLimit = randomIntBetween(1, PROCESSOR_RETRY_LIMIT - 1);
        mockShardCangesApiCallWithRetry(client, testRetryLimit, new ConnectException("connection exception"));

        Executor ccrExecutor = Runnable::run;
        ShardId leaderShardId = new ShardId("index1", "index1", 0);
        ShardId followShardId = new ShardId("index2", "index1", 0);
        IndexMetadataVersionChecker checker = new IndexMetadataVersionChecker(leaderShardId.getIndex(),
                followShardId.getIndex(), client, client);

        boolean[] invoked = new boolean[1];
        Exception[] exception = new Exception[1];
        Consumer<Exception> handler = e -> {invoked[0] = true;exception[0] = e;};
        ChunkWorker chunkWorker = new ChunkWorker(client, client, chunks, ccrExecutor, checker, leaderShardId,
                followShardId, handler);
        chunkWorker.start(0, 10, Long.MAX_VALUE);
        assertThat(invoked[0], is(true));
        assertThat(exception[0], nullValue());
        assertThat(chunkWorker.retryCounter.get(), equalTo(testRetryLimit + 1));
    }

    public void testChunkWorkerRetryTooManyTimes() {
        Client client = createClientMock();
        Queue<long[]> chunks = new LinkedList<>();
        mockBulkShardOperationsApiCall(client);
        int testRetryLimit = PROCESSOR_RETRY_LIMIT + 1;
        mockShardCangesApiCallWithRetry(client, testRetryLimit, new ConnectException("connection exception"));

        Executor ccrExecutor = Runnable::run;
        ShardId leaderShardId = new ShardId("index1", "index1", 0);
        ShardId followShardId = new ShardId("index2", "index1", 0);
        IndexMetadataVersionChecker checker = new IndexMetadataVersionChecker(leaderShardId.getIndex(),
                followShardId.getIndex(), client, client);

        boolean[] invoked = new boolean[1];
        Exception[] exception = new Exception[1];
        Consumer<Exception> handler = e -> {invoked[0] = true;exception[0] = e;};
        ChunkWorker chunkWorker = new ChunkWorker(client, client, chunks, ccrExecutor, checker, leaderShardId,
                followShardId, handler);
        chunkWorker.start(0, 10, Long.MAX_VALUE);
        assertThat(invoked[0], is(true));
        assertThat(exception[0], notNullValue());
        assertThat(exception[0].getMessage(), equalTo("retrying failed [17] times, aborting..."));
        assertThat(exception[0].getCause().getMessage(), equalTo("connection exception"));
        assertThat(chunkWorker.retryCounter.get(), equalTo(testRetryLimit));
    }

    public void testChunkWorkerNoneRetryableError() {
        Client client = createClientMock();
        Queue<long[]> chunks = new LinkedList<>();
        mockBulkShardOperationsApiCall(client);
        mockShardCangesApiCallWithRetry(client, 3, new RuntimeException("unexpected"));

        Executor ccrExecutor = Runnable::run;
        ShardId leaderShardId = new ShardId("index1", "index1", 0);
        ShardId followShardId = new ShardId("index2", "index1", 0);
        IndexMetadataVersionChecker checker = new IndexMetadataVersionChecker(leaderShardId.getIndex(),
            followShardId.getIndex(), client, client);

        boolean[] invoked = new boolean[1];
        Exception[] exception = new Exception[1];
        Consumer<Exception> handler = e -> {invoked[0] = true;exception[0] = e;};
        ChunkWorker chunkWorker = new ChunkWorker(client, client, chunks, ccrExecutor, checker, leaderShardId,
                followShardId, handler);
        chunkWorker.start(0, 10, Long.MAX_VALUE);
        assertThat(invoked[0], is(true));
        assertThat(exception[0], notNullValue());
        assertThat(exception[0].getMessage(), equalTo("unexpected"));
        assertThat(chunkWorker.retryCounter.get(), equalTo(0));
    }

    public void testChunkWorkerExceedMaxTranslogsBytes() {
        long from = 0;
        long to = 20;
        long actualTo = 10;
        Client client = createClientMock();
        Queue<long[]> chunks = new LinkedList<>();
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 3;
            @SuppressWarnings("unchecked")
            ActionListener<ShardChangesAction.Response> listener = (ActionListener) args[2];

            List<Translog.Operation> operations = new ArrayList<>();
            for (int i = 0; i <= actualTo; i++) {
                operations.add(new Translog.NoOp(i, 1, "test"));
            }
            listener.onResponse(new ShardChangesAction.Response(1L, operations.toArray(new Translog.Operation[0])));
            return null;
        }).when(client).execute(same(ShardChangesAction.INSTANCE), any(ShardChangesAction.Request.class), any());

        mockBulkShardOperationsApiCall(client);
        Executor ccrExecutor = Runnable::run;
        ShardId leaderShardId = new ShardId("index1", "index1", 0);
        ShardId followShardId = new ShardId("index2", "index1", 0);
        IndexMetadataVersionChecker checker = new IndexMetadataVersionChecker(leaderShardId.getIndex(),
                followShardId.getIndex(), client, client);

        boolean[] invoked = new boolean[1];
        Exception[] exception = new Exception[1];
        Consumer<Exception> handler = e -> {invoked[0] = true;exception[0] = e;};
        BiConsumer<Long, Consumer<Exception>> versionChecker = (indexVersiuon, consumer) -> consumer.accept(null);
        ChunkWorker chunkWorker = new ChunkWorker(client, client, chunks, ccrExecutor, versionChecker, leaderShardId,
            followShardId, handler);
        chunkWorker.start(from, to, Long.MAX_VALUE);
        assertThat(invoked[0], is(true));
        assertThat(exception[0], nullValue());
        assertThat(chunks.size(), equalTo(1));
        assertThat(chunks.peek()[0], equalTo(11L));
        assertThat(chunks.peek()[1], equalTo(20L));
    }

    private Client createClientMock() {
        Client client = mock(Client.class);
        ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        when(client.admin()).thenReturn(adminClient);
        return client;
    }

    private void mockShardCangesApiCallWithRetry(Client client, int testRetryLimit, Exception e) {
        int[] retryCounter = new int[1];
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 3;
            ShardChangesAction.Request request = (ShardChangesAction.Request) args[1];
            @SuppressWarnings("unchecked")
            ActionListener<ShardChangesAction.Response> listener = (ActionListener) args[2];
            if (retryCounter[0]++ <= testRetryLimit) {
                listener.onFailure(e);
            } else {
                long delta = request.getMaxSeqNo() - request.getMinSeqNo();
                Translog.Operation[] operations = new Translog.Operation[(int) delta];
                for (int i = 0; i < operations.length; i++) {
                    operations[i] = new Translog.NoOp(request.getMinSeqNo() + i, 1, "test");
                }
                ShardChangesAction.Response response = new ShardChangesAction.Response(0L, operations);
                listener.onResponse(response);
            }
            return null;
        }).when(client).execute(same(ShardChangesAction.INSTANCE), any(ShardChangesAction.Request.class), any());
    }

    private void mockShardChangesApiCall(Client client) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 3;
            ShardChangesAction.Request request = (ShardChangesAction.Request) args[1];
            @SuppressWarnings("unchecked")
            ActionListener<ShardChangesAction.Response> listener = (ActionListener) args[2];

            List<Translog.Operation> operations = new ArrayList<>();
            for (long i = request.getMinSeqNo(); i <= request.getMaxSeqNo(); i++) {
                operations.add(new Translog.NoOp(request.getMinSeqNo() + i, 1, "test"));
            }
            ShardChangesAction.Response response = new ShardChangesAction.Response(0L, operations.toArray(new Translog.Operation[0]));
            listener.onResponse(response);
            return null;
        }).when(client).execute(same(ShardChangesAction.INSTANCE), any(ShardChangesAction.Request.class), any());
    }

    private void mockBulkShardOperationsApiCall(Client client) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 3;
            @SuppressWarnings("unchecked")
            ActionListener<BulkShardOperationsResponse> listener = (ActionListener) args[2];
            listener.onResponse(new BulkShardOperationsResponse());
            return null;
        }).when(client).execute(same(BulkShardOperationsAction.INSTANCE), any(BulkShardOperationsRequest.class), any());
    }
    
    private ThreadPool createMockThreadPool(boolean fork) {
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executor = mock(ExecutorService.class);
        doAnswer(invocation -> {
            Runnable runnable = (Runnable) invocation.getArguments()[0];
            if (fork) {
                new Thread(runnable).start();
            } else {
                runnable.run();
            }
            return null;
        }).when(executor).execute(any());
        when(threadPool.executor(Ccr.CCR_THREAD_POOL_NAME)).thenReturn(executor);
        return threadPool;
    }

}
