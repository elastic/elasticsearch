/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class IncrementalBulkIT extends ESIntegTestCase {

    public void testSingleBulkRequest() {
        String index = "test";
        createIndex(index);

        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class);

        IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
        IndexRequest indexRequest = indexRequest(index);

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        handler.lastItems(List.of(indexRequest), refCounted::decRef, future);

        BulkResponse bulkResponse = future.actionGet();
        assertNoFailures(bulkResponse);

        refresh(index);

        assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) 1));
        });

        assertFalse(refCounted.hasReferences());
    }

    public void testMultipleBulkPartsWithBackoff() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        try (Releasable ignored = executorService::shutdown;) {
            String index = "test";
            createIndex(index);

            IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class);
            long docs = randomIntBetween(200, 400);

            IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();

            BulkResponse bulkResponse = executeBulk(docs, index, handler, executorService);
            assertNoFailures(bulkResponse);

            refresh(index);

            assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
                assertNoFailures(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(docs));
            });
        }
    }

    public void testGlobalBulkFailure() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        CountDownLatch blockingLatch = new CountDownLatch(1);

        try (Releasable ignored = executorService::shutdown; Releasable ignored2 = blockingLatch::countDown) {
            String index = "test";
            createIndex(index);

            String randomNodeName = internalCluster().getRandomNodeName();
            IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, randomNodeName);
            ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, randomNodeName);

            int threadCount = threadPool.info(ThreadPool.Names.WRITE).getMax();
            long queueSize = threadPool.info(ThreadPool.Names.WRITE).getQueueSize().singles();
            blockWritePool(threadCount, threadPool, blockingLatch);

            Runnable runnable = () -> {};
            for (int i = 0; i < queueSize; i++) {
                threadPool.executor(ThreadPool.Names.WRITE).execute(runnable);
            }

            IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
            expectThrows(
                EsRejectedExecutionException.class,
                () -> executeBulk(randomIntBetween(200, 400), index, handler, executorService)
            );
        }
    }

    public void testTopLevelBulkFailureAfterFirstIncrementalRequest() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        try (Releasable ignored = executorService::shutdown) {
            String index = "test";
            createIndex(index);

            String randomNodeName = internalCluster().getRandomNodeName();
            IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, randomNodeName);
            ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, randomNodeName);
            IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();
            AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
            PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();

            int threadCount = threadPool.info(ThreadPool.Names.WRITE).getMax();
            long queueSize = threadPool.info(ThreadPool.Names.WRITE).getQueueSize().singles();

            CountDownLatch blockingLatch1 = new CountDownLatch(1);

            AtomicBoolean nextRequested = new AtomicBoolean(true);
            AtomicLong hits = new AtomicLong(0);
            try (Releasable ignored2 = blockingLatch1::countDown;) {
                blockWritePool(threadCount, threadPool, blockingLatch1);
                while (nextRequested.get()) {
                    nextRequested.set(false);
                    refCounted.incRef();
                    handler.addItems(List.of(indexRequest(index)), refCounted::decRef, () -> nextRequested.set(true));
                    hits.incrementAndGet();
                }
            }
            assertBusy(() -> assertTrue(nextRequested.get()));

            CountDownLatch blockingLatch2 = new CountDownLatch(1);

            try (Releasable ignored3 = blockingLatch2::countDown;) {
                blockWritePool(threadCount, threadPool, blockingLatch2);
                Runnable runnable = () -> {};
                // Fill Queue
                for (int i = 0; i < queueSize; i++) {
                    threadPool.executor(ThreadPool.Names.WRITE).execute(runnable);
                }

                handler.lastItems(List.of(indexRequest(index)), refCounted::decRef, future);
            }

            // Should not throw because some succeeded
            BulkResponse bulkResponse = future.actionGet();

            assertTrue(bulkResponse.hasFailures());
            BulkItemResponse[] items = bulkResponse.getItems();
            assertThat(Arrays.stream(items).filter(r -> r.isFailed() == false).count(), equalTo(hits.get()));
            assertThat(items[items.length - 1].getFailure().getCause(), instanceOf(EsRejectedExecutionException.class));

            refresh(index);

            assertResponse(prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
                assertNoFailures(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(hits.get()));
            });
        }
    }

    private static void blockWritePool(int threadCount, ThreadPool threadPool, CountDownLatch blockingLatch) throws InterruptedException {
        CountDownLatch startedLatch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(() -> {
                startedLatch.countDown();
                try {
                    blockingLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        startedLatch.await();
    }

    private BulkResponse executeBulk(long docs, String index, IncrementalBulkService.Handler handler, ExecutorService executorService) {
        ConcurrentLinkedQueue<DocWriteRequest<?>> queue = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < docs; i++) {
            IndexRequest indexRequest = indexRequest(index);
            queue.add(indexRequest);
        }

        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        Runnable r = new Runnable() {

            @Override
            public void run() {
                int toRemove = Math.min(randomIntBetween(5, 10), queue.size());
                ArrayList<DocWriteRequest<?>> docs = new ArrayList<>();
                for (int i = 0; i < toRemove; i++) {
                    docs.add(queue.poll());
                }

                if (queue.isEmpty()) {
                    handler.lastItems(docs, refCounted::decRef, future);
                } else {
                    refCounted.incRef();
                    handler.addItems(docs, refCounted::decRef, () -> executorService.execute(this));
                }
            }
        };

        executorService.execute(r);

        BulkResponse bulkResponse = future.actionGet();
        assertFalse(refCounted.hasReferences());
        return bulkResponse;
    }

    private static IndexRequest indexRequest(String index) {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index);
        indexRequest.source(Map.of("field", randomAlphaOfLength(10)));
        return indexRequest;
    }
}
