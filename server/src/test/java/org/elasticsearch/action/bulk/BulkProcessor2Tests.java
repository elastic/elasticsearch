/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BulkProcessor2Tests extends ESTestCase {

    private ThreadPool threadPool;
    private final Logger logger = LogManager.getLogger(BulkProcessor2Tests.class);

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool("BulkProcessor2Tests");
    }

    @After
    public void stopThreadPool() throws InterruptedException {
        terminate(threadPool);
    }

    public void testRetry() throws Exception {
        final int maxAttempts = between(1, 3);
        final AtomicInteger attemptRef = new AtomicInteger();

        final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = (request, listener) -> {
            final int attempt = attemptRef.incrementAndGet();
            assertThat(attempt, lessThanOrEqualTo(maxAttempts));
            if (attempt != 1) {
                assertThat(Thread.currentThread().getName(), containsString("[BulkProcessor2Tests][generic]"));
            }

            if (attempt == maxAttempts) {
                listener.onFailure(new ElasticsearchException("final failure"));
            } else {
                listener.onFailure(new RemoteTransportException("remote", new EsRejectedExecutionException("retryable failure")));
            }
        };

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final BulkProcessor2.Listener listener = new BulkProcessor2.Listener() {

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                fail("afterBulk should not return success");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Exception failure) {
                assertThat(failure, instanceOf(ElasticsearchException.class));
                assertThat(failure.getMessage(), equalTo("final failure"));
                countDownLatch.countDown();
            }
        };

        BulkProcessor2 bulkProcessor = BulkProcessor2.builder(consumer, listener, threadPool)
            .setMaxNumberOfRetries(maxAttempts)
            .setFlushInterval(TimeValue.timeValueMillis(1))
            .build();
        try {
            bulkProcessor.add(new IndexRequest());
            assertTrue(countDownLatch.await(5, TimeUnit.MINUTES));
            assertThat(bulkProcessor.getTotalBytesInFlight(), equalTo(0L));
        } finally {
            bulkProcessor.awaitClose(1, TimeUnit.SECONDS);
        }

        assertThat(attemptRef.get(), equalTo(maxAttempts));
    }

    // @TestLogging(
    // value = "org.elasticsearch.action.bulk:trace",
    // reason = "Logging information about locks useful for tracking down deadlock"
    // )
    public void testConcurrentExecutions() throws Exception {
        final AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        int estimatedTimeForTest = Integer.MAX_VALUE;
        final int simulateWorkTimeInMillis = 5;
        int concurrentClients = 0;
        int concurrentBulkRequests = 0;
        AtomicInteger expectedExecutions = new AtomicInteger(0);
        int maxBatchSize = 0;
        int maxDocuments = 0;
        int iterations = 0;
        boolean runTest = true;
        // find some randoms that allow this test to take under ~ 10 seconds
        while (estimatedTimeForTest > 10_000) {
            if (iterations++ > 1_000) { // extremely unlikely
                runTest = false;
                break;
            }
            maxBatchSize = randomIntBetween(1, 100);
            maxDocuments = randomIntBetween(maxBatchSize, 100_000);
            concurrentClients = randomIntBetween(1, 20);
            concurrentBulkRequests = randomIntBetween(0, 20);
            expectedExecutions.set(maxDocuments / maxBatchSize);
            estimatedTimeForTest = (expectedExecutions.get() * simulateWorkTimeInMillis) / Math.min(
                concurrentBulkRequests + 1,
                concurrentClients
            );
        }
        assumeTrue("failed to find random values that allows test to run quickly", runTest);
        BulkResponse bulkResponse = new BulkResponse(
            new BulkItemResponse[] { BulkItemResponse.success(0, randomFrom(DocWriteRequest.OpType.values()), mockResponse()) },
            0
        );
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger requestCount = new AtomicInteger(0);
        AtomicInteger docCount = new AtomicInteger(0);
        ScheduledExecutorService consumerExecutor = Executors.newScheduledThreadPool(2 * concurrentBulkRequests);
        // All consumers are expected to be async:
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = (request, listener) -> {
            consumerExecutor.schedule(() -> {
                try {
                    Thread.sleep(simulateWorkTimeInMillis); // simulate work
                    listener.onResponse(bulkResponse);
                } catch (InterruptedException e) {
                    // should never happen
                    Thread.currentThread().interrupt();
                    failureCount.getAndIncrement();
                    exceptionRef.set(ExceptionsHelper.useOrSuppress(exceptionRef.get(), e));
                }
            }, 0, TimeUnit.SECONDS);
        };
        BulkProcessor2 bulkProcessor = new BulkProcessor2(
            consumer,
            0,
            countingListener(requestCount, successCount, failureCount, docCount, exceptionRef),
            maxBatchSize,
            ByteSizeValue.ofBytes(Integer.MAX_VALUE),
            new ByteSizeValue(50, ByteSizeUnit.MB),
            null,
            threadPool
        );
        try {
            IndexRequest indexRequest = new IndexRequest();
            for (final AtomicInteger i = new AtomicInteger(0); i.getAndIncrement() < maxDocuments;) {
                bulkProcessor.add(indexRequest);
            }

            assertBusy(() -> {
                String message = "Expected Bulks: %s\nRequested Bulks: %s\nSuccessful Bulks: %s\nFailed Bulks: %ds";
                String formattedMessage = String.format(
                    Locale.ROOT,
                    message,
                    expectedExecutions.get(),
                    requestCount.get(),
                    successCount.get(),
                    failureCount.get()
                );
                assertTrue(
                    formattedMessage,
                    failureCount.get() == 0 && successCount.get() == expectedExecutions.get() && requestCount.get() == successCount.get()
                );
            }, 30, TimeUnit.SECONDS);
            if (failureCount.get() > 0 || successCount.get() != expectedExecutions.get() || requestCount.get() != successCount.get()) {
                if (exceptionRef.get() != null) {
                    logger.error("exception(s) caught during test", exceptionRef.get());
                }
                String message = "Expected Bulks: %s\nRequested Bulks: %s\nSuccessful Bulks: %s\nExpected Bulks: %s\nFailed Bulks: "
                    + "%ds\nMax Documents: %s\nMax Batch Size: %s\nConcurrent Clients: %s\nConcurrent Bulk Requests: %s";
                fail(
                    String.format(
                        Locale.ROOT,
                        message,
                        expectedExecutions,
                        requestCount.get(),
                        successCount.get(),
                        expectedExecutions.get(),
                        failureCount.get(),
                        maxDocuments,
                        maxBatchSize,
                        concurrentClients,
                        concurrentBulkRequests
                    )
                );
            }
        } finally {
            bulkProcessor.awaitClose(1, TimeUnit.SECONDS);
        }

        assertThat(bulkProcessor.getTotalBytesInFlight(), equalTo(0L));
        // count total docs after processor is closed since there may have been partial batches that are flushed on close.
        assertEquals(docCount.get(), maxDocuments);
        consumerExecutor.shutdown();
    }

    public void testConcurrentExecutionsWithFlush() throws Exception {
        final AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        final int maxDocuments = 100_000;
        final int concurrentClients = 2;
        final int maxBatchSize = Integer.MAX_VALUE; // don't flush based on size
        final int concurrentBulkRequests = randomIntBetween(0, 20);
        final int simulateWorkTimeInMillis = 5;
        BulkResponse bulkResponse = new BulkResponse(
            new BulkItemResponse[] { BulkItemResponse.success(0, randomFrom(DocWriteRequest.OpType.values()), mockResponse()) },
            0
        );
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger requestCount = new AtomicInteger(0);
        AtomicInteger docCount = new AtomicInteger(0);
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = (request, listener) -> {
            try {
                Thread.sleep(simulateWorkTimeInMillis); // simulate work
                listener.onResponse(bulkResponse);
            } catch (InterruptedException e) {
                // should never happen
                Thread.currentThread().interrupt();
                failureCount.getAndIncrement();
                exceptionRef.set(ExceptionsHelper.useOrSuppress(exceptionRef.get(), e));
            }
        };
        BulkProcessor2 bulkProcessor = new BulkProcessor2(
            consumer,
            0,
            countingListener(requestCount, successCount, failureCount, docCount, exceptionRef),
            maxBatchSize,
            ByteSizeValue.ofBytes(Integer.MAX_VALUE),
            new ByteSizeValue(50, ByteSizeUnit.MB),
            TimeValue.timeValueMillis(simulateWorkTimeInMillis * 2),
            threadPool
        );
        try {

            ExecutorService executorService = Executors.newFixedThreadPool(concurrentClients);
            IndexRequest indexRequest = new IndexRequest();
            List<Future<?>> futures = new ArrayList<>();
            CountDownLatch startGate = new CountDownLatch(1 + concurrentClients);
            for (final AtomicInteger i = new AtomicInteger(0); i.getAndIncrement() < maxDocuments;) {
                futures.add(executorService.submit(() -> {
                    try {
                        // don't start any work until all tasks are submitted
                        startGate.countDown();
                        startGate.await();
                        // alternate between ways to add to the bulk processor
                        bulkProcessor.add(indexRequest);
                    } catch (Exception e) {
                        throw ExceptionsHelper.convertToRuntime(e);
                    }
                }));
            }
            startGate.countDown();
            startGate.await();

            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    exceptionRef.set(ExceptionsHelper.useOrSuppress(exceptionRef.get(), e));
                }
            }
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } finally {
            bulkProcessor.awaitClose(1, TimeUnit.SECONDS);
        }
        assertThat(bulkProcessor.getTotalBytesInFlight(), equalTo(0L));

        if (failureCount.get() > 0 || requestCount.get() != successCount.get() || maxDocuments != docCount.get()) {
            if (exceptionRef.get() != null) {
                logger.error("exception(s) caught during test", exceptionRef.get());
            }
            String message = "Requested Bulks: %d\nSuccessful Bulks: %d\nFailed Bulks: %d\nTotal Documents: %d\nMax Documents: %d\nMax "
                + "Batch Size: %d\nConcurrent Clients: %d\nConcurrent Bulk Requests: %d";
            fail(
                String.format(
                    Locale.ROOT,
                    message,
                    requestCount.get(),
                    successCount.get(),
                    failureCount.get(),
                    docCount.get(),
                    maxDocuments,
                    maxBatchSize,
                    concurrentClients,
                    concurrentBulkRequests
                )
            );
        }
    }

    public void testRejections() throws Exception {
        /*
        * This test loads data into a BulkProcessor2 with a "max bytes in flight" value, and expects to see an
        * EsERejectedExecutionException.
        */
        final int simulateWorkTimeInMillis = 5;
        int maxBatchSize = randomIntBetween(2, 100);
        int maxDocuments = randomIntBetween(maxBatchSize, 1_000_000);
        int concurrentBulkRequests = randomIntBetween(0, 20);
        BulkResponse bulkResponse = new BulkResponse(
            new BulkItemResponse[] { BulkItemResponse.success(0, randomFrom(DocWriteRequest.OpType.values()), mockResponse()) },
            0
        );
        ScheduledExecutorService consumerExecutor = Executors.newScheduledThreadPool(2 * concurrentBulkRequests);
        // All consumers are expected to be async:
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = (request, listener) -> {
            consumerExecutor.schedule(() -> {
                try {
                    Thread.sleep(simulateWorkTimeInMillis); // simulate work
                    listener.onResponse(bulkResponse);
                } catch (InterruptedException e) {
                    // should never happen
                    fail("Test interrupted");
                }
            }, 0, TimeUnit.SECONDS);
        };
        BulkProcessor2 bulkProcessor = new BulkProcessor2(
            consumer,
            0,
            emptyListener(),
            maxBatchSize,
            ByteSizeValue.ofBytes(Integer.MAX_VALUE),
            ByteSizeValue.ofBytes(500),
            null,
            threadPool
        );
        try {
            IndexRequest indexRequest = new IndexRequest();
            boolean rejectedRequests = false;
            for (int i = 0; i < maxDocuments; i++) {
                try {
                    bulkProcessor.add(indexRequest);
                } catch (EsRejectedExecutionException e) {
                    rejectedRequests = true;
                    break;
                }
            }
            assertThat(rejectedRequests, equalTo(true));
        } finally {
            bulkProcessor.awaitClose(1, TimeUnit.SECONDS);
        }
        assertThat(bulkProcessor.getTotalBytesInFlight(), equalTo(0L));
        consumerExecutor.shutdown();
    }

    private BulkProcessor2.Listener emptyListener() {
        return new BulkProcessor2.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, Exception failure) {}
        };
    }

    private BulkProcessor2.Listener countingListener(
        AtomicInteger requestCount,
        AtomicInteger successCount,
        AtomicInteger failureCount,
        AtomicInteger docCount,
        AtomicReference<Throwable> exceptionRef
    ) {

        return new BulkProcessor2.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                requestCount.incrementAndGet();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                successCount.incrementAndGet();
                docCount.addAndGet(request.requests().size());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Exception failure) {
                if (failure != null) {
                    failureCount.incrementAndGet();
                    exceptionRef.set(ExceptionsHelper.useOrSuppress(exceptionRef.get(), failure));

                }
            }
        };
    }

    private DocWriteResponse mockResponse() {
        return new IndexResponse(new ShardId("index", "uid", 0), "_doc", "id", 1, 1, 1, true);
    }
}
