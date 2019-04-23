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

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class BulkProcessorTests extends ESTestCase {

    private ThreadPool threadPool;
    private final Logger logger = LogManager.getLogger(BulkProcessorTests.class);

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool("BulkProcessorTests");
    }

    @After
    public void stopThreadPool() throws InterruptedException {
        terminate(threadPool);
    }

    public void testBulkProcessorFlushPreservesContext() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final String headerKey = randomAlphaOfLengthBetween(1, 8);
        final String transientKey = randomAlphaOfLengthBetween(1, 8);
        final String headerValue = randomAlphaOfLengthBetween(1, 32);
        final Object transientValue = new Object();

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = (request, listener) -> {
            ThreadContext threadContext = threadPool.getThreadContext();
            assertEquals(headerValue, threadContext.getHeader(headerKey));
            assertSame(transientValue, threadContext.getTransient(transientKey));
            latch.countDown();
        };

        final int bulkSize = randomIntBetween(2, 32);
        final TimeValue flushInterval = TimeValue.timeValueSeconds(1L);
        final BulkProcessor bulkProcessor;
        assertNull(threadPool.getThreadContext().getHeader(headerKey));
        assertNull(threadPool.getThreadContext().getTransient(transientKey));
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader(headerKey, headerValue);
            threadPool.getThreadContext().putTransient(transientKey, transientValue);
            bulkProcessor = new BulkProcessor(consumer, BackoffPolicy.noBackoff(), emptyListener(),
                1, bulkSize, new ByteSizeValue(5, ByteSizeUnit.MB), flushInterval,
                threadPool, () -> {
            }, BulkRequest::new);
        }
        assertNull(threadPool.getThreadContext().getHeader(headerKey));
        assertNull(threadPool.getThreadContext().getTransient(transientKey));

        // add a single item which won't be over the size or number of items
        bulkProcessor.add(new IndexRequest());

        // wait for flush to execute
        latch.await();

        assertNull(threadPool.getThreadContext().getHeader(headerKey));
        assertNull(threadPool.getThreadContext().getTransient(transientKey));
        bulkProcessor.close();
    }

    public void testConcurrentExecutions() throws Exception {
        final AtomicBoolean called = new AtomicBoolean(false);
        final int maxBatchSize = randomIntBetween(1, 1000);
        final int maxDocuments = randomIntBetween(maxBatchSize, 1000000);
        final int concurrentClients = randomIntBetween(1, 20);
        final int concurrentBulkRequests = randomIntBetween(0, 20);
        final int expectedExecutions = maxDocuments / maxBatchSize;
        BulkResponse bulkResponse = new BulkResponse(new BulkItemResponse[]{new BulkItemResponse()}, 0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger requestCount = new AtomicInteger(0);
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = (request, listener) -> listener.onResponse(bulkResponse);
        BulkProcessor bulkProcessor = new BulkProcessor(consumer, BackoffPolicy.noBackoff(),
            countingListener(requestCount, successCount, failureCount),
            concurrentBulkRequests, maxBatchSize, new ByteSizeValue(Integer.MAX_VALUE), null,
            (command, delay, executor) -> null, () -> called.set(true), BulkRequest::new);

        ExecutorService executorService = Executors.newFixedThreadPool(concurrentClients);

        IndexRequest indexRequest = new IndexRequest();
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < maxDocuments; i++) {
            futures.add(executorService.submit(() -> bulkProcessor.add(indexRequest)));
        }

        for (Future f : futures) {
            try {
                f.get(1, TimeUnit.SECONDS);
            }catch (Exception e){
                failureCount.incrementAndGet();
                logger.error("failure while getting future", e);
            }
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        if (failureCount.get() > 0 || successCount.get() != expectedExecutions || requestCount.get() != successCount.get()) {
            fail("\nExpected Bulks: " + expectedExecutions + "\n" +
                "Requested Bulks: " + requestCount.get() + "\n" +
                "Successful Bulks: " + successCount.get() + "\n" +
                "Failed Bulks: " + failureCount.get() + "\n" +
                "Max Documents: " + maxDocuments + "\n" +
                "Max Batch Size: " + maxBatchSize + "\n" +
                "Concurrent Clients " + concurrentClients + "\n" +
                "Concurrent Bulk Requests " + concurrentBulkRequests + "\n"
            );
        }
        bulkProcessor.close();
    }

    public void testAwaitOnCloseCallsOnClose() throws Exception {
        final AtomicBoolean called = new AtomicBoolean(false);
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = (request, listener) -> {
        };
        BulkProcessor bulkProcessor = new BulkProcessor(consumer, BackoffPolicy.noBackoff(), emptyListener(),
            0, 10, new ByteSizeValue(1000), null,
            (command, delay, executor) -> null, () -> called.set(true), BulkRequest::new);

        assertFalse(called.get());
        bulkProcessor.awaitClose(100, TimeUnit.MILLISECONDS);
        assertTrue(called.get());
    }

    private BulkProcessor.Listener emptyListener() {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            }
        };
    }

    private BulkProcessor.Listener countingListener(AtomicInteger requestCount, AtomicInteger successCount, AtomicInteger failureCount) {

        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                requestCount.incrementAndGet();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                successCount.incrementAndGet();
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                if (failure != null) {
                    failureCount.incrementAndGet();
                }
            }
        };
    }
}
