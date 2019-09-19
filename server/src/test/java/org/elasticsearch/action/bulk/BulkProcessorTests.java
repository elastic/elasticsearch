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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class BulkProcessorTests extends ESTestCase {

    private ThreadPool threadPool;
    private ExecutorService asyncExec = Executors.newFixedThreadPool(1);
    private ExecutorService userExec = Executors.newFixedThreadPool(1);
    private ScheduledThreadPoolExecutor scheduleThreadPoolExecutor = Scheduler.initScheduler(Settings.EMPTY);

    @Rule
    public ExpectedException exception = ExpectedException.none();
    
    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool("BulkProcessorTests");
    }

    @After
    public void stopThreadPool() throws InterruptedException {
        terminate(threadPool);
        asyncExec.shutdown();
        userExec.shutdownNow();
        scheduleThreadPoolExecutor.shutdownNow();
    }

    public void testBulkProcessorThreadBlocked() throws Exception {
        exception.expect(TimeoutException.class);
        Future<?> future = buildAndExecuteBulkProcessor(initScheduler(1));
        future.get(8, TimeUnit.SECONDS);// thread has been blocked, the IndexRequest cannot be successfully executed.
    }

    public void testBulkProcessorThread() throws Exception {
        Future<?> future = buildAndExecuteBulkProcessor(initScheduler(2));
         assertNull(future.get(4, TimeUnit.SECONDS));//the IndexRequest executed successfully.
    }

    private Scheduler initScheduler(int corePoolSize) {
        scheduleThreadPoolExecutor.setCorePoolSize(corePoolSize);
        return (command, delay, executor) ->
            Scheduler.wrapAsScheduledCancellable(scheduleThreadPoolExecutor.schedule(command, delay.millis(), TimeUnit.MILLISECONDS));
    }

    private Future<?> buildAndExecuteBulkProcessor(Scheduler scheduler) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        final int concurrentRequests = 0;
        final int bulkActions = 3;
        final TimeValue flushInterval = TimeValue.timeValueMillis(1000L);
        final BackoffPolicy backoff = BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(100L), 1);
        final ByteSizeValue bulkSize = new ByteSizeValue(5, ByteSizeUnit.MB);
        BulkProcessor bulkProcessor = new BulkProcessor(bulkAsync(latch), backoff, emptyListener(),
                concurrentRequests, bulkActions, bulkSize, flushInterval,
                scheduler, null, BulkRequest::new);
        Future<?> future = userExec.submit(() -> {
                bulkProcessor.add(new IndexRequest());
                bulkProcessor.add(new IndexRequest());
                bulkProcessor.add(new IndexRequest());// Step-1: execute `BulkRequestHandler` and locked 'BulkProcessor'
            });
        Thread.sleep(2000L);// Step-2: wait and ensure IntervalFlush is called
        latch.countDown();  // Due to step-1, the scheduling thread state is BLOCKED (on object monitor)
        return future;
    }

    private BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkAsync(CountDownLatch latch) {
        return (request, listener) ->
        {
            // Step-3: retry of bulk request by using scheduler thread
            // Due to step-2, scheduler thread is already occupied, causing the retry policy to not be executed 
            // Causes the lock and latch in step-1 not to be released
            asyncExec.execute(() -> {
                try {
                    latch.await();
                    listener.onResponse(createBulkResponse());
                } catch (InterruptedException e) {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            });
        };
    }

    private BulkResponse createBulkResponse() {
        EsRejectedExecutionException exception =new EsRejectedExecutionException();
        Failure failure = new Failure("", "", "", exception, ExceptionsHelper.status(exception)); 
        BulkItemResponse[] bulkActionResponses = new BulkItemResponse[] {
                new BulkItemResponse(),
                new BulkItemResponse(),
                new BulkItemResponse(3, OpType.INDEX, failure)
        };
        BulkResponse bulkResponse = new BulkResponse(bulkActionResponses, 3000L);
        return bulkResponse;
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
                threadPool, () -> {}, BulkRequest::new);
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


    public void testAwaitOnCloseCallsOnClose() throws Exception {
        final AtomicBoolean called = new AtomicBoolean(false);
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = (request, listener) -> {};
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
}
