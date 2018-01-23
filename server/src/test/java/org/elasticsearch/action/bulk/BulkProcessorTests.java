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

import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;

public class BulkProcessorTests extends ESTestCase {

    private ThreadPool threadPool;

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
            bulkProcessor = new BulkProcessor(consumer, BackoffPolicy.noBackoff(), new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                }
            }, 1, bulkSize, new ByteSizeValue(5, ByteSizeUnit.MB), flushInterval, threadPool, () -> {});
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
}
