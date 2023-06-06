/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class AsyncOperatorTests extends ESTestCase {

    private TestThreadPool threadPool;

    @Before
    public void setThreadPool() {
        int numThreads = randomBoolean() ? 1 : between(2, 16);
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, "esql_test_executor", numThreads, 1024, "esql", false)
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testBasic() {
        int positions = randomIntBetween(0, 10_000);
        List<Long> ids = new ArrayList<>(positions);
        Map<Long, String> dict = new HashMap<>();
        for (int i = 0; i < positions; i++) {
            long id = randomLong();
            ids.add(id);
            if (randomBoolean()) {
                dict.computeIfAbsent(id, k -> randomAlphaOfLength(5));
            }
        }
        SourceOperator sourceOperator = new AbstractBlockSourceOperator(randomIntBetween(10, 1000)) {
            @Override
            protected int remaining() {
                return ids.size() - currentPosition;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                LongVector.Builder builder = LongVector.newVectorBuilder(length);
                for (int i = 0; i < length; i++) {
                    builder.appendLong(ids.get(currentPosition++));
                }
                return new Page(builder.build().asBlock());
            }
        };
        int maxConcurrentRequests = randomIntBetween(1, 10);
        AsyncOperator asyncOperator = new AsyncOperator(maxConcurrentRequests) {
            final LookupService lookupService = new LookupService(threadPool, dict, maxConcurrentRequests);

            @Override
            protected void performAsync(Page inputPage, ActionListener<Page> listener) {
                lookupService.lookupAsync(inputPage, listener);
            }

            @Override
            public void close() {

            }
        };
        Iterator<Long> it = ids.iterator();
        SinkOperator outputOperator = new PageConsumerOperator(page -> {
            assertThat(page.getBlockCount(), equalTo(2));
            LongBlock b1 = page.getBlock(0);
            BytesRefBlock b2 = page.getBlock(1);
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertTrue(it.hasNext());
                long key = b1.getLong(i);
                assertThat(key, equalTo(it.next()));
                String v = dict.get(key);
                if (v == null) {
                    assertTrue(b2.isNull(i));
                } else {
                    assertThat(b2.getBytesRef(i, scratch), equalTo(new BytesRef(v)));
                }
            }
        });
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        Driver driver = new Driver(
            new DriverContext(),
            sourceOperator,
            List.of(asyncOperator),
            outputOperator,
            () -> assertFalse(it.hasNext())
        );
        Driver.start(threadPool.executor("esql_test_executor"), driver, future);
        future.actionGet();
    }

    public void testStatus() {
        Map<Page, ActionListener<Page>> handlers = new HashMap<>();
        AsyncOperator operator = new AsyncOperator(2) {
            @Override
            protected void performAsync(Page inputPage, ActionListener<Page> listener) {
                handlers.put(inputPage, listener);
            }

            @Override
            public void close() {

            }
        };
        assertTrue(operator.isBlocked().isDone());
        assertTrue(operator.needsInput());

        Page page1 = new Page(Block.constantNullBlock(1));
        operator.addInput(page1);
        assertFalse(operator.isBlocked().isDone());
        ListenableActionFuture<Void> blocked1 = operator.isBlocked();
        assertTrue(operator.needsInput());

        Page page2 = new Page(Block.constantNullBlock(2));
        operator.addInput(page2);
        assertFalse(operator.needsInput()); // reached the max outstanding requests
        assertFalse(operator.isBlocked().isDone());
        assertThat(operator.isBlocked(), equalTo(blocked1));

        Page page3 = new Page(Block.constantNullBlock(3));
        handlers.remove(page1).onResponse(page3);
        assertFalse(operator.needsInput()); // still have 2 outstanding requests
        assertTrue(operator.isBlocked().isDone());
        assertTrue(blocked1.isDone());

        assertThat(operator.getOutput(), equalTo(page3));
        assertTrue(operator.needsInput());
        assertFalse(operator.isBlocked().isDone());

        operator.close();
    }

    static class LookupService {
        private final ThreadPool threadPool;
        private final Map<Long, String> dict;
        private final int maxConcurrentRequests;
        private final AtomicInteger pendingRequests = new AtomicInteger();

        LookupService(ThreadPool threadPool, Map<Long, String> dict, int maxConcurrentRequests) {
            this.threadPool = threadPool;
            this.dict = dict;
            this.maxConcurrentRequests = maxConcurrentRequests;
        }

        public void lookupAsync(Page input, ActionListener<Page> listener) {
            int total = pendingRequests.incrementAndGet();
            assert total <= maxConcurrentRequests : "too many pending requests: total=" + total + ",limit=" + maxConcurrentRequests;
            ActionRunnable<Page> command = new ActionRunnable<>(listener) {
                @Override
                protected void doRun() {
                    LongBlock ids = input.getBlock(0);
                    BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(ids.getPositionCount());
                    for (int i = 0; i < ids.getPositionCount(); i++) {
                        String v = dict.get(ids.getLong(i));
                        if (v != null) {
                            builder.appendBytesRef(new BytesRef(v));
                        } else {
                            builder.appendNull();
                        }
                    }
                    int current = pendingRequests.decrementAndGet();
                    assert current >= 0 : "pending requests must be non-negative";
                    Page result = input.appendBlock(builder.build());
                    listener.onResponse(result);
                }
            };
            TimeValue delay = TimeValue.timeValueMillis(randomIntBetween(0, 50));
            threadPool.schedule(command, delay, "esql_test_executor");
        }
    }
}
