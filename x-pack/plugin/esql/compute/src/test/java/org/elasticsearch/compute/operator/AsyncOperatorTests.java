/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.MockBlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AsyncOperatorTests extends ESTestCase {

    private TestThreadPool threadPool;

    private static final String ESQL_TEST_EXECUTOR = "esql_test_executor";

    @Before
    public void setThreadPool() {
        int numThreads = randomBoolean() ? 1 : between(2, 16);
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, ESQL_TEST_EXECUTOR, numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testBasic() {
        BlockFactory globalBlockFactory = blockFactory();
        LocalCircuitBreaker localBreaker = null;
        final DriverContext driverContext;
        if (randomBoolean()) {
            localBreaker = new LocalCircuitBreaker(globalBlockFactory.breaker(), between(0, 1024), between(0, 4096));
            BlockFactory localFactory = globalBlockFactory.newChildFactory(localBreaker);
            driverContext = new DriverContext(globalBlockFactory.bigArrays(), localFactory);
        } else {
            driverContext = new DriverContext(globalBlockFactory.bigArrays(), globalBlockFactory);
        }
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
        SourceOperator sourceOperator = new AbstractBlockSourceOperator(driverContext.blockFactory(), randomIntBetween(10, 1000)) {
            @Override
            protected int remaining() {
                return ids.size() - currentPosition;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                try (LongVector.Builder builder = blockFactory.newLongVectorBuilder(length)) {
                    for (int i = 0; i < length; i++) {
                        builder.appendLong(ids.get(currentPosition++));
                    }
                    return new Page(builder.build().asBlock());
                }
            }
        };
        int maxConcurrentRequests = randomIntBetween(1, 10);
        AsyncOperator asyncOperator = new AsyncOperator(driverContext, maxConcurrentRequests) {
            final LookupService lookupService = new LookupService(threadPool, globalBlockFactory, dict, maxConcurrentRequests);

            @Override
            protected void performAsync(Page inputPage, ActionListener<Page> listener) {
                lookupService.lookupAsync(inputPage, listener);
            }

            @Override
            public void doClose() {

            }
        };
        List<Operator> intermediateOperators = new ArrayList<>();
        intermediateOperators.add(asyncOperator);
        final Iterator<Long> it;
        if (randomBoolean()) {
            int limit = between(0, ids.size());
            it = ids.subList(0, limit).iterator();
            intermediateOperators.add(new LimitOperator(limit));
        } else {
            it = ids.iterator();
        }
        SinkOperator outputOperator = new PageConsumerOperator(page -> {
            try (Releasable ignored = page::releaseBlocks) {
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
            }
        });
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        Driver driver = new Driver(driverContext, sourceOperator, intermediateOperators, outputOperator, () -> assertFalse(it.hasNext()));
        Driver.start(threadPool.getThreadContext(), threadPool.executor(ESQL_TEST_EXECUTOR), driver, between(1, 10000), future);
        future.actionGet();
        Releasables.close(localBreaker);
    }

    class TestOp extends AsyncOperator {
        Map<Page, ActionListener<Page>> handlers = new HashMap<>();

        TestOp(DriverContext driverContext, int maxOutstandingRequests) {
            super(driverContext, maxOutstandingRequests);
        }

        @Override
        protected void performAsync(Page inputPage, ActionListener<Page> listener) {
            handlers.put(inputPage, listener);
        }

        @Override
        protected void doClose() {

        }
    }

    public void testStatus() {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory);
        TestOp operator = new TestOp(driverContext, 2);
        assertTrue(operator.isBlocked().listener().isDone());
        assertTrue(operator.needsInput());

        Page page1 = new Page(driverContext.blockFactory().newConstantNullBlock(1));
        operator.addInput(page1);
        assertFalse(operator.isBlocked().listener().isDone());
        SubscribableListener<Void> blocked1 = operator.isBlocked().listener();
        assertTrue(operator.needsInput());

        Page page2 = new Page(driverContext.blockFactory().newConstantNullBlock(2));
        operator.addInput(page2);
        assertFalse(operator.needsInput()); // reached the max outstanding requests
        assertFalse(operator.isBlocked().listener().isDone());
        assertThat(operator.isBlocked(), equalTo(new IsBlockedResult(blocked1, "TestOp")));

        Page page3 = new Page(driverContext.blockFactory().newConstantNullBlock(3));
        operator.handlers.remove(page1).onResponse(page3);
        page1.releaseBlocks();
        assertFalse(operator.needsInput()); // still have 2 outstanding requests
        assertTrue(operator.isBlocked().listener().isDone());
        assertTrue(blocked1.isDone());
        assertThat(operator.getOutput(), equalTo(page3));
        page3.releaseBlocks();

        assertTrue(operator.needsInput());
        assertFalse(operator.isBlocked().listener().isDone());
        Page page4 = new Page(driverContext.blockFactory().newConstantNullBlock(3));
        operator.handlers.remove(page2).onResponse(page4);
        page2.releaseBlocks();
        assertThat(operator.getOutput(), equalTo(page4));
        page4.releaseBlocks();

        operator.close();
    }

    public void testFailure() throws Exception {
        BlockFactory globalBlockFactory = blockFactory();
        LocalCircuitBreaker localBreaker = null;
        final DriverContext driverContext;
        if (randomBoolean()) {
            localBreaker = new LocalCircuitBreaker(globalBlockFactory.breaker(), between(0, 1024), between(0, 4096));
            BlockFactory localFactory = globalBlockFactory.newChildFactory(localBreaker);
            driverContext = new DriverContext(globalBlockFactory.bigArrays(), localFactory);
        } else {
            driverContext = new DriverContext(globalBlockFactory.bigArrays(), globalBlockFactory);
        }
        final SequenceLongBlockSourceOperator sourceOperator = new SequenceLongBlockSourceOperator(
            driverContext.blockFactory(),
            LongStream.range(0, 100 * 1024)
        );
        int maxConcurrentRequests = randomIntBetween(1, 10);
        AtomicBoolean failed = new AtomicBoolean();
        AsyncOperator asyncOperator = new AsyncOperator(driverContext, maxConcurrentRequests) {
            @Override
            protected void performAsync(Page inputPage, ActionListener<Page> listener) {
                ActionRunnable<Page> command = new ActionRunnable<>(listener) {
                    @Override
                    protected void doRun() {
                        if (randomInt(100) < 10) {
                            failed.set(true);
                            throw new ElasticsearchException("simulated");
                        }
                        int positionCount = inputPage.getBlock(0).getPositionCount();
                        IntBlock block = globalBlockFactory.newConstantIntBlockWith(between(1, 100), positionCount);
                        listener.onResponse(inputPage.appendPage(new Page(block)));
                    }
                };
                if (randomBoolean()) {
                    command.run();
                } else {
                    TimeValue delay = TimeValue.timeValueMillis(randomIntBetween(0, 50));
                    threadPool.schedule(command, delay, threadPool.executor(ESQL_TEST_EXECUTOR));
                }
            }

            @Override
            protected void doClose() {

            }
        };
        SinkOperator outputOperator = new PageConsumerOperator(Page::releaseBlocks);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        Driver driver = new Driver(driverContext, sourceOperator, List.of(asyncOperator), outputOperator, localBreaker);
        Driver.start(threadPool.getThreadContext(), threadPool.executor(ESQL_TEST_EXECUTOR), driver, between(1, 1000), future);
        assertBusy(() -> assertTrue(future.isDone()));
        if (failed.get()) {
            ElasticsearchException error = expectThrows(ElasticsearchException.class, future::actionGet);
            assertThat(error.getMessage(), containsString("simulated"));
            error = expectThrows(ElasticsearchException.class, asyncOperator::isFinished);
            assertThat(error.getMessage(), containsString("simulated"));
            error = expectThrows(ElasticsearchException.class, asyncOperator::getOutput);
            assertThat(error.getMessage(), containsString("simulated"));
        } else {
            assertTrue(asyncOperator.isFinished());
            assertNull(asyncOperator.getOutput());
        }
    }

    public void testIsFinished() {
        int iters = iterations(10, 10_000);
        BlockFactory blockFactory = blockFactory();
        for (int i = 0; i < iters; i++) {
            DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory);
            CyclicBarrier barrier = new CyclicBarrier(2);
            AsyncOperator asyncOperator = new AsyncOperator(driverContext, between(1, 10)) {
                @Override
                protected void performAsync(Page inputPage, ActionListener<Page> listener) {
                    ActionRunnable<Page> command = new ActionRunnable<>(listener) {
                        @Override
                        protected void doRun() {
                            try {
                                barrier.await(10, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            }
                            listener.onFailure(new ElasticsearchException("simulated"));
                        }
                    };
                    threadPool.executor(ESQL_TEST_EXECUTOR).execute(command);
                }

                @Override
                protected void doClose() {

                }
            };
            asyncOperator.addInput(new Page(blockFactory.newConstantIntBlockWith(randomInt(), between(1, 10))));
            asyncOperator.finish();
            try {
                barrier.await(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            int numChecks = between(10, 100);
            while (--numChecks >= 0) {
                try {
                    assertFalse("must not finished or failed", asyncOperator.isFinished());
                } catch (ElasticsearchException e) {
                    assertThat(e.getMessage(), equalTo("simulated"));
                    break;
                }
            }
            driverContext.finish();
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            driverContext.waitForAsyncActions(future);
            future.actionGet(30, TimeUnit.SECONDS);
        }
    }

    static class LookupService {
        private final ThreadPool threadPool;
        private final Map<Long, String> dict;
        private final int maxConcurrentRequests;
        private final AtomicInteger pendingRequests = new AtomicInteger();
        private final BlockFactory blockFactory;

        LookupService(ThreadPool threadPool, BlockFactory blockFactory, Map<Long, String> dict, int maxConcurrentRequests) {
            this.threadPool = threadPool;
            this.dict = dict;
            this.blockFactory = blockFactory;
            this.maxConcurrentRequests = maxConcurrentRequests;
        }

        public void lookupAsync(Page input, ActionListener<Page> listener) {
            int total = pendingRequests.incrementAndGet();
            assert total <= maxConcurrentRequests : "too many pending requests: total=" + total + ",limit=" + maxConcurrentRequests;
            ActionRunnable<Page> command = new ActionRunnable<>(listener) {
                @Override
                protected void doRun() {
                    int current = pendingRequests.decrementAndGet();
                    assert current >= 0 : "pending requests must be non-negative";
                    LongBlock ids = input.getBlock(0);
                    try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(ids.getPositionCount())) {
                        for (int i = 0; i < ids.getPositionCount(); i++) {
                            String v = dict.get(ids.getLong(i));
                            if (v != null) {
                                builder.appendBytesRef(new BytesRef(v));
                            } else {
                                builder.appendNull();
                            }
                        }
                        Page result = input.appendPage(new Page(builder.build()));
                        listener.onResponse(result);
                    }
                }
            };
            TimeValue delay = TimeValue.timeValueMillis(randomIntBetween(0, 50));
            threadPool.schedule(command, delay, threadPool.executor(ESQL_TEST_EXECUTOR));
        }
    }

    protected BlockFactory blockFactory() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        BlockFactory factory = new MockBlockFactory(breaker, bigArrays);
        blockFactories.add(factory);
        return factory;
    }

    private final List<CircuitBreaker> breakers = new ArrayList<>();
    private final List<BlockFactory> blockFactories = new ArrayList<>();

    @After
    public void allBreakersEmpty() throws Exception {
        // first check that all big arrays are released, which can affect breakers
        MockBigArrays.ensureAllArraysAreReleased();

        for (CircuitBreaker breaker : breakers) {
            for (var factory : blockFactories) {
                if (factory instanceof MockBlockFactory mockBlockFactory) {
                    mockBlockFactory.ensureAllBlocksAreReleased();
                }
            }
            assertThat("Unexpected used in breaker: " + breaker, breaker.getUsed(), equalTo(0L));
        }
    }
}
