/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.BreakerTestUtil;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Base tests for {@link Operator}s that are not {@link SourceOperator} or {@link SinkOperator}.
 */
public abstract class OperatorTestCase extends AnyOperatorTestCase {
    /**
     * Valid input to be sent to {@link #simple};
     */
    protected abstract SourceOperator simpleInput(BlockFactory blockFactory, int size);

    /**
     * Assert that output from {@link #simple} is correct for the
     * given input.
     */
    protected abstract void assertSimpleOutput(List<Page> input, List<Page> results);

    /**
     * Test a small input set against {@link #simple}. Smaller input sets
     * are more likely to discover accidental behavior for clumped inputs.
     */
    public final void testSimpleSmallInput() {
        assertSimple(driverContext(), between(10, 100));
    }

    /**
     * Test a larger input set against {@link #simple}.
     */
    public final void testSimpleLargeInput() {
        assertSimple(driverContext(), between(1_000, 10_000));
    }

    /**
     * Enough memory for {@link #simple} not to throw a {@link CircuitBreakingException}.
     * It's fine if this is <strong>much</strong> more memory than {@linkplain #simple} needs.
     * When we want to make {@linkplain #simple} throw we'll find the precise amount of memory
     * that'll make it throw with a binary search.
     */
    protected ByteSizeValue enoughMemoryForSimple() {
        return ByteSizeValue.ofGb(1);
    }

    /**
     * Run {@link #simple} with a circuit breaker many times, making sure all blocks
     * are properly released. In particular, we perform a binary search to find the
     * largest amount of memory that'll throw a {@link CircuitBreakingException} with
     * starting bounds of {@code 0b} and {@link #enoughMemoryForSimple}. Then we pick
     * a random amount of memory between {@code 0b} and the maximum and run that,
     * asserting both that this throws a {@link CircuitBreakingException} and releases
     * all pages.
     */
    public final void testSimpleCircuitBreaking() {
        ByteSizeValue memoryLimitForSimple = enoughMemoryForSimple();
        Operator.OperatorFactory simple = simple();
        DriverContext inputFactoryContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(inputFactoryContext.blockFactory(), between(1_000, 10_000)));
        try {
            ByteSizeValue limit = BreakerTestUtil.findBreakerLimit(memoryLimitForSimple, l -> runWithLimit(simple, input, l));
            ByteSizeValue testWithSize = ByteSizeValue.ofBytes(randomLongBetween(0, limit.getBytes()));
            logger.info("testing with {} against a limit of {}", testWithSize, limit);
            Exception e = expectThrows(CircuitBreakingException.class, () -> runWithLimit(simple, input, testWithSize));
            assertThat(e.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(input.iterator(), p -> p::releaseBlocks)));
        }
        assertThat(inputFactoryContext.breaker().getUsed(), equalTo(0L));
    }

    private void runWithLimit(Operator.OperatorFactory factory, List<Page> input, ByteSizeValue limit) {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, limit).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        MockBlockFactory blockFactory = new MockBlockFactory(breaker, bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory);
        List<Page> localInput = CannedSourceOperator.deepCopyOf(blockFactory, input);
        boolean driverStarted = false;
        try {
            var operator = factory.get(driverContext);
            driverStarted = true;
            drive(operator, localInput.iterator(), driverContext);
        } finally {
            if (driverStarted == false) {
                // if drive hasn't even started then we need to release the input pages manually
                Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(localInput.iterator(), p -> p::releaseBlocks)));
            }
            blockFactory.ensureAllBlocksAreReleased();
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

    /**
     * Run {@link #simple} with the {@link CrankyCircuitBreakerService}
     * which fails randomly. This will catch errors caused by not
     * properly cleaning up things like {@link BigArray}s, particularly
     * in ctors.
     */
    public final void testSimpleWithCranky() {
        DriverContext inputFactoryContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(inputFactoryContext.blockFactory(), between(1_000, 10_000)));

        DriverContext driverContext = crankyDriverContext();

        Exception exception = null;
        boolean driverStarted = false;
        try {
            Operator operator = simple().get(driverContext);
            driverStarted = true;
            drive(operator, input.iterator(), driverContext);
            // Either we get lucky and cranky doesn't throw and the test completes or we don't and it throws
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
            exception = e;
        }
        if (driverStarted == false) {
            // if drive hasn't even started then we need to release the input pages
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(input.iterator(), p -> p::releaseBlocks)));
        }

        // Note the lack of try/finally here - we're asserting that when the driver throws an exception we clear the breakers.
        long inputUsedBytes = inputFactoryContext.breaker().getUsed();
        if (inputUsedBytes != 0L) {
            fail(exception, "Expected no used bytes for input, found: " + inputUsedBytes);
        }
        long driverUsedBytes = driverContext.breaker().getUsed();
        if (driverUsedBytes != 0L) {
            fail(exception, "Expected no used bytes for driver, found: " + driverUsedBytes);
        }
    }

    /**
     * Run the {@code operators} once per page in the {@code input}.
     */
    protected final List<Page> oneDriverPerPage(List<Page> input, Supplier<List<Operator>> operators) {
        return oneDriverPerPageList(input.stream().map(List::of).iterator(), operators);
    }

    /**
     * Run the {@code operators} once to entry in the {@code source}.
     */
    protected final List<Page> oneDriverPerPageList(Iterator<List<Page>> source, Supplier<List<Operator>> operators) {
        List<Page> result = new ArrayList<>();
        while (source.hasNext()) {
            List<Page> in = source.next();
            try (
                Driver d = new Driver(
                    "test",
                    driverContext(),
                    new CannedSourceOperator(in.iterator()),
                    operators.get(),
                    new PageConsumerOperator(result::add),
                    () -> {}
                )
            ) {
                runDriver(d);
            }
        }
        return result;
    }

    protected final void assertSimple(DriverContext context, int size) {
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(context.blockFactory(), size));

        List<Block> inputBlocks = new ArrayList<>();
        for (Page p : input) {
            for (int i = 0; i < p.getBlockCount(); i++) {
                inputBlocks.add(p.getBlock(i));
            }
        }

        // Clone the input so that the operator can close it, then, later, we can read it again to build the assertion.
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());

        var operator = simple().get(context);
        List<Page> results = drive(operator, input.iterator(), context);
        assertSimpleOutput(origInput, results);
        assertThat(context.breaker().getUsed(), equalTo(0L));

        // Release all result blocks. After this, all input blocks should be released as well, otherwise we have a leak.
        for (Page p : results) {
            p.releaseBlocks();
        }

        int unreleasedInputs = 0;
        for (Block b : inputBlocks) {
            if (b.isReleased() == false) {
                unreleasedInputs++;
            }
        }
        if (unreleasedInputs > 0) {
            throw new AssertionError("[" + unreleasedInputs + "] unreleased input blocks");
        }
    }

    /**
     * Tests that finish then close without calling {@link Operator#getOutput} to
     * retrieve a potential last page, releases all memory.
     */
    public void testSimpleFinishClose() {
        DriverContext driverContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), 1));
        // eventually, when driverContext always returns a tracking factory, we can enable this assertion
        // assertThat(driverContext.blockFactory().breaker().getUsed(), greaterThan(0L));
        try (var operator = simple().get(driverContext)) {
            assert operator.needsInput();
            for (Page page : input) {
                if (operator.needsInput()) {
                    operator.addInput(page);
                } else {
                    page.releaseBlocks();
                }
            }
            operator.finish();
            // for async operator, we need to wait for async actions to finish.
            if (operator instanceof AsyncOperator<?> || randomBoolean()) {
                driverContext.finish();
                PlainActionFuture<Void> waitForAsync = new PlainActionFuture<>();
                driverContext.waitForAsyncActions(waitForAsync);
                waitForAsync.actionGet(TimeValue.timeValueSeconds(30));
            }
        }
    }

    protected final List<Page> drive(Operator operator, Iterator<Page> input, DriverContext driverContext) {
        return drive(List.of(operator), input, driverContext);
    }

    protected final List<Page> drive(List<Operator> operators, Iterator<Page> input, DriverContext driverContext) {
        List<Page> results = new ArrayList<>();
        boolean success = false;
        try (
            Driver d = new Driver(
                "test",
                driverContext,
                new CannedSourceOperator(input),
                operators,
                new TestResultPageSinkOperator(results::add),
                () -> {}
            )
        ) {
            runDriver(d);
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(results.iterator(), p -> p::releaseBlocks)));
            }
        }
        return results;
    }

    public static void runDriver(Driver driver) {
        runDriver(List.of(driver));
    }

    public static void runDriver(List<Driver> drivers) {
        drivers = new ArrayList<>(drivers);
        int dummyDrivers = between(0, 10);
        for (int i = 0; i < dummyDrivers; i++) {
            drivers.add(
                new Driver(
                    "test",
                    "dummy-session",
                    0,
                    0,
                    new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance()),
                    () -> "dummy-driver",
                    new SequenceLongBlockSourceOperator(
                        TestBlockFactory.getNonBreakingInstance(),
                        LongStream.range(0, between(1, 100)),
                        between(1, 100)
                    ),
                    List.of(),
                    new PageConsumerOperator(page -> page.releaseBlocks()),
                    Driver.DEFAULT_STATUS_INTERVAL,
                    () -> {}
                )
            );
        }
        Randomness.shuffle(drivers);
        int numThreads = between(1, 16);
        ThreadPool threadPool = new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(Settings.EMPTY, "esql", numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        var driverRunner = new DriverRunner(threadPool.getThreadContext()) {
            @Override
            protected void start(Driver driver, ActionListener<Void> driverListener) {
                Driver.start(threadPool.getThreadContext(), threadPool.executor("esql"), driver, between(1, 10000), driverListener);
            }
        };
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        try {
            driverRunner.runToCompletion(drivers, future);
            future.actionGet(TimeValue.timeValueSeconds(30));
        } finally {
            terminate(threadPool);
        }
    }

    public static void assertDriverContext(DriverContext driverContext) {
        assertTrue(driverContext.isFinished());
        assertThat(driverContext.getSnapshot().releasables(), empty());
    }

    public static int randomPageSize() {
        if (randomBoolean()) {
            return between(1, 16);
        } else {
            return between(1, 16 * 1024);
        }
    }
}
