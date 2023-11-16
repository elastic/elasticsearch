/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

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
import org.elasticsearch.compute.data.BlockTestUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AssumptionViolatedException;

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
     * A {@link ByteSizeValue} that is so small any input to the operator
     * will cause it to circuit break. If the operator can't break then
     * throw an {@link AssumptionViolatedException}.
     */
    protected abstract ByteSizeValue smallEnoughToCircuitBreak();

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
     * Run {@link #simple} with a circuit breaker configured by
     * {@link #smallEnoughToCircuitBreak} and assert that it breaks
     * in a sane way.
     */
    public final void testSimpleCircuitBreaking() {
        /*
         * We build two CircuitBreakers - one for the input blocks and one for the operation itself.
         * The input blocks don't count against the memory usage for the limited operator that we
         * build.
         */
        DriverContext inputFactoryContext = driverContext();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, smallEnoughToCircuitBreak())
            .withCircuitBreaking();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(inputFactoryContext.blockFactory(), between(1_000, 10_000)));
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory blockFactory = BlockFactory.getInstance(breaker, bigArrays);
        DriverContext driverContext = new DriverContext(bigArrays, blockFactory);
        boolean[] driverStarted = new boolean[1];
        Exception e = expectThrows(CircuitBreakingException.class, () -> {
            var operator = simple(bigArrays).get(driverContext);
            driverStarted[0] = true;
            drive(operator, input.iterator(), driverContext);
        });
        if (driverStarted[0] == false) {
            // if drive hasn't even started then we need to release the input pages
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(input.iterator(), p -> p::releaseBlocks)));
        }
        assertThat(e.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
        assertThat(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));

        // Note the lack of try/finally here - we're asserting that when the driver throws an exception we clear the breakers.
        assertThat(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
        assertThat(inputFactoryContext.breaker().getUsed(), equalTo(0L));
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

        boolean driverStarted = false;
        try {
            Operator operator = simple(driverContext.bigArrays()).get(driverContext);
            driverStarted = true;
            drive(operator, input.iterator(), driverContext);
            // Either we get lucky and cranky doesn't throw and the test completes or we don't and it throws
        } catch (CircuitBreakingException e) {
            logger.info("broken", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
        if (driverStarted == false) {
            // if drive hasn't even started then we need to release the input pages
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(input.iterator(), p -> p::releaseBlocks)));
        }

        // Note the lack of try/finally here - we're asserting that when the driver throws an exception we clear the breakers.
        assertThat(inputFactoryContext.breaker().getUsed(), equalTo(0L));
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
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, BlockFactory.getNonBreakingInstance());
        BigArrays bigArrays = context.bigArrays().withCircuitBreaking();

        List<Page> results = drive(simple(bigArrays).get(context), input.iterator(), context);
        assertSimpleOutput(origInput, results);
        assertThat(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));

        List<Block> resultBlocks = new ArrayList<>();
        // Release all result blocks. After this, all input blocks should be released as well, otherwise we have a leak.
        for (Page p : results) {
            for (int i = 0; i < p.getBlockCount(); i++) {
                resultBlocks.add(p.getBlock(i));
            }

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
        assert input.size() == 1 : "Expected single page, got: " + input;
        // eventually, when driverContext always returns a tracking factory, we can enable this assertion
        // assertThat(driverContext.blockFactory().breaker().getUsed(), greaterThan(0L));
        Page page = input.get(0);
        try (var operator = simple(driverContext.bigArrays()).get(driverContext)) {
            assert operator.needsInput();
            operator.addInput(page);
            operator.finish();
        }
        assertThat(driverContext.blockFactory().breaker().getUsed(), equalTo(0L));
    }

    protected final List<Page> drive(Operator operator, Iterator<Page> input, DriverContext driverContext) {
        return drive(List.of(operator), input, driverContext);
    }

    protected final List<Page> drive(List<Operator> operators, Iterator<Page> input, DriverContext driverContext) {
        List<Page> results = new ArrayList<>();
        boolean success = false;
        try (
            Driver d = new Driver(
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
                    "dummy-session",
                    new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, BlockFactory.getNonBreakingInstance()),
                    () -> "dummy-driver",
                    new SequenceLongBlockSourceOperator(
                        BlockFactory.getNonBreakingInstance(),
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
