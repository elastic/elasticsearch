/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.startsWith;

/**
 * Test case for all operators that support parallel operation in the
 * shape of "single", "initial", "intermediate", and "final" modes.
 */
public abstract class ForkingOperatorTestCase extends OperatorTestCase {

    private static final String ESQL_TEST_EXECUTOR = "esql_test_executor";

    protected abstract Operator.OperatorFactory simpleWithMode(SimpleOptions options, AggregatorMode mode);

    /**
     * Calls {@link #simpleWithMode(SimpleOptions, AggregatorMode)} with the default options.
     */
    protected final Operator.OperatorFactory simpleWithMode(AggregatorMode mode) {
        return simpleWithMode(SimpleOptions.DEFAULT, mode);
    }

    @Override
    protected final Operator.OperatorFactory simple(SimpleOptions options) {
        return simpleWithMode(options, AggregatorMode.SINGLE);
    }

    public final void testInitialFinal() {
        DriverContext driverContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), between(1_000, 100_000)));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = new ArrayList<>();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(input.iterator()),
                List.of(simpleWithMode(AggregatorMode.INITIAL).get(driverContext), simpleWithMode(AggregatorMode.FINAL).get(driverContext)),
                new TestResultPageSinkOperator(page -> results.add(page))
            )
        ) {
            runDriver(d);
        }
        assertSimpleOutput(origInput, results);
        assertDriverContext(driverContext);
    }

    public final void testManyInitialFinal() {
        DriverContext driverContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), between(1_000, 100_000)));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> partials = oneDriverPerPage(input, () -> List.of(simpleWithMode(AggregatorMode.INITIAL).get(driverContext)));
        List<Page> results = new ArrayList<>();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(partials.iterator()),
                List.of(simpleWithMode(AggregatorMode.FINAL).get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            runDriver(d);
        }
        assertSimpleOutput(origInput, results);
        assertDriverContext(driverContext);
    }

    public final void testInitialIntermediateFinal() {
        DriverContext driverContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), between(1_000, 100_000)));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = new ArrayList<>();

        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(input.iterator()),
                List.of(
                    simpleWithMode(AggregatorMode.INITIAL).get(driverContext),
                    simpleWithMode(AggregatorMode.INTERMEDIATE).get(driverContext),
                    simpleWithMode(AggregatorMode.FINAL).get(driverContext)
                ),
                new TestResultPageSinkOperator(page -> results.add(page))
            )
        ) {
            runDriver(d);
        }
        assertSimpleOutput(origInput, results);
        assertDriverContext(driverContext);
    }

    public final void testManyInitialManyPartialFinal() {
        DriverContext driverContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), between(1_000, 100_000)));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());

        List<Page> partials = oneDriverPerPage(input, () -> List.of(simpleWithMode(AggregatorMode.INITIAL).get(driverContext)));
        Collections.shuffle(partials, random());
        List<Page> intermediates = oneDriverPerPageList(
            randomSplits(partials).iterator(),
            () -> List.of(simpleWithMode(AggregatorMode.INTERMEDIATE).get(driverContext))
        );

        List<Page> results = new ArrayList<>();
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(intermediates.iterator()),
                List.of(simpleWithMode(AggregatorMode.FINAL).get(driverContext)),
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            runDriver(d);
        }
        assertSimpleOutput(origInput, results);
        assertDriverContext(driverContext);
    }

    // Similar to testManyInitialManyPartialFinal, but uses with the DriverRunner infrastructure
    // to move the data through the pipeline.
    public final void testManyInitialManyPartialFinalRunner() {
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext().blockFactory(), between(1_000, 100_000)));
        List<Page> origInput = BlockTestUtils.deepCopyOf(input, TestBlockFactory.getNonBreakingInstance());
        List<Page> results = new ArrayList<>();
        List<Driver> drivers = createDriversForInput(input, results, false /* no throwing ops */);
        var runner = new DriverRunner(threadPool.getThreadContext()) {
            @Override
            protected void start(Driver driver, ActionListener<Void> listener) {
                Driver.start(threadPool.getThreadContext(), threadPool.executor(ESQL_TEST_EXECUTOR), driver, between(1, 10000), listener);
            }
        };
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        runner.runToCompletion(drivers, future);
        future.actionGet(TimeValue.timeValueMinutes(1));
        assertSimpleOutput(origInput, results);
        drivers.stream().map(Driver::driverContext).forEach(OperatorTestCase::assertDriverContext);
    }

    // Similar to testManyInitialManyPartialFinalRunner, but creates a pipeline that contains an
    // operator that throws - fails. The primary motivation for this is to ensure that the driver
    // runner behaves correctly and also releases all resources (bigArrays) appropriately.
    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 100)
    public final void testManyInitialManyPartialFinalRunnerThrowing() throws Exception {
        DriverContext driverContext = driverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(driverContext.blockFactory(), between(1_000, 100_000)));
        List<Page> results = new ArrayList<>();

        List<Driver> drivers = createDriversForInput(input, results, true /* one throwing op */);
        var runner = new DriverRunner(threadPool.getThreadContext()) {
            @Override
            protected void start(Driver driver, ActionListener<Void> listener) {
                Driver.start(threadPool.getThreadContext(), threadPool.executor(ESQL_TEST_EXECUTOR), driver, between(1, 1000), listener);
            }
        };
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        runner.runToCompletion(drivers, future);
        BadException e = expectThrows(BadException.class, () -> future.actionGet(TimeValue.timeValueMinutes(1)));
        assertThat(e.getMessage(), startsWith("bad exception from"));
        Releasables.close(() -> Iterators.map(results.iterator(), p -> p::releaseBlocks));
        drivers.stream().map(Driver::driverContext).forEach(OperatorTestCase::assertDriverContext);
    }

    // Creates a set of drivers that splits the execution into two separate sets of pipelines. The
    // first is a number of source drivers that consume the input (split across them), and output
    // intermediate results. The second is a single operator that consumes intermediate input and
    // produces the final results. The throwingOp param allows to construct a pipeline that will
    // fail by throwing an exception in one of the operators.
    List<Driver> createDriversForInput(List<Page> input, List<Page> results, boolean throwingOp) {
        Collection<List<Page>> splitInput = randomSplits(input, randomIntBetween(2, 4));
        BlockFactory factory = blockFactory();
        ExchangeSinkHandler sinkExchanger = new ExchangeSinkHandler(
            factory,
            randomIntBetween(2, 10),
            threadPool.relativeTimeInMillisSupplier()
        );
        ExchangeSourceHandler sourceExchanger = new ExchangeSourceHandler(randomIntBetween(1, 4), threadPool.executor(ESQL_TEST_EXECUTOR));
        sourceExchanger.addRemoteSink(
            sinkExchanger::fetchPageAsync,
            randomBoolean(),
            () -> {},
            1,
            ActionListener.<Void>noop().delegateResponse((l, e) -> {
                throw new AssertionError("unexpected failure", e);
            })
        );

        Iterator<? extends Operator> intermediateOperatorItr;
        int itrSize = (splitInput.size() * 3) + 3; // 3 inter ops per initial source drivers, and 3 per final
        if (throwingOp) {
            intermediateOperatorItr = randomPassThroughOperatorListWithSingleThrowingOp(itrSize).iterator();
        } else {
            intermediateOperatorItr = IntStream.range(0, itrSize).mapToObj(i -> new PassThroughOperator()).toList().iterator();
        }

        List<Driver> drivers = new ArrayList<>();
        for (List<Page> pages : splitInput) {
            DriverContext driver1Context = driverContext();
            drivers.add(
                TestDriverFactory.create(
                    driver1Context,
                    new CannedSourceOperator(pages.iterator()),
                    List.of(
                        intermediateOperatorItr.next(),
                        simpleWithMode(AggregatorMode.INITIAL).get(driver1Context),
                        intermediateOperatorItr.next(),
                        simpleWithMode(AggregatorMode.INTERMEDIATE).get(driver1Context),
                        intermediateOperatorItr.next()
                    ),
                    new ExchangeSinkOperator(sinkExchanger.createExchangeSink(() -> {}))
                )
            );
        }
        DriverContext driver2Context = driverContext();
        drivers.add(
            TestDriverFactory.create(
                driver2Context,
                new ExchangeSourceOperator(sourceExchanger.createExchangeSource()),
                List.of(
                    intermediateOperatorItr.next(),
                    simpleWithMode(AggregatorMode.INTERMEDIATE).get(driver2Context),
                    intermediateOperatorItr.next(),
                    simpleWithMode(AggregatorMode.FINAL).get(driver2Context),
                    intermediateOperatorItr.next()
                ),
                new TestResultPageSinkOperator(results::add)
            )
        );
        assert intermediateOperatorItr.hasNext() == false;
        return drivers;
    }

    static class BadException extends RuntimeException {
        BadException(String message) {
            super(message);
        }
    }

    // Returns a random list of operators, where all but one are PassThrough and exactly one is a
    // Throwing operator.
    static List<Operator> randomPassThroughOperatorListWithSingleThrowingOp(int size) {
        assert size > 1;
        List<Operator> l = Stream.concat(
            IntStream.range(0, size - 1).mapToObj(i -> new PassThroughOperator()),
            Stream.of(randomThrowingOperator())
        ).collect(toList());
        Collections.shuffle(l, random());
        assert l.size() == size;
        return l;
    }

    static Operator randomThrowingOperator() {
        return randomFrom(
            Set.of(
                new ThrowInNeedsInputOperator(),
                new ThrowInAddInputOperator(),
                new ThrowInGetOutputOperator(),
                new ThrowInIsFinishedOperator(),
                new ThrowInFinishOperator(),
                new ThrowInCloseOperator()
            )
        );
    }

    private static class ThrowInNeedsInputOperator extends PassThroughOperator {
        @Override
        public boolean needsInput() {
            throw new BadException("bad exception from needsInput");
        }
    }

    private static class ThrowInAddInputOperator extends PassThroughOperator {
        @Override
        public void addInput(Page page) {
            super.addInput(page); // retain a reference to the Page, so it is not lost
            throw new BadException("bad exception from addInput");
        }
    }

    private static class ThrowInGetOutputOperator extends PassThroughOperator {
        @Override
        public Page getOutput() {
            throw new BadException("bad exception from getOutput");
        }
    }

    private static class ThrowInIsFinishedOperator extends PassThroughOperator {
        @Override
        public boolean isFinished() {
            throw new BadException("bad exception from isFinished");
        }
    }

    private static class ThrowInFinishOperator extends PassThroughOperator {
        @Override
        public void finish() {
            throw new BadException("bad exception from finish");
        }
    }

    private static class ThrowInCloseOperator extends PassThroughOperator {
        @Override
        public void close() {
            super.close();
            throw new BadException("bad exception from close");
        }
    }

    private static <T> Collection<List<T>> randomSplits(List<T> in) {
        return randomSplits(in, in.size());
    }

    private static <T> Collection<List<T>> randomSplits(List<T> in, int maxGroups) {
        return in.stream().collect(Collectors.groupingBy(s -> randomInt(in.size() - 1) % maxGroups)).values();
    }

    private TestThreadPool threadPool;

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

    protected Comparator<Float> floatComparator() {
        return FloatComparator.INSTANCE;
    }

    static final class FloatComparator implements Comparator<Float> {

        static final FloatComparator INSTANCE = new FloatComparator();

        @Override
        public int compare(Float o1, Float o2) {
            float first = o1;
            float second = o2;
            if (first < second) {
                return -1;
            } else if (first == second) {
                return 0;
            } else {
                return 1;
            }
        }
    }
}
