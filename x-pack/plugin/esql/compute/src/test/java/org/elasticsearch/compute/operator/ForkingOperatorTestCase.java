/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    protected abstract Operator.OperatorFactory simpleWithMode(BigArrays bigArrays, AggregatorMode mode);

    @Override
    protected final Operator.OperatorFactory simple(BigArrays bigArrays) {
        return simpleWithMode(bigArrays, AggregatorMode.SINGLE);
    }

    public final void testInitialFinal() {
        BigArrays bigArrays = nonBreakingBigArrays();
        DriverContext driverContext = new DriverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(between(1_000, 100_000)));
        List<Page> results = new ArrayList<>();

        try (
            Driver d = new Driver(
                driverContext,
                new CannedSourceOperator(input.iterator()),
                List.of(
                    simpleWithMode(bigArrays, AggregatorMode.INITIAL).get(driverContext),
                    simpleWithMode(bigArrays, AggregatorMode.FINAL).get(driverContext)
                ),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(input, results);
        assertDriverContext(driverContext);
    }

    public final void testManyInitialFinal() {
        BigArrays bigArrays = nonBreakingBigArrays();
        DriverContext driverContext = new DriverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(between(1_000, 100_000)));
        List<Page> partials = oneDriverPerPage(input, () -> List.of(simpleWithMode(bigArrays, AggregatorMode.INITIAL).get(driverContext)));
        List<Page> results = new ArrayList<>();
        try (
            Driver d = new Driver(
                driverContext,
                new CannedSourceOperator(partials.iterator()),
                List.of(simpleWithMode(bigArrays, AggregatorMode.FINAL).get(driverContext)),
                new PageConsumerOperator(results::add),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(input, results);
        assertDriverContext(driverContext);
    }

    public final void testInitialIntermediateFinal() {
        BigArrays bigArrays = nonBreakingBigArrays();
        DriverContext driverContext = new DriverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(between(1_000, 100_000)));
        List<Page> results = new ArrayList<>();

        try (
            Driver d = new Driver(
                driverContext,
                new CannedSourceOperator(input.iterator()),
                List.of(
                    simpleWithMode(bigArrays, AggregatorMode.INITIAL).get(driverContext),
                    simpleWithMode(bigArrays, AggregatorMode.INTERMEDIATE).get(driverContext),
                    simpleWithMode(bigArrays, AggregatorMode.FINAL).get(driverContext)
                ),
                new PageConsumerOperator(page -> results.add(page)),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(input, results);
        assertDriverContext(driverContext);
    }

    public final void testManyInitialManyPartialFinal() {
        BigArrays bigArrays = nonBreakingBigArrays();
        DriverContext driverContext = new DriverContext();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(between(1_000, 100_000)));

        List<Page> partials = oneDriverPerPage(input, () -> List.of(simpleWithMode(bigArrays, AggregatorMode.INITIAL).get(driverContext)));
        Collections.shuffle(partials, random());
        List<Page> intermediates = oneDriverPerPageList(
            randomSplits(partials).iterator(),
            () -> List.of(simpleWithMode(bigArrays, AggregatorMode.INTERMEDIATE).get(driverContext))
        );

        List<Page> results = new ArrayList<>();
        try (
            Driver d = new Driver(
                driverContext,
                new CannedSourceOperator(intermediates.iterator()),
                List.of(simpleWithMode(bigArrays, AggregatorMode.FINAL).get(driverContext)),
                new PageConsumerOperator(results::add),
                () -> {}
            )
        ) {
            d.run();
        }
        assertSimpleOutput(input, results);
        assertDriverContext(driverContext);
    }

    // Similar to testManyInitialManyPartialFinal, but uses with the DriverRunner infrastructure
    // to move the data through the pipeline.
    public final void testManyInitialManyPartialFinalRunner() {
        BigArrays bigArrays = nonBreakingBigArrays();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(between(1_000, 100_000)));
        List<Page> results = new ArrayList<>();

        List<Driver> drivers = createDriversForInput(bigArrays, input, results, false /* no throwing ops */);
        var runner = new DriverRunner() {
            @Override
            protected void start(Driver driver, ActionListener<Void> listener) {
                Driver.start(threadPool.executor("esql_test_executor"), driver, listener);
            }
        };
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        runner.runToCompletion(drivers, future);
        future.actionGet(TimeValue.timeValueMinutes(1));
        assertSimpleOutput(input, results);
        drivers.stream().map(Driver::driverContext).forEach(OperatorTestCase::assertDriverContext);
    }

    // Similar to testManyInitialManyPartialFinalRunner, but creates a pipeline that contains an
    // operator that throws - fails. The primary motivation for this is to ensure that the driver
    // runner behaves correctly and also releases all resources (bigArrays) appropriately.
    public final void testManyInitialManyPartialFinalRunnerThrowing() {
        BigArrays bigArrays = nonBreakingBigArrays();
        List<Page> input = CannedSourceOperator.collectPages(simpleInput(between(1_000, 100_000)));
        List<Page> results = new ArrayList<>();

        List<Driver> drivers = createDriversForInput(bigArrays, input, results, true /* one throwing op */);
        var runner = new DriverRunner() {
            @Override
            protected void start(Driver driver, ActionListener<Void> listener) {
                Driver.start(threadPool.executor("esql_test_executor"), driver, listener);
            }
        };
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        runner.runToCompletion(drivers, future);
        BadException e = expectThrows(BadException.class, () -> future.actionGet(TimeValue.timeValueMinutes(1)));
        assertThat(e.getMessage(), startsWith("bad exception from"));
        drivers.stream().map(Driver::driverContext).forEach(OperatorTestCase::assertDriverContext);
    }

    // Creates a set of drivers that splits the execution into two separate sets of pipelines. The
    // first is a number of source drivers that consume the input (split across them), and output
    // intermediate results. The second is a single operator that consumes intermediate input and
    // produces the final results. The throwingOp param allows to construct a pipeline that will
    // fail by throwing an exception in one of the operators.
    List<Driver> createDriversForInput(BigArrays bigArrays, List<Page> input, List<Page> results, boolean throwingOp) {
        Collection<List<Page>> splitInput = randomSplits(input, randomIntBetween(2, 4));

        ExchangeSinkHandler sinkExchanger = new ExchangeSinkHandler(randomIntBetween(2, 10), threadPool::relativeTimeInMillis);
        ExchangeSourceHandler sourceExchanger = new ExchangeSourceHandler(
            randomIntBetween(1, 4),
            threadPool.executor("esql_test_executor")
        );
        sourceExchanger.addRemoteSink(sinkExchanger::fetchPageAsync, 1);

        Iterator<? extends Operator> intermediateOperatorItr;
        int itrSize = (splitInput.size() * 3) + 3; // 3 inter ops per initial source drivers, and 3 per final
        if (throwingOp) {
            intermediateOperatorItr = randomPassThroughOperatorListWithSingleThrowingOp(itrSize).iterator();
        } else {
            intermediateOperatorItr = IntStream.range(0, itrSize).mapToObj(i -> new PassThroughOperator()).toList().iterator();
        }

        List<Driver> drivers = new ArrayList<>();
        for (List<Page> pages : splitInput) {
            DriverContext driver1Context = new DriverContext();
            drivers.add(
                new Driver(
                    driver1Context,
                    new CannedSourceOperator(pages.iterator()),
                    List.of(
                        intermediateOperatorItr.next(),
                        simpleWithMode(bigArrays, AggregatorMode.INITIAL).get(driver1Context),
                        intermediateOperatorItr.next(),
                        simpleWithMode(bigArrays, AggregatorMode.INTERMEDIATE).get(driver1Context),
                        intermediateOperatorItr.next()
                    ),
                    new ExchangeSinkOperator(sinkExchanger.createExchangeSink()),
                    () -> {}
                )
            );
        }
        DriverContext driver2Context = new DriverContext();
        drivers.add(
            new Driver(
                driver2Context,
                new ExchangeSourceOperator(sourceExchanger.createExchangeSource()),
                List.of(
                    intermediateOperatorItr.next(),
                    simpleWithMode(bigArrays, AggregatorMode.INTERMEDIATE).get(driver2Context),
                    intermediateOperatorItr.next(),
                    simpleWithMode(bigArrays, AggregatorMode.FINAL).get(driver2Context),
                    intermediateOperatorItr.next()
                ),
                new PageConsumerOperator(results::add),
                () -> {}
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
            new FixedExecutorBuilder(Settings.EMPTY, "esql_test_executor", numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }
}
