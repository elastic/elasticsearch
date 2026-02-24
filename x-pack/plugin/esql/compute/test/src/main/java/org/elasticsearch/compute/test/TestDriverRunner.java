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
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.LongStream;

import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.terminate;

/**
 * Utility for running {@link Driver} with configurations customized for tests.
 * See {@link #builder} for lots of utilities for building the {@link Driver}.
 */
public class TestDriverRunner {
    private Integer numThreads = null;

    /**
     * Set the number of threads use to run the driver. If this isn't called
     * we run the driver with a random number of threads.
     */
    public TestDriverRunner numThreads(int numThreads) {
        this.numThreads = numThreads;
        return this;
    }

    /**
     * Helpers for building the {@link Driver} out of {@link Operator}
     * or {@link Operator.OperatorFactory}.
     * <p>
     *     Generally you can use it like:
     * </p>
     * <pre>{@code
     *   var runner = new TestDriverRunner().builder(driverContext());
     *   runner.input(buildInput(runner.blockFactory()));
     *   List<Page> results = runner.run(operatorFactory);
     * }</pre>
     * <p>
     *     If you need the inputs to assert the contents of the outputs
     *     then use {@link DriverBuilder#collectDeepCopy()}:
     * </p>
     * <pre>{@code
     *   var runner = new TestDriverRunner().builder(driverContext()).collectDeepCopy();
     *   runner.input(buildInput(runner.blockFactory()));
     *   assertResults(runner.deepCopy(), runner.run(operatorFactory));
     * }</pre>
     */
    public DriverBuilder builder(DriverContext context) {
        return new DriverBuilder(context);
    }

    /**
     * Run a driver.
     */
    public void run(Driver driver) {
        run(List.of(driver));
    }

    /**
     * Run many drivers.
     */
    public void run(List<Driver> drivers) {
        drivers = new ArrayList<>(drivers);
        int dummyDrivers = between(0, 10);
        for (int i = 0; i < dummyDrivers; i++) {
            drivers.add(
                TestDriverFactory.create(
                    new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null),
                    new SequenceLongBlockSourceOperator(
                        TestBlockFactory.getNonBreakingInstance(),
                        LongStream.range(0, between(1, 100)),
                        between(1, 100)
                    ),
                    List.of(),
                    new PageConsumerOperator(Page::releaseBlocks)
                )
            );
        }
        Randomness.shuffle(drivers);
        int numThreads = this.numThreads == null ? between(1, 16) : this.numThreads;
        ThreadPool threadPool = new TestThreadPool(
            "test",
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

    /**
     * Helpers for building the {@link Driver}.
     */
    public class DriverBuilder {
        private final DriverContext context;

        private boolean collectDeepCopy = false;
        private boolean insertNulls = false;
        private List<Page> deepCopy;
        private SourceOperator input;
        private List<Operator.Status> statuses;

        DriverBuilder(DriverContext context) {
            this.context = context;
        }

        /**
         * Asks the runner to collect a deep copy of the input. Must be called before
         * any call to {@link #input(CannedSourceOperator)} or it's overwrites.
         */
        public DriverBuilder collectDeepCopy() {
            if (input != null) {
                throw new IllegalStateException("must be called before `input`");
            }
            collectDeepCopy = true;
            return this;
        }

        /**
         * Asks the runner to insert null rows. Must be called before
         * any call to {@link #input(CannedSourceOperator)} or it's overwrites.
         */
        public DriverBuilder insertNulls() {
            if (input != null) {
                throw new IllegalStateException("must be called before `input`");
            }
            insertNulls = true;
            return this;
        }

        /**
         * Builds a {@link Page}, then a {@link CannedSourceOperator}
         * and delegates to {@link #input(CannedSourceOperator)}.
         */
        public DriverBuilder input(List<Block> input) {
            return input(input.toArray(Block[]::new));
        }

        /**
         * Builds a {@link Page}, then a {@link CannedSourceOperator}
         * and delegates to {@link #input(CannedSourceOperator)}.
         */
        public DriverBuilder input(Block... input) {
            return input(new Page(input));
        }

        /**
         * Builds a {@link CannedSourceOperator} and delegates
         * to {@link #input(CannedSourceOperator)}.
         */
        public DriverBuilder input(Page input) {
            return input(Iterators.single(input));
        }

        /**
         * Builds a {@link CannedSourceOperator} and delegates
         * to {@link #input(CannedSourceOperator)}.
         */
        public DriverBuilder input(Page... input) {
            return input(List.of(input));
        }

        /**
         * Builds a {@link CannedSourceOperator} and delegates
         * to {@link #input(CannedSourceOperator)}.
         */
        public DriverBuilder input(Iterable<Page> input) {
            return input(input.iterator());
        }

        /**
         * Builds a {@link CannedSourceOperator} and delegates
         * to {@link #input(CannedSourceOperator)}.
         */
        public DriverBuilder input(Iterator<Page> input) {
            return input(new CannedSourceOperator(input));
        }

        /**
         * Collects input from a {@link SourceOperator.SourceOperatorFactory}, builds a
         * {@link CannedSourceOperator}, and delegates to {@link #input(CannedSourceOperator)}.
         */
        public DriverBuilder input(SourceOperator.SourceOperatorFactory input) {
            return input(input.get(context));
        }

        /**
         * Collects input from a {@link SourceOperator}, builds a
         * {@link CannedSourceOperator}, and delegates to {@link #input(CannedSourceOperator)}.
         */
        public DriverBuilder input(SourceOperator input) {
            return input(CannedSourceOperator.collectPages(input));
        }

        /**
         * Sets the input passed to the driver.
         */
        public DriverBuilder input(CannedSourceOperator input) {
            if (this.input != null) {
                throw new IllegalStateException("already added input");
            }
            SourceOperator source = input;
            if (insertNulls) {
                source = new NullInsertingSourceOperator(input, blockFactory());
            }
            if (collectDeepCopy) {
                List<Page> collected = CannedSourceOperator.collectPages(source);
                this.deepCopy = BlockTestUtils.deepCopyOf(collected, TestBlockFactory.getNonBreakingInstance());
                this.input = new CannedSourceOperator(collected.iterator());
            } else {
                this.input = source;
            }
            return this;
        }

        /**
         * Run a single driver, returning the result.
         */
        public List<Page> run(Operator.OperatorFactory operator) {
            return run(operator.get(context));
        }

        /**
         * Run a single driver, returning the result.
         */
        public List<Page> run(Operator.OperatorFactory... operators) {
            return run(List.of(operators).stream().map(f -> f.get(context)).toList());
        }

        /**
         * Run a single driver, returning the result.
         */
        public List<Page> run(Operator operator) {
            return run(List.of(operator));
        }

        /**
         * Run a single driver, returning the result.
         */
        public List<Page> run(Operator... operators) {
            return run(List.of(operators));
        }

        /**
         * Run a single driver, returning the result.
         */
        public List<Page> run(List<Operator> operators) {
            List<Page> results = new ArrayList<>();
            boolean success = false;
            try (Driver d = TestDriverFactory.create(context, input, operators, new TestResultPageSinkOperator(results::add))) {
                TestDriverRunner.this.run(d);
                success = true;
            } finally {
                if (success == false) {
                    Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(results.iterator(), p -> p::releaseBlocks)));
                }
            }
            statuses = operators.stream().map(Operator::status).toList();
            return results;
        }

        /**
         * A deep copy of the inputs. Available only after calling
         * {@link #collectDeepCopy()} and {@link #input(CannedSourceOperator)}.
         */
        public List<Page> deepCopy() {
            if (collectDeepCopy == false) {
                throw new IllegalStateException("didn't collect deep copy");
            }
            if (deepCopy == null) {
                throw new IllegalStateException("input not called");
            }
            return deepCopy;
        }

        /**
         * The status of the completed operators. Available after
         * {@link #run(List)}.
         */
        public List<Operator.Status> statuses() {
            if (statuses == null) {
                throw new IllegalStateException("didn't run");
            }
            return statuses;
        }

        /**
         * The {@link DriverContext}. Always available.
         */
        public DriverContext context() {
            return context;
        }

        /**
         * The {@link BlockFactory}. Always available.
         */
        public BlockFactory blockFactory() {
            return context.blockFactory();
        }
    }
}
