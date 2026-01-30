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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
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
     * Run a driver with a single operator, returning the result.
     */
    public List<Page> run(Operator operator, Iterator<Page> input, DriverContext context) {
        return run(List.of(operator), input, context);
    }

    /**
     * Run a single driver, returning the result.
     */
    public List<Page> run(List<Operator> operators, Iterator<Page> input, DriverContext driverContext) {
        List<Page> results = new ArrayList<>();
        boolean success = false;
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(input),
                operators,
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            run(d);
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(results.iterator(), p -> p::releaseBlocks)));
            }
        }
        return results;
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
}
