/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * A driver operates single-threadedly on a simple chain of {@link Operator}s, passing
 * {@link Page}s from one operator to the next. It also controls the lifecycle of the
 * operators.
 * The operator chain typically starts with a source operator (i.e. an operator that purely produces pages)
 * and ends with a sink operator (i.e. an operator that purely consumes pages).
 *
 * More details on how this integrates with other components can be found in the package documentation of
 * {@link org.elasticsearch.xpack.sql.action.compute}
 */
public class Driver implements Runnable {

    private final List<Operator> activeOperators;
    private final Releasable releasable;

    /**
     * Creates a new driver with a chain of operators.
     * @param operators  the chain of operators to execute
     * @param releasable a {@link Releasable} to invoked once the chain of operators has run to completion
     */
    public Driver(List<Operator> operators, Releasable releasable) {
        this.activeOperators = new ArrayList<>(operators);
        this.releasable = releasable;
    }

    /**
     * Convenience method to run the chain of operators to completion. Does not leverage
     * the non-blocking nature of operators, but keeps busy-spinning when an operator is
     * blocked.
     */
    @Override
    public void run() {
        while (run(TimeValue.MAX_VALUE, Integer.MAX_VALUE) != Operator.NOT_BLOCKED)
            ;
    }

    /**
     * Runs computations on the chain of operators for a given maximum amount of time or iterations.
     * Returns a blocked future when the chain of operators is blocked, allowing the caller
     * thread to do other work instead of blocking or busy-spinning on the blocked operator.
     */
    public ListenableActionFuture<Void> run(TimeValue maxTime, int maxIterations) {
        long maxTimeNanos = maxTime.nanos();
        long startTime = System.nanoTime();
        int iter = 0;
        while (isFinished() == false) {
            ListenableActionFuture<Void> fut = runSingleLoopIteration();
            if (fut.isDone() == false) {
                return fut;
            }
            if (++iter >= maxIterations) {
                break;
            }
            long now = System.nanoTime();
            if (now - startTime > maxTimeNanos) {
                break;
            }
        }
        if (isFinished()) {
            releasable.close();
        }
        return Operator.NOT_BLOCKED;
    }

    /**
     * Whether the driver has run the chain of operators to completion.
     */
    public boolean isFinished() {
        return activeOperators.isEmpty();
    }

    private ListenableActionFuture<Void> runSingleLoopIteration() {

        boolean movedPage = false;

        for (int i = 0; i < activeOperators.size() - 1; i++) {
            Operator op = activeOperators.get(i);
            Operator nextOp = activeOperators.get(i + 1);

            // skip blocked operator
            if (op.isBlocked().isDone() == false) {
                continue;
            }

            if (op.isFinished() == false && nextOp.isBlocked().isDone() && nextOp.needsInput()) {
                Page page = op.getOutput();
                if (page != null && page.getPositionCount() != 0) {
                    nextOp.addInput(page);
                    movedPage = true;
                }
            }

            if (op.isFinished()) {
                nextOp.finish();
            }
        }

        for (int index = activeOperators.size() - 1; index >= 0; index--) {
            if (activeOperators.get(index).isFinished()) {
                // close and remove this operator and all source operators
                List<Operator> finishedOperators = this.activeOperators.subList(0, index + 1);
                finishedOperators.stream().forEach(Operator::close);
                finishedOperators.clear();

                // Finish the next operator, which is now the first operator.
                if (activeOperators.isEmpty() == false) {
                    Operator newRootOperator = activeOperators.get(0);
                    newRootOperator.finish();
                }
                break;
            }
        }

        if (movedPage == false) {
            return oneOf(
                activeOperators.stream().map(Operator::isBlocked).filter(laf -> laf.isDone() == false).collect(Collectors.toList())
            );
        }
        return Operator.NOT_BLOCKED;
    }

    public static void runToCompletion(Executor executor, List<Driver> drivers) {
        start(executor, drivers).actionGet();
    }

    public static ListenableActionFuture<Void> start(Executor executor, List<Driver> drivers) {
        TimeValue maxTime = TimeValue.timeValueMillis(200);
        int maxIterations = 10000;
        List<ListenableActionFuture<Void>> futures = new ArrayList<>();
        for (Driver driver : drivers) {
            futures.add(schedule(maxTime, maxIterations, executor, driver));
        }
        return Driver.allOf(futures);
    }

    private static ListenableActionFuture<Void> schedule(TimeValue maxTime, int maxIterations, Executor executor, Driver driver) {
        ListenableActionFuture<Void> future = new ListenableActionFuture<>();
        executor.execute(new ActionRunnable<>(future) {
            @Override
            protected void doRun() {
                if (driver.isFinished()) {
                    future.onResponse(null);
                    return;
                }
                ListenableActionFuture<Void> fut = driver.run(maxTime, maxIterations);
                if (fut.isDone()) {
                    schedule(maxTime, maxIterations, executor, driver).addListener(future);
                } else {
                    fut.addListener(
                        ActionListener.wrap(
                            ignored -> schedule(maxTime, maxIterations, executor, driver).addListener(future),
                            e -> future.onFailure(e)
                        )
                    );
                }
            }
        });
        return future;
    }

    private static ListenableActionFuture<Void> oneOf(List<ListenableActionFuture<Void>> futures) {
        if (futures.isEmpty()) {
            return Operator.NOT_BLOCKED;
        }
        if (futures.size() == 1) {
            return futures.get(0);
        }
        ListenableActionFuture<Void> oneOf = new ListenableActionFuture<>();
        for (ListenableActionFuture<Void> fut : futures) {
            fut.addListener(oneOf);
        }
        return oneOf;
    }

    private static ListenableActionFuture<Void> allOf(List<ListenableActionFuture<Void>> futures) {
        if (futures.isEmpty()) {
            return Operator.NOT_BLOCKED;
        }
        if (futures.size() == 1) {
            return futures.get(0);
        }
        ListenableActionFuture<Void> allOf = new ListenableActionFuture<>();
        for (ListenableActionFuture<Void> fut : futures) {
            fut.addListener(ActionListener.wrap(ignored -> {
                if (futures.stream().allMatch(BaseFuture::isDone)) {
                    allOf.onResponse(null);
                }
            }, e -> allOf.onFailure(e)));
        }
        return allOf;
    }
}
