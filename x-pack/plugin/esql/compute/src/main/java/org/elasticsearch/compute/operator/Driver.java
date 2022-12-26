/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A driver operates single-threadedly on a simple chain of {@link Operator}s, passing
 * {@link Page}s from one operator to the next. It also controls the lifecycle of the
 * operators.
 * The operator chain typically starts with a source operator (i.e. an operator that purely produces pages)
 * and ends with a sink operator (i.e. an operator that purely consumes pages).
 *
 * More details on how this integrates with other components can be found in the package documentation of
 * {@link org.elasticsearch.compute}
 */
@Experimental
public class Driver implements Runnable, Releasable {

    private final List<Operator> activeOperators;
    private final Releasable releasable;

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final AtomicReference<ListenableActionFuture<Void>> blocked = new AtomicReference<>();

    /**
     * Creates a new driver with a chain of operators.
     * @param source source operator
     * @param intermediateOperators  the chain of operators to execute
     * @param sink sink operator
     * @param releasable a {@link Releasable} to invoked once the chain of operators has run to completion
     */
    public Driver(SourceOperator source, List<Operator> intermediateOperators, SinkOperator sink, Releasable releasable) {
        this.activeOperators = new ArrayList<>();
        activeOperators.add(source);
        activeOperators.addAll(intermediateOperators);
        activeOperators.add(sink);
        this.releasable = releasable;
    }

    /**
     * Convenience method to run the chain of operators to completion. Does not leverage
     * the non-blocking nature of operators, but keeps busy-spinning when an operator is
     * blocked.
     */
    @Override
    public void run() {   // TODO this is dangerous because it doesn't close the Driver.
        while (run(TimeValue.MAX_VALUE, Integer.MAX_VALUE) != Operator.NOT_BLOCKED)
            ;
    }

    /**
     * Runs computations on the chain of operators for a given maximum amount of time or iterations.
     * Returns a blocked future when the chain of operators is blocked, allowing the caller
     * thread to do other work instead of blocking or busy-spinning on the blocked operator.
     */
    public ListenableActionFuture<Void> run(TimeValue maxTime, int maxIterations) {
        if (cancelled.get()) {
            throw new CancellationException();
        }
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

    @Override
    public void close() {
        Releasables.close(activeOperators);
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
                /*
                 * Close and remove this operator and all source operators in the
                 * most paranoid possible way. Closing operators shouldn't throw,
                 * but if it does, this will make sure we don't try to close any
                 * that succeed twice.
                 */
                List<Operator> finishedOperators = this.activeOperators.subList(0, index + 1);
                Iterator<Operator> itr = finishedOperators.iterator();
                while (itr.hasNext()) {
                    itr.next().close();
                    itr.remove();
                }

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

    public static void start(Executor executor, List<Driver> drivers, Consumer<List<Result>> listener) {
        if (drivers.isEmpty()) {
            listener.accept(List.of());
            return;
        }
        TimeValue maxTime = TimeValue.timeValueMillis(200);
        int maxIterations = 10000;
        CountDown counter = new CountDown(drivers.size());
        AtomicArray<Result> results = new AtomicArray<>(drivers.size());

        for (int d = 0; d < drivers.size(); d++) {
            int index = d;
            schedule(maxTime, maxIterations, executor, drivers.get(d), new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    results.setOnce(index, Result.success());
                    if (counter.countDown()) {
                        done();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    drivers.stream().forEach(d -> {
                        synchronized (d) {
                            d.cancelled.set(true);
                            ListenableActionFuture<Void> fut = d.blocked.get();
                            if (fut != null) {
                                fut.onFailure(new CancellationException());
                            }
                        }
                    });
                    results.set(index, Result.failure(e));
                    if (counter.countDown()) {
                        done();
                    }
                }

                private void done() {
                    listener.accept(results.asList());
                }
            });
        }
    }

    public static class Result {
        public static RuntimeException collectFailures(List<Driver.Result> results) {
            List<Exception> failures = results.stream().filter(r -> r.isSuccess() == false).map(r -> r.getFailure()).toList();
            if (failures.isEmpty()) {
                return null;
            }
            List<Exception> failuresToReport = failures.stream().filter(e -> e instanceof CancellationException == false).toList();
            failuresToReport = failuresToReport.isEmpty() ? failures : failuresToReport;
            Iterator<Exception> e = failuresToReport.iterator();
            ElasticsearchException result = new ElasticsearchException("compute engine failure", e.next());
            while (e.hasNext()) {
                result.addSuppressed(e.next());
            }
            return result;
        }

        static Result success() {
            return new Result(null);
        }

        static Result failure(Exception e) {
            return new Result(e);
        }

        private final Exception failure;

        private Result(Exception failure) {
            this.failure = failure;
        }

        public boolean isSuccess() {
            return failure == null;
        }

        public Exception getFailure() {
            if (failure == null) {
                throw new IllegalStateException("not a failure");
            }
            return failure;
        }
    }

    private static void schedule(TimeValue maxTime, int maxIterations, Executor executor, Driver driver, ActionListener<Void> listener) {
        executor.execute(new ActionRunnable<>(listener) {
            @Override
            protected void doRun() {
                if (driver.isFinished()) {
                    listener.onResponse(null);
                    return;
                }
                ListenableActionFuture<Void> fut = driver.run(maxTime, maxIterations);
                if (fut.isDone()) {
                    schedule(maxTime, maxIterations, executor, driver, listener);
                } else {
                    synchronized (driver) {
                        if (driver.cancelled.get()) {
                            throw new CancellationException();
                        }
                        driver.blocked.set(fut);
                    }
                    fut.addListener(
                        ActionListener.wrap(ignored -> schedule(maxTime, maxIterations, executor, driver, listener), listener::onFailure)
                    );
                }
            }
        });
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

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[activeOperators=" + activeOperators + "]";
    }
}
