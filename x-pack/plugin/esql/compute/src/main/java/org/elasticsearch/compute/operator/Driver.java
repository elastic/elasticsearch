/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
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

public class Driver implements Releasable, Describable {
    public static final TimeValue DEFAULT_TIME_BEFORE_YIELDING = TimeValue.timeValueMinutes(5);
    public static final int DEFAULT_MAX_ITERATIONS = 10_000;
    /**
     * Minimum time between updating status.
     */
    public static final TimeValue DEFAULT_STATUS_INTERVAL = TimeValue.timeValueSeconds(1);

    private final String sessionId;

    /**
     * Description of the task this driver is running. This description should be
     * short and meaningful as a grouping identifier. We use the phase of the
     * query right now: "data", "node_reduce", "final".
     */
    private final String taskDescription;

    /**
     * The wall clock time when this driver was created in milliseconds since epoch.
     * Compared to {@link #startNanos} this is less accurate and is measured by a
     * timer that can go backwards. This is only useful for presenting times to a
     * user, like over the status API.
     */
    private final long startTime;

    /**
     * The time when this driver was created in nanos. This time is relative to
     * some arbitrary point - imagine its program startup. The timer that generates
     * this is monotonically increasing so even if NTP or something changes the
     * clock it won't change. As such, this is only useful for measuring durations.
     */
    private final long startNanos;
    private final DriverContext driverContext;
    private final Supplier<String> description;
    private final List<Operator> activeOperators;
    private final List<DriverStatus.OperatorStatus> statusOfCompletedOperators = new ArrayList<>();
    private final Releasable releasable;
    private final long statusNanos;

    private final AtomicReference<String> cancelReason = new AtomicReference<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final SubscribableListener<Void> completionListener = new SubscribableListener<>();
    private final DriverScheduler scheduler = new DriverScheduler();

    /**
     * Status reported to the tasks API. We write the status at most once every
     * {@link #statusNanos}, as soon as loop has finished and after {@link #statusNanos}
     * have passed.
     */
    private final AtomicReference<DriverStatus> status;

    /**
     * The time this driver finished. Only set once the driver is finished, defaults to 0
     * which is *possibly* a valid value, so always use the driver status to check
     * if the driver is actually finished.
     */
    private long finishNanos;

    /**
     * Creates a new driver with a chain of operators.
     * @param sessionId session Id
     * @param taskDescription Description of the task this driver is running. This
     *                        description should be short and meaningful as a grouping
     *                        identifier. We use the phase of the query right now:
     *                        "data", "node_reduce", "final".
     * @param driverContext the driver context
     * @param source source operator
     * @param intermediateOperators  the chain of operators to execute
     * @param sink sink operator
     * @param statusInterval minimum status reporting interval
     * @param releasable a {@link Releasable} to invoked once the chain of operators has run to completion
     */
    public Driver(
        String sessionId,
        String taskDescription,
        long startTime,
        long startNanos,
        DriverContext driverContext,
        Supplier<String> description,
        SourceOperator source,
        List<Operator> intermediateOperators,
        SinkOperator sink,
        TimeValue statusInterval,
        Releasable releasable
    ) {
        this.sessionId = sessionId;
        this.taskDescription = taskDescription;
        this.startTime = startTime;
        this.startNanos = startNanos;
        this.driverContext = driverContext;
        this.description = description;
        this.activeOperators = new ArrayList<>();
        this.activeOperators.add(source);
        this.activeOperators.addAll(intermediateOperators);
        this.activeOperators.add(sink);
        this.statusNanos = statusInterval.nanos();
        this.releasable = releasable;
        this.status = new AtomicReference<>(
            new DriverStatus(
                sessionId,
                taskDescription,
                startTime,
                System.currentTimeMillis(),
                0,
                0,
                DriverStatus.Status.QUEUED,
                List.of(),
                List.of(),
                DriverSleeps.empty()
            )
        );
    }

    /**
     * Creates a new driver with a chain of operators.
     * @param driverContext the driver context
     * @param source source operator
     * @param intermediateOperators  the chain of operators to execute
     * @param sink sink operator
     * @param releasable a {@link Releasable} to invoked once the chain of operators has run to completion
     */
    public Driver(
        String taskDescription,
        DriverContext driverContext,
        SourceOperator source,
        List<Operator> intermediateOperators,
        SinkOperator sink,
        Releasable releasable
    ) {
        this(
            "unset",
            taskDescription,
            System.currentTimeMillis(),
            System.nanoTime(),
            driverContext,
            () -> null,
            source,
            intermediateOperators,
            sink,
            DEFAULT_STATUS_INTERVAL,
            releasable
        );
    }

    public DriverContext driverContext() {
        return driverContext;
    }

    /**
     * Runs computations on the chain of operators for a given maximum amount of time or iterations.
     * Returns a blocked future when the chain of operators is blocked, allowing the caller
     * thread to do other work instead of blocking or busy-spinning on the blocked operator.
     */
    SubscribableListener<Void> run(TimeValue maxTime, int maxIterations, LongSupplier nowSupplier) {
        updateStatus(0, 0, DriverStatus.Status.RUNNING, "driver running");
        long maxTimeNanos = maxTime.nanos();
        // Start time, used to stop the calculations after maxTime has passed.
        long startTime = nowSupplier.getAsLong();
        // The time of the next forced status update.
        long nextStatus = startTime + statusNanos;
        // Total executed iterations this run, used to stop the calculations after maxIterations have passed.
        int totalIterationsThisRun = 0;
        // The iterations to be reported on the next status update.
        int iterationsSinceLastStatusUpdate = 0;
        // The time passed since the last status update.
        long lastStatusUpdateTime = startTime;
        while (true) {
            IsBlockedResult isBlocked = Operator.NOT_BLOCKED;
            try {
                assert driverContext.assertBeginRunLoop();
                isBlocked = runSingleLoopIteration();
            } catch (DriverEarlyTerminationException unused) {
                closeEarlyFinishedOperators();
                assert isFinished() : "not finished after early termination";
            } finally {
                assert driverContext.assertEndRunLoop();
            }
            totalIterationsThisRun++;
            iterationsSinceLastStatusUpdate++;

            long now = nowSupplier.getAsLong();
            if (isBlocked.listener().isDone() == false) {
                updateStatus(now - lastStatusUpdateTime, iterationsSinceLastStatusUpdate, DriverStatus.Status.ASYNC, isBlocked.reason());
                return isBlocked.listener();
            }
            if (isFinished()) {
                finishNanos = now;
                updateStatus(finishNanos - lastStatusUpdateTime, iterationsSinceLastStatusUpdate, DriverStatus.Status.DONE, "driver done");
                driverContext.finish();
                Releasables.close(releasable, driverContext.getSnapshot());
                return Operator.NOT_BLOCKED.listener();
            }
            if (totalIterationsThisRun >= maxIterations) {
                updateStatus(now - lastStatusUpdateTime, iterationsSinceLastStatusUpdate, DriverStatus.Status.WAITING, "driver iterations");
                return Operator.NOT_BLOCKED.listener();
            }
            if (now - startTime >= maxTimeNanos) {
                updateStatus(now - lastStatusUpdateTime, iterationsSinceLastStatusUpdate, DriverStatus.Status.WAITING, "driver time");
                return Operator.NOT_BLOCKED.listener();
            }
            if (now > nextStatus) {
                updateStatus(now - lastStatusUpdateTime, iterationsSinceLastStatusUpdate, DriverStatus.Status.RUNNING, "driver running");
                iterationsSinceLastStatusUpdate = 0;
                lastStatusUpdateTime = now;
                nextStatus = now + statusNanos;
            }
        }
    }

    /**
     * Whether the driver has run the chain of operators to completion.
     */
    private boolean isFinished() {
        return activeOperators.isEmpty();
    }

    @Override
    public void close() {
        drainAndCloseOperators(null);
    }

    /**
     * Abort the driver and wait for it to finish
     */
    public void abort(Exception reason, ActionListener<Void> listener) {
        finishNanos = System.nanoTime();
        completionListener.addListener(listener);
        if (started.compareAndSet(false, true)) {
            drainAndCloseOperators(reason);
            completionListener.onFailure(reason);
        } else {
            cancel(reason.getMessage());
        }
    }

    private IsBlockedResult runSingleLoopIteration() {
        driverContext.checkForEarlyTermination();
        boolean movedPage = false;

        for (int i = 0; i < activeOperators.size() - 1; i++) {
            Operator op = activeOperators.get(i);
            Operator nextOp = activeOperators.get(i + 1);

            // skip blocked operator
            if (op.isBlocked().listener().isDone() == false) {
                continue;
            }

            if (op.isFinished() == false && nextOp.needsInput()) {
                driverContext.checkForEarlyTermination();
                Page page = op.getOutput();
                if (page == null) {
                    // No result, just move to the next iteration
                } else if (page.getPositionCount() == 0) {
                    // Empty result, release any memory it holds immediately and move to the next iteration
                    page.releaseBlocks();
                } else {
                    // Non-empty result from the previous operation, move it to the next operation
                    try {
                        driverContext.checkForEarlyTermination();
                    } catch (DriverEarlyTerminationException | TaskCancelledException e) {
                        page.releaseBlocks();
                        throw e;
                    }
                    nextOp.addInput(page);
                    movedPage = true;
                }
            }

            if (op.isFinished()) {
                driverContext.checkForEarlyTermination();
                nextOp.finish();
            }
        }

        closeEarlyFinishedOperators();

        if (movedPage == false) {
            return oneOf(
                activeOperators.stream()
                    .map(Operator::isBlocked)
                    .filter(laf -> laf.listener().isDone() == false)
                    .collect(Collectors.toList())
            );
        }
        return Operator.NOT_BLOCKED;
    }

    private void closeEarlyFinishedOperators() {
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
                    Operator op = itr.next();
                    statusOfCompletedOperators.add(new DriverStatus.OperatorStatus(op.toString(), op.status()));
                    op.close();
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
    }

    public void cancel(String reason) {
        if (cancelReason.compareAndSet(null, reason)) {
            scheduler.runPendingTasks();
        }
    }

    public static void start(
        ThreadContext threadContext,
        Executor executor,
        Driver driver,
        int maxIterations,
        ActionListener<Void> listener
    ) {
        driver.completionListener.addListener(listener);
        if (driver.started.compareAndSet(false, true)) {
            driver.updateStatus(0, 0, DriverStatus.Status.STARTING, "driver starting");
            initializeEarlyTerminationChecker(driver);
            schedule(DEFAULT_TIME_BEFORE_YIELDING, maxIterations, threadContext, executor, driver, driver.completionListener);
        }
    }

    private static void initializeEarlyTerminationChecker(Driver driver) {
        // Register a listener to an exchange sink to handle early completion scenarios:
        // 1. When the query accumulates sufficient data (e.g., reaching the LIMIT).
        // 2. When users abort the query but want to retain the current result.
        // This allows the Driver to finish early without waiting for the scheduled task.
        final AtomicBoolean earlyFinished = new AtomicBoolean();
        driver.driverContext.initializeEarlyTerminationChecker(() -> {
            final String reason = driver.cancelReason.get();
            if (reason != null) {
                throw new TaskCancelledException(reason);
            }
            if (earlyFinished.get()) {
                throw new DriverEarlyTerminationException("Exchange sink is closed");
            }
        });
        final List<Operator> operators = driver.activeOperators;
        if (operators.isEmpty() == false) {
            if (operators.get(operators.size() - 1) instanceof ExchangeSinkOperator sinkOperator) {
                sinkOperator.addCompletionListener(ActionListener.running(() -> {
                    earlyFinished.set(true);
                    driver.scheduler.runPendingTasks();
                }));
            }
        }
    }

    // Drains all active operators and closes them.
    private void drainAndCloseOperators(@Nullable Exception e) {
        Iterator<Operator> itr = activeOperators.iterator();
        while (itr.hasNext()) {
            try {
                Releasables.closeWhileHandlingException(itr.next());
            } catch (Exception x) {
                if (e != null) {
                    e.addSuppressed(x);
                }
            }
            itr.remove();
        }
        driverContext.finish();
        Releasables.closeWhileHandlingException(releasable, driverContext.getSnapshot());
    }

    private static void schedule(
        TimeValue maxTime,
        int maxIterations,
        ThreadContext threadContext,
        Executor executor,
        Driver driver,
        ActionListener<Void> listener
    ) {
        final var task = new AbstractRunnable() {

            @Override
            protected void doRun() {
                SubscribableListener<Void> fut = driver.run(maxTime, maxIterations, System::nanoTime);
                if (driver.isFinished()) {
                    onComplete(listener);
                    return;
                }
                if (fut.isDone()) {
                    schedule(maxTime, maxIterations, threadContext, executor, driver, listener);
                } else {
                    ActionListener<Void> readyListener = ActionListener.wrap(
                        ignored -> schedule(maxTime, maxIterations, threadContext, executor, driver, listener),
                        this::onFailure
                    );
                    fut.addListener(ContextPreservingActionListener.wrapPreservingContext(readyListener, threadContext));
                    driver.scheduler.addOrRunDelayedTask(() -> fut.onResponse(null));
                }
            }

            @Override
            public void onFailure(Exception e) {
                driver.drainAndCloseOperators(e);
                onComplete(ActionListener.running(() -> listener.onFailure(e)));
            }

            void onComplete(ActionListener<Void> listener) {
                driver.driverContext.waitForAsyncActions(ContextPreservingActionListener.wrapPreservingContext(listener, threadContext));
            }
        };
        driver.scheduler.scheduleOrRunTask(executor, task);
    }

    private static IsBlockedResult oneOf(List<IsBlockedResult> results) {
        if (results.isEmpty()) {
            return Operator.NOT_BLOCKED;
        }
        if (results.size() == 1) {
            return results.get(0);
        }
        SubscribableListener<Void> oneOf = new SubscribableListener<>();
        StringBuilder reason = new StringBuilder();
        for (IsBlockedResult r : results) {
            r.listener().addListener(oneOf);
            if (reason.isEmpty() == false) {
                reason.append(" OR ");
            }
            reason.append(r.reason());
        }
        return new IsBlockedResult(oneOf, reason.toString());
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[activeOperators=" + activeOperators + "]";
    }

    @Override
    public String describe() {
        return description.get();
    }

    public String sessionId() {
        return sessionId;
    }

    /**
     * Get the last status update from the driver. These updates are made
     * when the driver is queued and after every
     * processing {@link #run batch}.
     */
    public DriverStatus status() {
        return status.get();
    }

    /**
     * Build a "profile" of this driver's operations after it's been completed.
     * This doesn't make sense to call before the driver is done.
     */
    public DriverProfile profile() {
        DriverStatus status = status();
        if (status.status() != DriverStatus.Status.DONE) {
            throw new IllegalStateException("can only get profile from finished driver");
        }
        return new DriverProfile(
            status.taskDescription(),
            status.started(),
            status.lastUpdated(),
            finishNanos - startNanos,
            status.cpuNanos(),
            status.iterations(),
            status.completedOperators(),
            status.sleeps()
        );
    }

    /**
     * Update the status.
     * @param extraCpuNanos how many cpu nanoseconds to add to the previous status
     * @param extraIterations how many iterations to add to the previous status
     * @param status the status of the overall driver request
     */
    private void updateStatus(long extraCpuNanos, int extraIterations, DriverStatus.Status status, String reason) {
        this.status.getAndUpdate(prev -> {
            long now = System.currentTimeMillis();
            DriverSleeps sleeps = prev.sleeps();

            // Rebuild the sleeps or bail entirely based on the updated status.
            // Sorry for the complexity here. If anyone has a nice way to refactor this, be my guest.
            switch (status) {
                case ASYNC, WAITING -> sleeps = sleeps.sleep(reason, now);
                case RUNNING -> {
                    switch (prev.status()) {
                        case ASYNC, WAITING -> sleeps = sleeps.wake(now);
                        case STARTING -> {
                            if (extraIterations == 0) {
                                /*
                                 * 0 extraIterations means we haven't started the loop - we're just
                                 * signaling that we've woken up. We don't need to signal that when
                                 * the state is already STARTING because we don't have anything
                                 * interesting to report. And some tests rely on the status staying
                                 * in the STARTING state until the first status report.
                                 */
                                return prev;
                            }
                        }
                    }
                }
            }

            return new DriverStatus(
                sessionId,
                taskDescription,
                startTime,
                now,
                prev.cpuNanos() + extraCpuNanos,
                prev.iterations() + extraIterations,
                status,
                statusOfCompletedOperators,
                activeOperators.stream().map(op -> new DriverStatus.OperatorStatus(op.toString(), op.status())).toList(),
                sleeps
            );
        });
    }
}
