/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.PageToBatchPageOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

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
    private static final Logger LOGGER = LogManager.getLogger(Driver.class);

    public static final TimeValue DEFAULT_TIME_BEFORE_YIELDING = TimeValue.timeValueMinutes(5);
    public static final int DEFAULT_MAX_ITERATIONS = 10_000;
    /**
     * Minimum time between updating status.
     */
    public static final TimeValue DEFAULT_STATUS_INTERVAL = TimeValue.timeValueSeconds(1);

    private final String sessionId;

    /**
     * Description of the driver. This description should be short and meaningful as a grouping identifier.
     * We use the phase of the query right now: "data", "node_reduce", "final".
     */
    protected final String shortDescription;

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
    protected List<Operator> activeOperators;
    private final List<OperatorStatus> statusOfCompletedOperators = new ArrayList<>();
    /**
     * Operators that asked to be resnapshotted after {@code waitForAsyncActions}. Index is into
     * {@link #statusOfCompletedOperators}. Mutated only after {@link #isFinished()} when the
     * driver loop is dead.
     */
    private final List<PendingFinalStatus> pendingFinalStatus = new ArrayList<>();
    private final Releasable releasable;
    private final long statusNanos;

    private final AtomicReference<String> cancelReason = new AtomicReference<>();
    private final AtomicBoolean started = new AtomicBoolean();
    /**
     * Flips to {@code true} when the driver should stop pulling new pages and wind down cleanly,
     * surfacing whatever it has already produced. The driver loop polls this flag through
     * {@link DriverContext#checkForEarlyTermination()} and throws {@link DriverEarlyTerminationException},
     * which the loop treats as clean completion. Set by the existing exchange-sink-closed path
     * (LIMIT / coordinator-driven STOP for distributed plans) and by {@link #finishEarly()} for plans
     * with no exchange-sink path (e.g. coordinator-only EXTERNAL reads).
     */
    private final AtomicBoolean earlyFinished = new AtomicBoolean();
    private final SubscribableListener<Void> completionListener = new SubscribableListener<>();
    private final DriverScheduler scheduler = new DriverScheduler();
    /** Reusable list to collect blocked results, avoiding new allocation on every driver loop. */
    private final List<IsBlockedResult> blockedResults = new ArrayList<>();

    /**
     * Status reported to the tasks API. We write the status at most once every
     * {@link #statusNanos}, as soon as loop has finished and after {@link #statusNanos}
     * have passed.
     */
    private final AtomicReference<DriverStatus> status;

    /**
     * Plan-node ids for operators created from a physical plan. Identity map, touched on the driver thread.
     * Empty when the driver was not built from a plan (tests, enrich lookup).
     */
    private Map<Operator, String> planNodeIds = Map.of();

    /**
     * Associates operators with the physical plan node that created them.
     * The map must use identity equality. Operators missing from the map omit {@code plan_node_id}.
     */
    public void setPlanNodeIds(Map<Operator, String> planNodeIds) {
        this.planNodeIds = planNodeIds == null ? Map.of() : planNodeIds;
    }

    private OperatorStatus operatorStatus(Operator op) {
        String planNodeId = planNodeIds.get(op);
        return planNodeId == null
            ? new OperatorStatus(op.toString(), op.status())
            : new OperatorStatus(op.toString(), op.status(), planNodeId);
    }

    /**
     * The time this driver finished. Only set once the driver is finished, defaults to 0
     * which is *possibly* a valid value, so always use the driver status to check
     * if the driver is actually finished.
     */
    private long finishNanos;

    /**
     * Creates a new driver with a chain of operators.
     * @param sessionId session Id
     * @param shortDescription Description of the driver. This description should be short and meaningful as a grouping identifier.
     *                         We use the phase of the query right now: "data", "node_reduce", "final".
     * @param driverContext the driver context
     * @param source source operator
     * @param intermediateOperators  the chain of operators to execute
     * @param sink sink operator
     * @param statusInterval minimum status reporting interval
     * @param releasable a {@link Releasable} to invoked once the chain of operators has run to completion
     */
    public Driver(
        String sessionId,
        String shortDescription,
        String clusterName,
        String nodeName,
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
        this.shortDescription = shortDescription;
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
                shortDescription,
                clusterName,
                nodeName,
                "",
                startTime,
                System.currentTimeMillis(),
                0,
                0,
                0,
                DriverStatus.Status.QUEUED,
                List.of(),
                List.of(),
                DriverSleeps.empty()
            )
        );
    }

    public DriverContext driverContext() {
        return driverContext;
    }

    /**
     * Runs computations on the chain of operators for a given maximum amount of time or iterations.
     * Returns a blocked future when the chain of operators is blocked, allowing the caller
     * thread to do other work instead of blocking or busy-spinning on the blocked operator.
     * The thread CPU time it uses is measured by {@link DriverContext#meteredCpu}, so it is counted once even when
     * this runs inside an operator's measurement of this driver. That happens once the driver was cancelled or finished
     * early: the scheduler then runs it directly on the thread that completes the listener it is blocked on, such as a
     * worker's.
     * CPU and driver time are published at status updates. If an iteration throws, including cancellation,
     * time accumulated since the last status update is not included in the reported totals.
     */
    SubscribableListener<Void> run(TimeValue maxTime, int maxIterations, LongSupplier currentTimeNanosSupplier) {
        return driverContext.meteredCpu(() -> runLoop(maxTime, maxIterations, currentTimeNanosSupplier));
    }

    private SubscribableListener<Void> runLoop(TimeValue maxTime, int maxIterations, LongSupplier currentTimeNanosSupplier) {
        long maxTimeNanos = maxTime.nanos();
        // Start time, used to stop the calculations after maxTime has passed.
        long startTime = currentTimeNanosSupplier.getAsLong();
        updateStatus(0, 0, DriverStatus.Status.RUNNING, "driver running", startTime);
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
                isBlocked = runSingleLoopIteration(currentTimeNanosSupplier, lastStatusUpdateTime);
            } catch (DriverEarlyTerminationException unused) {
                closeEarlyFinishedOperators(
                    activeOperators.listIterator(activeOperators.size()),
                    currentTimeNanosSupplier,
                    lastStatusUpdateTime
                );
                assert isFinished() : "not finished after early termination";
            } catch (TaskCancelledException e) {
                LOGGER.debug("Cancelling running driver [{}]", shortDescription, e);
                throw e;
            } catch (RuntimeException e) {
                RestStatus status = ExceptionsHelper.status(e);
                // HTTP 4xx status codes (400-499) indicate client/user errors: malformed requests, authentication
                // failures, cancellations, etc. These are expected user actions, not system failures, so log at
                // DEBUG level to reduce noise. HTTP 5xx status codes (500+) indicate internal server failures and
                // potential bugs, so log at ERROR.
                if (status.getStatus() >= 400 && status.getStatus() < 500) {
                    LOGGER.debug(Strings.format("User error running driver [%s]", shortDescription), e);
                } else {
                    LOGGER.error(Strings.format("Error running driver [%s]", shortDescription), e);
                }
                throw e;
            } finally {
                assert driverContext.assertEndRunLoop();
            }
            totalIterationsThisRun++;
            iterationsSinceLastStatusUpdate++;

            long now = currentTimeNanosSupplier.getAsLong();
            if (isBlocked.listener().isDone() == false) {
                updateStatus(
                    now - lastStatusUpdateTime,
                    iterationsSinceLastStatusUpdate,
                    DriverStatus.Status.ASYNC,
                    isBlocked.reason(),
                    now
                );
                return isBlocked.listener();
            }
            if (isFinished()) {
                finishNanos = now;
                if (pendingFinalStatus.isEmpty()) {
                    updateStatus(
                        finishNanos - lastStatusUpdateTime,
                        iterationsSinceLastStatusUpdate,
                        DriverStatus.Status.DONE,
                        "driver done",
                        now
                    );
                } else {
                    // Delay DONE until waitForAsyncActions so a later resnapshot of kept operators
                    // is the status the profile sees. Final-loop CPU and driver wall time are recorded here.
                    updateStatus(
                        finishNanos - lastStatusUpdateTime,
                        iterationsSinceLastStatusUpdate,
                        DriverStatus.Status.RUNNING,
                        "driver finishing",
                        now
                    );
                }
                driverContext.finish();
                Releasables.close(releasable, driverContext.getSnapshot());
                return Operator.NOT_BLOCKED.listener();
            }
            if (totalIterationsThisRun >= maxIterations) {
                updateStatus(
                    now - lastStatusUpdateTime,
                    iterationsSinceLastStatusUpdate,
                    DriverStatus.Status.WAITING,
                    "driver iterations",
                    now
                );
                return Operator.NOT_BLOCKED.listener();
            }
            if (now - startTime >= maxTimeNanos) {
                updateStatus(now - lastStatusUpdateTime, iterationsSinceLastStatusUpdate, DriverStatus.Status.WAITING, "driver time", now);
                return Operator.NOT_BLOCKED.listener();
            }
            if (now > nextStatus) {
                updateStatus(
                    now - lastStatusUpdateTime,
                    iterationsSinceLastStatusUpdate,
                    DriverStatus.Status.RUNNING,
                    "driver running",
                    now
                );
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

    private IsBlockedResult runSingleLoopIteration(LongSupplier currentTimeNanosSupplier, long lastStatusUpdate) {
        driverContext.checkForEarlyTermination();
        boolean movedPage = false;

        ListIterator<Operator> iterator = activeOperators.listIterator();
        while (iterator.hasNext()) {
            Operator op = iterator.next();
            if (iterator.hasNext() == false) {
                break;
            }
            Operator nextOp = activeOperators.get(iterator.nextIndex());

            // skip blocked operator
            if (op.isBlocked().listener().isDone() == false) {
                continue;
            }

            if (op.isFinished() == false && nextOp.needsInput()) {
                driverContext.checkForEarlyTermination();
                assert nextOp.isFinished() == false
                    || nextOp instanceof ExchangeSinkOperator
                    || nextOp instanceof LimitOperator
                    || nextOp instanceof PageToBatchPageOperator
                    || nextOp instanceof StreamingPageOperator : "next operator should not be finished yet: " + nextOp;
                Page page = op.getOutput();
                if (page == null) {
                    // No result, just move to the next iteration
                } else if (page.getPositionCount() == 0 && page.batchMetadata() == null) {
                    // Empty result (not a batch marker), release any memory it holds immediately and move to the next iteration
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
                    Operator promoted = nextOp.tryPromote(driverContext);
                    if (promoted != nextOp) {
                        String planNodeId = planNodeIds.get(nextOp);
                        if (planNodeId != null) {
                            planNodeIds.put(promoted, planNodeId);
                        }
                        activeOperators.set(iterator.nextIndex(), promoted);
                    }
                }
            }

            if (op.isFinished()) {
                driverContext.checkForEarlyTermination();
                var originalIndex = iterator.previousIndex();
                var index = closeEarlyFinishedOperators(iterator, currentTimeNanosSupplier, lastStatusUpdate);
                if (index >= 0) {
                    iterator = new ArrayList<>(activeOperators).listIterator(originalIndex - index);
                }
            }
        }

        closeEarlyFinishedOperators(activeOperators.listIterator(activeOperators.size()), currentTimeNanosSupplier, lastStatusUpdate);

        if (movedPage == false) {
            blockedResults.clear();
            activeOperators.stream().map(Operator::isBlocked).filter(laf -> laf.listener().isDone() == false).forEach(blockedResults::add);
            IsBlockedResult result = oneOf(blockedResults);
            onNoPagesMoved();
            return result;
        }
        return Operator.NOT_BLOCKED;
    }

    /**
     * Called when no pages were moved in a loop iteration.
     * Subclasses can override this to detect when the driver is idle/empty.
     */
    protected void onNoPagesMoved() {
        // Default implementation does nothing
    }

    // Returns the index of the last operator that was closed, -1 if no operator was closed.
    protected int closeEarlyFinishedOperators(
        ListIterator<Operator> operators,
        LongSupplier currentTimeNanosSupplier,
        long lastStatusUpdate
    ) {
        var iterator = activeOperators.listIterator(operators.nextIndex());
        while (iterator.hasPrevious()) {
            if (iterator.previous().isFinished()) {
                var index = iterator.nextIndex();
                /*
                 * Close and remove this operator and all source operators in the
                 * most paranoid possible way. Closing operators shouldn't throw,
                 * but if it does, this will make sure we don't try to close any
                 * that succeed twice.
                 */
                Iterator<Operator> finishedOperators = this.activeOperators.subList(0, index + 1).iterator();
                while (finishedOperators.hasNext()) {
                    Operator op = finishedOperators.next();
                    int statusIndex = statusOfCompletedOperators.size();
                    statusOfCompletedOperators.add(operatorStatus(op));
                    if (op instanceof SourceOperator sourceOperator) {
                        long now = currentTimeNanosSupplier.getAsLong();
                        // report one last time before closing
                        sourceOperator.reportSearchLoad(now - lastStatusUpdate, now);
                    }
                    op.close();
                    if (op.finalStatusAfterAsyncActions()) {
                        pendingFinalStatus.add(new PendingFinalStatus(statusIndex, op));
                    }
                    finishedOperators.remove();
                }

                // Finish the next operator, which is now the first operator.
                if (activeOperators.isEmpty() == false) {
                    Operator newRootOperator = activeOperators.get(0);
                    newRootOperator.finish();
                }
                return index;
            }
        }
        return -1;
    }

    /**
     * Finish all active operators. This is used before throwing DriverEarlyTerminationException
     * to ensure all operators are properly finished and can be closed.
     */
    protected void finishAllActiveOperators() {
        for (Operator op : activeOperators) {
            op.finish();
        }
    }

    public void cancel(String reason) {
        if (cancelReason.compareAndSet(null, reason)) {
            scheduler.runPendingTasks();
        }
    }

    // TODO: Remove this
    public static void start(
        ThreadContext threadContext,
        Executor executor,
        Driver driver,
        int maxIterations,
        ActionListener<Void> listener
    ) {
        start(threadContext, new ComputeExecutor(executor, threadContext), driver, maxIterations, listener);
    }

    public static void start(
        ThreadContext threadContext,
        ComputeExecutor executor,
        Driver driver,
        int maxIterations,
        ActionListener<Void> listener
    ) {
        driver.completionListener.addListener(listener);
        if (driver.started.compareAndSet(false, true)) {
            LongSupplier currentTimeNanosSupplier = System::nanoTime;
            driver.updateStatus(0, 0, DriverStatus.Status.STARTING, "driver starting", currentTimeNanosSupplier.getAsLong());
            initializeEarlyTerminationChecker(driver);
            schedule(
                DEFAULT_TIME_BEFORE_YIELDING,
                maxIterations,
                threadContext,
                executor,
                driver,
                driver.completionListener,
                currentTimeNanosSupplier,
                false
            );
        }
    }

    private static void initializeEarlyTerminationChecker(Driver driver) {
        // Register a listener to an exchange sink to handle early completion scenarios:
        // 1. When the query accumulates sufficient data (e.g., reaching the LIMIT).
        // 2. When users abort the query but want to retain the current result.
        // This allows the Driver to finish early without waiting for the scheduled task.
        driver.driverContext.initializeEarlyTerminationChecker(() -> {
            final String reason = driver.cancelReason.get();
            if (reason != null) {
                throw new TaskCancelledException(reason);
            }
            if (driver.earlyFinished.get()) {
                throw new DriverEarlyTerminationException("Exchange sink is closed");
            }
        });
        if (driver.activeOperators.isEmpty() == false) {
            if (driver.activeOperators.getLast() instanceof ExchangeSinkOperator sinkOperator) {
                sinkOperator.addCompletionListener(ActionListener.running(driver::finishEarly));
            } else if (driver.activeOperators.getLast() instanceof StreamingPageOperator streamingOperator) {
                streamingOperator.addCompletionListener(ActionListener.running(driver::finishEarly));
            }
        }
    }

    /**
     * Requests that this driver wind down at its next iteration by treating the operator chain as if
     * its sink had closed. Existing callers wire this to {@link ExchangeSinkOperator} completion, so
     * we keep its semantics narrow — the early-termination checker throws
     * {@link DriverEarlyTerminationException} and operator teardown discards anything that has not
     * already crossed the sink. Source operators that want STOP to drain in-flight pages rather than
     * drop them should register a {@link #runStopHooks() stop hook} instead.
     */
    public boolean finishEarly() {
        if (earlyFinished.compareAndSet(false, true)) {
            scheduler.runPendingTasks();
            return true;
        }
        return false;
    }

    /**
     * Fires any non-destructive stop hooks operators registered on this driver's {@link DriverContext}.
     * Operators use this to interrupt their <em>input side</em> (e.g. close a buffer's producer) while
     * leaving already-buffered pages reachable to the driver loop, so the response contains every row
     * the source had already produced when STOP arrived. Returns {@code true} when at least one hook
     * reported it cut a still-running unit of work, which the async stop action uses as an honest
     * signal to flag {@code is_partial=true}.
     */
    public boolean runStopHooks() {
        return driverContext.runStopHooks();
    }

    protected void drainAndCloseOperators(@Nullable Exception e) {
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
        ComputeExecutor executor,
        Driver driver,
        ActionListener<Void> listener,
        LongSupplier currentTimeNanosSupplier,
        boolean resumed
    ) {
        AbstractRunnable task = new AbstractRunnable() {
            @Override
            protected void doRun() {
                SubscribableListener<Void> fut = driver.run(maxTime, maxIterations, currentTimeNanosSupplier);
                if (driver.isFinished()) {
                    onComplete(listener);
                    return;
                }
                if (fut.isDone()) {
                    schedule(maxTime, maxIterations, threadContext, executor, driver, listener, currentTimeNanosSupplier, false);
                } else {
                    ActionListener<Void> readyListener = ActionListener.wrap(
                        ignored -> schedule(
                            maxTime,
                            maxIterations,
                            threadContext,
                            executor,
                            driver,
                            listener,
                            currentTimeNanosSupplier,
                            true
                        ),
                        this::onFailure
                    );
                    fut.addListener(ContextPreservingActionListener.wrapPreservingContext(readyListener, threadContext));
                    driver.scheduler.addOrRunDelayedTask(() -> fut.onResponse(null));
                }
            }

            @Override
            public void onFailure(Exception e) {
                driver.drainAndCloseOperators(e);
                driver.driverContext.waitForAsyncActions(
                    ContextPreservingActionListener.wrapPreservingContext(
                        ActionListener.running(() -> listener.onFailure(e)),
                        threadContext
                    )
                );
            }

            void onComplete(ActionListener<Void> listener) {
                driver.driverContext.waitForAsyncActions(
                    ContextPreservingActionListener.wrapPreservingContext(ActionListener.wrap(ignored -> {
                        driver.publishFinalStatus();
                        listener.onResponse(null);
                    }, listener::onFailure), threadContext)
                );
            }
        };
        task = (AbstractRunnable) threadContext.preserveContext(task); // Preserve warnings and such
        driver.scheduler.scheduleOrRunTask(executor, task, resumed);
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
            status.description(),
            status.clusterName(),
            status.nodeName(),
            status.started(),
            status.lastUpdated(),
            finishNanos - startNanos,
            status.cpuNanos(),
            status.driverNanos(),
            status.iterations(),
            status.completedOperators(),
            status.sleeps()
        );
    }

    private void reportSearchLoad(long extraDriverNanos, long now) {
        activeOperators.stream()
            .filter(o -> o instanceof SourceOperator)
            .map(o -> (SourceOperator) o)
            .findFirst()
            .ifPresent(sourceOperator -> sourceOperator.reportSearchLoad(extraDriverNanos, now));
    }

    /**
     * Replaces kept operator statuses after async close and publishes {@code DONE}. Always publishes, even when the
     * driver loop already published {@code DONE}, because the rest of that loop and operators on other threads add CPU
     * time after its last status update.
     * May run on a producer thread; the driver loop is already dead.
     */
    private void publishFinalStatus() {
        for (PendingFinalStatus pending : pendingFinalStatus) {
            Operator op = pending.operator;
            statusOfCompletedOperators.set(pending.index, operatorStatus(op));
        }
        pendingFinalStatus.clear();
        updateStatus(0, 0, DriverStatus.Status.DONE, "driver done", System.nanoTime());
    }

    private record PendingFinalStatus(int index, Operator operator) {}

    /**
     * Update the status. The CPU time is the driver context's {@link DriverContext#cpuNanos() total}, after adding
     * what the calling thread's open measurement of this driver has used so far.
     * @param extraDriverNanos how many nanoseconds the driver held a thread since the previous status
     * @param extraIterations how many iterations to add to the previous status
     * @param status the status of the overall driver request
     */
    private void updateStatus(long extraDriverNanos, int extraIterations, DriverStatus.Status status, String reason, long nowNanos) {
        driverContext.cpuTracker().checkpoint();
        long cpuNanos = driverContext.cpuNanos();
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
            // Search load stays on the time the driver held a thread, it's an autoscaling signal rather than billing.
            reportSearchLoad(extraDriverNanos, nowNanos);

            return new DriverStatus(
                sessionId,
                shortDescription,
                prev.clusterName(),
                prev.nodeName(),
                status == DriverStatus.Status.RUNNING ? Thread.currentThread().getName() : prev.threadName(),
                startTime,
                now,
                cpuNanos,
                prev.driverNanos() + extraDriverNanos,
                prev.iterations() + extraIterations,
                status,
                List.copyOf(statusOfCompletedOperators),
                activeOperators.stream().map(this::operatorStatus).toList(),
                sleeps
            );
        });
    }
}
