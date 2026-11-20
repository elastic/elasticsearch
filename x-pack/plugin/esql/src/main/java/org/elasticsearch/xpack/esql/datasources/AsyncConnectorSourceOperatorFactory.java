/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Limiter;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Source-operator factory that executes a connector query on a background thread and feeds pages
 * into an {@link AsyncExternalSourceBuffer} for the driver to consume.
 * <p>
 * Two execution modes, selected based on whether a {@link ExternalSliceQueue} is supplied:
 * <ul>
 *   <li><b>Slice-queue mode</b> — iterates each {@link ExternalSplit} pulled from the queue and
 *   executes it via {@link Connector#execute(QueryRequest, ExternalSplit)}. A pushed LIMIT is not
 *   reserved here ({@link QueryRequest} stays {@link FormatReader#NO_LIMIT}); the factory may
 *   observe a downstream {@link Limiter} to stop claiming once that limiter is exhausted.</li>
 *   <li><b>Single-shot mode</b> — executes the request exactly once using {@link Split#SINGLE} via
 *   {@link Connector#execute(QueryRequest, Split)}.</li>
 * </ul>
 * The producer runs as a flat state machine (see {@link #runProducerLoop}) driven by a single
 * {@code ProducerState} instance — no CPS recursion, no per-split trampoline. The drain is fully
 * non-blocking: it runs synchronously while the buffer has space and yields the producer thread
 * when full, resuming via the executor when space is freed.
 *
 * @see AsyncExternalSourceBuffer
 * @see AsyncExternalSourceOperator
 */
public class AsyncConnectorSourceOperatorFactory implements SourceOperator.SourceOperatorFactory {

    private final Connector connector;
    private final QueryRequest baseRequest;
    private final int maxBufferSize;
    private final Executor executor;
    private final ExternalSliceQueue sliceQueue;
    /**
     * Downstream {@link org.elasticsearch.compute.operator.LimitOperator} limiter, installed by the
     * planner when a Filter/Eval/Project chain sits between LIMIT and this source. Read-only stop
     * signal; connectors do not reserve against it.
     */
    @Nullable
    private volatile Limiter observedLimiter;
    private final AtomicBoolean started = new AtomicBoolean();
    /**
     * Live count of in-flight producer tasks plus unclosed source operators created by {@link #get}.
     * Each {@code get()} takes two refs: one released when that instance's producer finishes or
     * fails, and one released from {@link AsyncExternalSourceOperator#close()}. Hitting zero
     * closes the shared connector. Dual refs keep the Flight client alive until every operator
     * built during {@code createDrivers} has closed, even if an earlier producer finishes while
     * later {@code get()} calls are still pending.
     */
    private final AtomicInteger operatorRefCount = new AtomicInteger();
    private final Object filteredLimitRampLock = new Object();
    private int filteredLimitRunning;
    private int filteredLimitCap = AsyncExternalSourceOperatorFactory.FILTERED_LIMIT_READER_RAMP;
    private final ArrayDeque<ParkedProducer> filteredLimitParked = new ArrayDeque<>();

    public AsyncConnectorSourceOperatorFactory(
        Connector connector,
        QueryRequest baseRequest,
        int maxBufferSize,
        Executor executor,
        @Nullable ExternalSliceQueue sliceQueue
    ) {
        if (connector == null) {
            throw new IllegalArgumentException("connector must not be null");
        }
        if (baseRequest == null) {
            throw new IllegalArgumentException("baseRequest must not be null");
        }
        if (executor == null) {
            throw new IllegalArgumentException("executor must not be null");
        }
        if (maxBufferSize <= 0) {
            throw new IllegalArgumentException("maxBufferSize must be positive, got: " + maxBufferSize);
        }
        this.connector = connector;
        this.baseRequest = baseRequest;
        this.maxBufferSize = maxBufferSize;
        this.executor = executor;
        this.sliceQueue = sliceQueue;
    }

    /**
     * Installs the downstream {@link org.elasticsearch.compute.operator.LimitOperator} limiter so
     * this source can stop claiming splits once that limiter is exhausted.
     */
    public void setObservedLimiter(Limiter limiter) {
        if (started.get()) {
            throw new IllegalStateException("observed limiter must be installed before source operators are created");
        }
        this.observedLimiter = limiter;
    }

    @Nullable
    public Limiter observedLimiter() {
        return observedLimiter;
    }

    private boolean noFurtherCandidates() {
        Limiter limiter = observedLimiter;
        return limiter != null && limiter.remaining() == 0;
    }

    public AsyncConnectorSourceOperatorFactory(Connector connector, QueryRequest baseRequest, int maxBufferSize, Executor executor) {
        this(connector, baseRequest, maxBufferSize, executor, null);
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        started.set(true);
        QueryRequest request = baseRequest.withBlockFactory(driverContext.blockFactory());
        long maxBufferBytes = (long) maxBufferSize * Operator.TARGET_PAGE_SIZE;
        Releasable operatorHold = null;
        boolean refsTaken = false;
        boolean producerOwnsHold = false;
        boolean succeeded = false;
        try {
            operatorRefCount.addAndGet(2);
            refsTaken = true;
            operatorHold = Releasables.releaseOnce(() -> driverContext.meteredCpu(() -> {
                releaseOperator();
                return null;
            }));
            AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);
            driverContext.addAsyncAction();

            // Single completion listener: exactly one of {finish(false), onFailure(e)} fires, and
            // driverContext.removeAsyncAction() runs once. Connector.close() waits until the last
            // producer and operator hold are gone, so a sibling still in execute() — or a later
            // get() during createDrivers — cannot see a closed Flight client.
            ActionListener<Void> completionListener = ActionListener.assertOnce(
                ActionListener.runAfter(ActionListener.wrap(v -> buffer.finish(false), buffer::onFailure), () -> {
                    try {
                        releaseOperator();
                    } finally {
                        driverContext.removeAsyncAction();
                    }
                })
            );

            ProducerState state = new ProducerState(request, sliceQueue, buffer, driverContext);
            try {
                submitConnectorProducer(state, completionListener);
            } catch (Exception e) {
                completionListener.onFailure(e);
            }
            producerOwnsHold = true;
            SourceOperator operator = new AsyncExternalSourceOperator(buffer, driverContext, operatorHold);
            succeeded = true;
            return operator;
        } finally {
            if (succeeded == false) {
                if (operatorHold != null) {
                    try {
                        operatorHold.close();
                    } finally {
                        if (producerOwnsHold == false) {
                            releaseOperator();
                        }
                    }
                } else if (refsTaken) {
                    try {
                        releaseOperator();
                    } finally {
                        releaseOperator();
                    }
                }
            }
        }
    }

    private void releaseOperator() {
        if (operatorRefCount.decrementAndGet() == 0) {
            closeConnectorQuietly();
        }
    }

    private void submitConnectorProducer(ProducerState state, ActionListener<Void> finishBuffer) {
        if (filteredLimitRamp() == false) {
            executor.execute(ActionRunnable.wrap(finishBuffer, l -> runProducerLoop(state, l)));
            return;
        }
        ActionListener<Void> started = ActionListener.runAfter(finishBuffer, this::onFilteredLimitProducerFinished);
        boolean startNow;
        ParkedProducer skipped = null;
        synchronized (filteredLimitRampLock) {
            if (noFurtherCandidates() || state.buffer.noMoreInputs()) {
                skipped = new ParkedProducer(state, finishBuffer);
                startNow = false;
            } else if (filteredLimitRunning < filteredLimitCap) {
                filteredLimitRunning++;
                startNow = true;
            } else {
                filteredLimitParked.add(new ParkedProducer(state, finishBuffer));
                startNow = false;
            }
        }
        if (startNow) {
            try {
                executor.execute(ActionRunnable.wrap(started, l -> runProducerLoop(state, l)));
            } catch (Exception e) {
                started.onFailure(e);
            }
            return;
        }
        if (skipped != null) {
            skipped.finishBuffer().onResponse(null);
        }
    }

    private boolean filteredLimitRamp() {
        return observedLimiter != null && sliceQueue != null;
    }

    private void onFilteredLimitProducerFinished() {
        if (filteredLimitRamp() == false) {
            return;
        }
        List<ParkedProducer> toStart = new ArrayList<>();
        List<ParkedProducer> toSkip = new ArrayList<>();
        synchronized (filteredLimitRampLock) {
            filteredLimitRunning--;
            drainParkedLocked(toStart, toSkip);
        }
        dispatchParked(toStart, toSkip);
    }

    private void maybeRampAfterUnit() {
        if (filteredLimitRamp() == false || noFurtherCandidates()) {
            return;
        }
        List<ParkedProducer> toStart = new ArrayList<>();
        List<ParkedProducer> toSkip = new ArrayList<>();
        synchronized (filteredLimitRampLock) {
            if (filteredLimitCap < Integer.MAX_VALUE / 2) {
                filteredLimitCap *= 2;
            } else {
                filteredLimitCap = Integer.MAX_VALUE;
            }
            drainParkedLocked(toStart, toSkip);
        }
        dispatchParked(toStart, toSkip);
    }

    private void drainParkedLocked(List<ParkedProducer> toStart, List<ParkedProducer> toSkip) {
        if (noFurtherCandidates()) {
            while (filteredLimitParked.isEmpty() == false) {
                toSkip.add(filteredLimitParked.poll());
            }
            return;
        }
        while (filteredLimitRunning < filteredLimitCap && filteredLimitParked.isEmpty() == false) {
            ParkedProducer parked = filteredLimitParked.poll();
            if (parked.state().buffer.noMoreInputs() || noFurtherCandidates()) {
                toSkip.add(parked);
                continue;
            }
            filteredLimitRunning++;
            toStart.add(parked);
        }
    }

    private void dispatchParked(List<ParkedProducer> toStart, List<ParkedProducer> toSkip) {
        for (ParkedProducer parked : toSkip) {
            parked.finishBuffer().onResponse(null);
        }
        for (ParkedProducer parked : toStart) {
            ActionListener<Void> started = ActionListener.runAfter(parked.finishBuffer(), this::onFilteredLimitProducerFinished);
            try {
                executor.execute(ActionRunnable.wrap(started, l -> runProducerLoop(parked.state(), l)));
            } catch (Exception e) {
                started.onFailure(e);
            }
        }
    }

    private record ParkedProducer(ProducerState state, ActionListener<Void> finishBuffer) {}

    /**
     * Producer-loop state. One instance per {@code get(DriverContext)} call.
     * <p>
     * Tracks mode (queue vs single-shot), position within the queue, and the currently
     * open {@link ResultCursor}. Mutated only from the producer executor thread.
     */
    private static final class ProducerState {
        final QueryRequest request;
        @Nullable
        final ExternalSliceQueue queue;
        final AsyncExternalSourceBuffer buffer;
        /** Context of the driver that consumes the buffer. The producer's CPU time is metered into it. */
        final DriverContext driverContext;

        /** True iff single-shot mode has already opened its one cursor (subsequent advances return EOF). */
        boolean singleShotStarted;
        /** Currently active cursor, or {@code null} if between units or before the first open. */
        @Nullable
        ResultCursor cursor;
        /** The buffer's space signal when the last drain returned {@link DrainResult#BLOCKED}, else {@code null}. */
        @Nullable
        SubscribableListener<Void> blockedOn;

        ProducerState(
            QueryRequest request,
            @Nullable ExternalSliceQueue queue,
            AsyncExternalSourceBuffer buffer,
            DriverContext driverContext
        ) {
            if (request == null) {
                throw new IllegalArgumentException("ProducerState requires a non-null request");
            }
            if (buffer == null) {
                throw new IllegalArgumentException("ProducerState requires a non-null buffer");
            }
            this.request = request;
            this.queue = queue;
            this.buffer = buffer;
            this.driverContext = driverContext;
        }
    }

    private enum DrainResult {
        /** Hit EOF on the current cursor; caller should advance to the next unit. */
        EOF,
        /** Buffer is full; the space signal is in {@link ProducerState#blockedOn} for the caller to park on. */
        BLOCKED,
        /** Row limit exhausted or buffer finished; the whole producer is done. */
        DONE
    }

    /**
     * Single-step producer loop. Each invocation either drains some pages from the current cursor,
     * opens a new cursor for the next unit, or registers a space callback and returns. The loop
     * self-resubmits on the executor between units to avoid running producer I/O on the Driver
     * thread and to prevent unbounded recursion across many splits.
     */
    private void runProducerLoop(ProducerState state, ActionListener<Void> completionListener) {
        try {
            // DONE finishes the producer on this thread, and removing the driver's async action there counts the open
            // measurement first. EOF and BLOCKED hand off to another thread that can finish the producer, so the
            // measurement has to end before them. See DriverContext#meteredCpu.
            DrainResult result = state.driverContext.meteredCpu(() -> {
                DrainResult produced = produce(state);
                if (produced == DrainResult.DONE) {
                    completionListener.onResponse(null);
                }
                return produced;
            });
            switch (result) {
                case DONE -> {
                    // Completed inside the measurement.
                }
                case EOF -> executor.execute(ActionRunnable.wrap(completionListener, l -> runProducerLoop(state, l)));
                case BLOCKED -> {
                    SubscribableListener<Void> space = state.blockedOn;
                    state.blockedOn = null;
                    parkUntilReady(space, state, completionListener);
                }
            }
        } catch (Exception e) {
            closeCursorQuietly(state.cursor);
            state.cursor = null;
            completionListener.onFailure(e);
        }
    }

    /**
     * Opens the next unit when no cursor is open, then drains the cursor. Closes the cursor unless the drain
     * is {@link DrainResult#BLOCKED}, so no resources leak on cancellation / row-budget exhaustion paths.
     * Returns {@link DrainResult#DONE} when there is no next unit.
     */
    private DrainResult produce(ProducerState state) {
        if (state.cursor == null && advanceToNextUnit(state) == false) {
            return DrainResult.DONE;
        }
        DrainResult result = drainHotPath(state);
        if (result != DrainResult.BLOCKED) {
            closeCursorQuietly(state.cursor);
            state.cursor = null;
        }
        if (result == DrainResult.EOF) {
            maybeRampAfterUnit();
        }
        return result;
    }

    /**
     * Drain pages from the currently-open cursor into the buffer.
     * Runs synchronously while the buffer has space; when full, stores the buffer's space signal in
     * {@link ProducerState#blockedOn} and returns {@link DrainResult#BLOCKED}.
     */
    private DrainResult drainHotPath(ProducerState state) {
        ResultCursor cursor = state.cursor;
        AsyncExternalSourceBuffer buffer = state.buffer;
        while (true) {
            if (noFurtherCandidates()) {
                return DrainResult.DONE;
            }
            if (buffer.noMoreInputs()) {
                return DrainResult.DONE;
            }
            if (cursor.hasNext() == false) {
                return DrainResult.EOF;
            }
            SubscribableListener<Void> space = buffer.waitForSpace();
            if (space.isDone() == false) {
                state.blockedOn = space;
                return DrainResult.BLOCKED;
            }
            if (buffer.noMoreInputs() || noFurtherCandidates()) {
                return DrainResult.DONE;
            }
            Page page = cursor.next();
            if (buffer.noMoreInputs() || noFurtherCandidates()) {
                page.releaseBlocks();
                return DrainResult.DONE;
            }
            page.allowPassingToDifferentDriver();
            buffer.addPage(page);
        }
    }

    /**
     * Register a single listener that resumes the producer loop when {@code signal} fires, and
     * return synchronously. The producer resumes asynchronously when {@code signal} completes.
     * <p>
     * Cleanup semantics by branch:
     * <ul>
     * <li>Success branch (happy path): re-submits {@link #runProducerLoop} on {@code executor};
     *     the current cursor stays open across the park. Only if {@code executor.execute()}
     *     itself throws (e.g. shutting-down pool) is the cursor closed and the failure routed
     *     through {@code completionListener.onFailure}.</li>
     * <li>Failure branch: closes the current cursor and routes the signal's failure through
     *     {@code completionListener.onFailure}.</li>
     * </ul>
     * Both cleanup paths match what the surrounding {@link #runProducerLoop} {@code catch} block
     * does on a synchronous throw.
     *
     * @param signal a not-done listener from {@code waitForSpace()}; callers must verify
     *               {@code signal.isDone() == false} before invoking this helper.
     */
    private void parkUntilReady(SubscribableListener<Void> signal, ProducerState state, ActionListener<Void> completionListener) {
        signal.addListener(ActionListener.wrap(v -> {
            try {
                executor.execute(() -> runProducerLoop(state, completionListener));
            } catch (Exception e) {
                closeCursorQuietly(state.cursor);
                state.cursor = null;
                completionListener.onFailure(e);
            }
        }, e -> {
            closeCursorQuietly(state.cursor);
            state.cursor = null;
            completionListener.onFailure(e);
        }));
    }

    /**
     * Open a cursor for the next unit of work: either the next split pulled from the queue, or
     * the single-shot {@link Split#SINGLE} execution. Returns {@code false} if iteration is
     * exhausted (queue drained, or single-shot already done) or the buffer has been finished
     * externally / the row budget is exhausted.
     */
    private boolean advanceToNextUnit(ProducerState state) {
        if (state.buffer.noMoreInputs()) {
            return false;
        }
        if (noFurtherCandidates()) {
            return false;
        }
        if (state.queue != null) {
            ExternalSplit split = state.queue.nextSplit();
            if (split == null) {
                return false;
            }
            state.cursor = connector.execute(state.request, split);
            return true;
        } else {
            if (state.singleShotStarted) {
                return false;
            }
            state.singleShotStarted = true;
            state.cursor = connector.execute(state.request, Split.SINGLE);
            return true;
        }
    }

    private void closeConnectorQuietly() {
        try {
            connector.close();
        } catch (IOException ignored) {}
    }

    private static void closeCursorQuietly(@Nullable ResultCursor cursor) {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception ignored) {}
        }
    }

    @Override
    public String describe() {
        return "AsyncConnectorSource[" + connector + ", maxBufferBytes=" + ((long) maxBufferSize * Operator.TARGET_PAGE_SIZE) + "]";
    }
}
