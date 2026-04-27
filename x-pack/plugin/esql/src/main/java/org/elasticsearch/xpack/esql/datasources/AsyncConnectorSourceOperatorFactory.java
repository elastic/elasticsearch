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
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * Source-operator factory that executes a connector query on a background thread and feeds pages
 * into an {@link AsyncExternalSourceBuffer} for the driver to consume.
 * <p>
 * Two execution modes, selected based on whether a {@link ExternalSliceQueue} is supplied:
 * <ul>
 *   <li><b>Slice-queue mode</b> — iterates each {@link ExternalSplit} pulled from the queue and
 *   executes it via {@link Connector#execute(QueryRequest, ExternalSplit)}, sharing a single row
 *   budget across splits.</li>
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
    private final int rowLimit;
    private final Executor executor;
    private final ExternalSliceQueue sliceQueue;

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
        this.rowLimit = baseRequest.rowLimit();
        this.executor = executor;
        this.sliceQueue = sliceQueue;
    }

    public AsyncConnectorSourceOperatorFactory(Connector connector, QueryRequest baseRequest, int maxBufferSize, Executor executor) {
        this(connector, baseRequest, maxBufferSize, executor, null);
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        QueryRequest request = baseRequest.withBlockFactory(driverContext.blockFactory());
        long maxBufferBytes = (long) maxBufferSize * Operator.TARGET_PAGE_SIZE;
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferBytes);
        driverContext.addAsyncAction();

        // Single completion listener: exactly one of {finish(false), onFailure(e)} fires, and
        // driverContext.removeAsyncAction() plus connector.close() run exactly once via runAfter.
        ActionListener<Void> completionListener = ActionListener.assertOnce(
            ActionListener.runAfter(ActionListener.wrap(v -> buffer.finish(false), buffer::onFailure), () -> {
                closeConnectorQuietly();
                driverContext.removeAsyncAction();
            })
        );

        ProducerState state = new ProducerState(request, sliceQueue, buffer, rowLimit);
        try {
            executor.execute(ActionRunnable.wrap(completionListener, l -> runProducerLoop(state, l)));
        } catch (Exception e) {
            completionListener.onFailure(e);
        }
        return new AsyncExternalSourceOperator(buffer);
    }

    /**
     * Producer-loop state. One instance per {@code get(DriverContext)} call.
     * <p>
     * Tracks mode (queue vs single-shot), position within the queue, row budget, and the currently
     * open {@link ResultCursor}. Mutated only from the producer executor thread.
     */
    private static final class ProducerState {
        final QueryRequest request;
        @Nullable
        final ExternalSliceQueue queue;
        final AsyncExternalSourceBuffer buffer;

        /** True iff single-shot mode has already opened its one cursor (subsequent advances return EOF). */
        boolean singleShotStarted;
        /** Remaining row budget shared across all cursors for this producer. */
        int rowsRemaining;
        /** Currently active cursor, or {@code null} if between units or before the first open. */
        @Nullable
        ResultCursor cursor;

        ProducerState(QueryRequest request, @Nullable ExternalSliceQueue queue, AsyncExternalSourceBuffer buffer, int rowsRemaining) {
            if (request == null) {
                throw new IllegalArgumentException("ProducerState requires a non-null request");
            }
            if (buffer == null) {
                throw new IllegalArgumentException("ProducerState requires a non-null buffer");
            }
            this.request = request;
            this.queue = queue;
            this.buffer = buffer;
            this.rowsRemaining = rowsRemaining;
        }
    }

    private enum DrainResult {
        /** Hit EOF on the current cursor; caller should advance to the next unit. */
        EOF,
        /** Buffer is full; a callback is registered to resume the loop. */
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
            if (state.cursor == null) {
                if (advanceToNextUnit(state) == false) {
                    completionListener.onResponse(null);
                    return;
                }
            }
            DrainResult result = drainHotPath(state, completionListener);
            switch (result) {
                case DONE -> {
                    // Close the active cursor before reporting completion so no resources leak on
                    // cancellation / row-budget exhaustion paths.
                    closeCursorQuietly(state.cursor);
                    state.cursor = null;
                    completionListener.onResponse(null);
                }
                case EOF -> {
                    closeCursorQuietly(state.cursor);
                    state.cursor = null;
                    executor.execute(ActionRunnable.wrap(completionListener, l -> runProducerLoop(state, l)));
                }
                case BLOCKED -> {
                    // A waitForSpace listener has been registered that will re-submit runProducerLoop.
                }
            }
        } catch (Exception e) {
            closeCursorQuietly(state.cursor);
            state.cursor = null;
            completionListener.onFailure(e);
        }
    }

    /**
     * Drain pages from the currently-open cursor into the buffer.
     * Runs synchronously while the buffer has space; when full, registers a callback that
     * re-submits {@link #runProducerLoop} via the executor and returns {@link DrainResult#BLOCKED}.
     */
    private DrainResult drainHotPath(ProducerState state, ActionListener<Void> completionListener) {
        ResultCursor cursor = state.cursor;
        AsyncExternalSourceBuffer buffer = state.buffer;
        while (true) {
            if (buffer.noMoreInputs()) {
                return DrainResult.DONE;
            }
            if (rowLimit != FormatReader.NO_LIMIT && state.rowsRemaining <= 0) {
                return DrainResult.DONE;
            }
            if (cursor.hasNext() == false) {
                return DrainResult.EOF;
            }
            SubscribableListener<Void> space = buffer.waitForSpace();
            if (space.isDone() == false) {
                space.addListener(ActionListener.wrap(v -> {
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
                return DrainResult.BLOCKED;
            }
            if (buffer.noMoreInputs()) {
                return DrainResult.DONE;
            }
            Page page = cursor.next();
            int rows = page.getPositionCount();
            page.allowPassingToDifferentDriver();
            buffer.addPage(page);
            if (rowLimit != FormatReader.NO_LIMIT) {
                state.rowsRemaining -= rows;
            }
        }
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
        if (rowLimit != FormatReader.NO_LIMIT && state.rowsRemaining <= 0) {
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
