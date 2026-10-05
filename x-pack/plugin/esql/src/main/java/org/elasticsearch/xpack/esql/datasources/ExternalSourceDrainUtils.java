/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;

import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Utility for draining pages from a {@link CloseableIterator} into an {@link AsyncExternalSourceBuffer}
 * with non-blocking backpressure.
 *
 * <p>Runs synchronously while the buffer has space (hot path), yields the thread when the buffer is
 * full, and resumes via the provided {@link Executor} when space is freed (cold path). No timeout is
 * needed — cancellation propagates via {@link AsyncExternalSourceBuffer#finish(boolean)} setting
 * {@code noMoreInputs}, which causes {@link AsyncExternalSourceBuffer#waitForSpace()} to return an
 * already-completed listener so the drain loop exits promptly.
 */
public final class ExternalSourceDrainUtils {

    private ExternalSourceDrainUtils() {}

    /**
     * Drains pages from iterator into buffer asynchronously.
     * Runs synchronously while the buffer has space; yields the thread
     * when the buffer is full and resumes via {@code executor} when space is freed.
     * Completion (success or failure) is reported via the listener.
     *
     * <p><b>Iterator ownership:</b> This method does NOT close the iterator.
     * The caller must close it regardless of outcome (e.g. via
     * {@link ActionListener#runAfter}).
     *
     * <p><b>Executor contract:</b> The {@code executor} must be a real thread-pool
     * executor (e.g. {@code generic}), never {@code DIRECT_EXECUTOR_SERVICE}.
     * Continuations resume on this executor to avoid running producer I/O
     * on the Driver thread. The executor captures and restores thread context
     * at submission time, so no explicit context-preserving wrapper is needed.
     *
     * <p><b>Cancellation:</b> No timeout. Cooperative cancellation comes from
     * {@code buffer.finish(true)} setting {@code noMoreInputs}, which causes
     * {@code waitForSpace()} to return an already-completed listener and stops the loop at the next page
     * boundary. To also abort a page pull ({@code hasNext()}/{@code next()}) parked in storage retry/throttle
     * backoff, {@code readCancelled} is installed as the ambient {@link StorageRetryCancellation} signal around
     * every drain step (initial and executor-resumed) — mirroring the {@code runProducerLoop} read path.
     */
    public static void drainPagesAsync(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        Executor executor,
        BooleanSupplier readCancelled,
        ActionListener<Void> listener
    ) {
        drainPagesAsync(pages, buffer, executor, readCancelled, () -> false, defaultPageSink(buffer), listener);
    }

    /**
     * Overload without an ambient cancellation signal, preserving the pre-existing behaviour for callers
     * (currently tests) that do not thread a hard-cancel signal into the drain. Equivalent to passing a
     * supplier that never reports cancelled: cooperative {@code noMoreInputs} cancellation still applies.
     */
    public static void drainPagesAsync(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        Executor executor,
        ActionListener<Void> listener
    ) {
        drainPagesAsync(pages, buffer, executor, () -> false, listener);
    }

    /**
     * Like {@link #drainPagesAsync(CloseableIterator, AsyncExternalSourceBuffer, Executor, BooleanSupplier, ActionListener)}
     * but stops pulling when {@code stop} is true and delivers each page through {@code pageSink}
     * (typically the factory {@code deliverPage} that charges a pushed limiter). {@code pageSink}
     * takes ownership of the page; a page pulled after {@code stop} or {@code noMoreInputs} is
     * released rather than sunk.
     */
    public static void drainPagesAsync(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        Executor executor,
        BooleanSupplier readCancelled,
        BooleanSupplier stop,
        Consumer<Page> pageSink,
        ActionListener<Void> listener
    ) {
        drainBatch(pages, buffer, executor, readCancelled, stop, pageSink, listener);
    }

    private static Consumer<Page> defaultPageSink(AsyncExternalSourceBuffer buffer) {
        return page -> {
            page.allowPassingToDifferentDriver();
            buffer.addPage(page);
        };
    }

    private static void drainBatch(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        Executor executor,
        BooleanSupplier readCancelled,
        BooleanSupplier stop,
        Consumer<Page> pageSink,
        ActionListener<Void> listener
    ) {
        try {
            StorageRetryCancellation.runWithCancellation(readCancelled, () -> {
                while (buffer.noMoreInputs() == false && stop.getAsBoolean() == false && buffer.readCounters().meteredCpu(pages::hasNext)) {
                    SubscribableListener<Void> space = buffer.waitForSpace();
                    if (space.isDone()) {
                        if (buffer.noMoreInputs() || stop.getAsBoolean()) {
                            break;
                        }
                        Page page = buffer.readCounters().meteredCpu(pages::next);
                        if (buffer.noMoreInputs() || stop.getAsBoolean()) {
                            page.releaseBlocks();
                            break;
                        }
                        pageSink.accept(page);
                    } else {
                        space.addListener(ActionListener.wrap(v -> {
                            try {
                                executor.execute(() -> drainBatch(pages, buffer, executor, readCancelled, stop, pageSink, listener));
                            } catch (Exception e) {
                                listener.onFailure(e);
                            }
                        }, listener::onFailure));
                        return;
                    }
                }
                listener.onResponse(null);
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
