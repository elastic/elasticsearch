/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.util.Holder;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
     * The hot loop parks on {@link CloseableIterator#waitForReady()} rather than blocking
     * {@code hasNext()}, so an async iterator can yield the executor slot while I/O is in flight.
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
        drainBatch(pages, buffer, executor, readCancelled, stop, pageSink, listener, new DrainSession());
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
        ActionListener<Void> listener,
        DrainSession session
    ) {
        Object token = new Object();
        if (session.run.compareAndSet(null, token) == false) {
            recordDrainError(session, new IllegalStateException("overlapping drain run"));
            return;
        }
        try {
            StorageRetryCancellation.runWithCancellation(readCancelled, () -> {
                while (buffer.noMoreInputs() == false && stop.getAsBoolean() == false) {
                    Exception overlap = session.error.get();
                    if (overlap != null) {
                        failDrain(session, listener, overlap);
                        return;
                    }
                    SubscribableListener<Void> ready = pages.waitForReady();
                    if (ready.isDone() == false) {
                        park(ready, null, pages, buffer, executor, readCancelled, stop, pageSink, listener, session, token);
                        return;
                    }

                    Holder<SubscribableListener<Void>> blockedOn = new Holder<>();
                    Page page = buffer.readCounters().meteredCpu(() -> {
                        Page tryPage = pages.tryAdvance();
                        if (tryPage == null) {
                            SubscribableListener<Void> recheck = pages.waitForReady();
                            if (recheck.isDone()) {
                                if (pages.hasNext() == false) {
                                    return null;
                                }
                                tryPage = pages.next();
                            } else {
                                blockedOn.set(recheck);
                                return null;
                            }
                        }
                        return tryPage;
                    });
                    if (blockedOn.get() != null) {
                        park(blockedOn.get(), null, pages, buffer, executor, readCancelled, stop, pageSink, listener, session, token);
                        return;
                    }
                    if (page == null) {
                        completeDrain(session, listener);
                        return;
                    }
                    overlap = session.error.get();
                    if (overlap != null) {
                        page.releaseBlocks();
                        failDrain(session, listener, overlap);
                        return;
                    }

                    SubscribableListener<Void> space = buffer.waitForSpace();
                    if (space.isDone() == false) {
                        pages.revokeOvershootOnPark();
                        park(space, page, pages, buffer, executor, readCancelled, stop, pageSink, listener, session, token);
                        return;
                    }
                    if (buffer.noMoreInputs() || stop.getAsBoolean()) {
                        page.releaseBlocks();
                        break;
                    }
                    pageSink.accept(page);
                }
                Exception overlap = session.error.get();
                if (overlap != null) {
                    failDrain(session, listener, overlap);
                    return;
                }
                completeDrain(session, listener);
            });
        } catch (Exception e) {
            failDrain(session, listener, e);
        } finally {
            session.run.compareAndSet(token, null);
        }
    }

    /**
     * Parks the drain until {@code signal} fires, then force-submits one continuation
     * on {@code executor}. {@code heldPage} is a page already pulled from the iterator; it is
     * sunk on resume or released if the drain has stopped.
     */
    private static void park(
        SubscribableListener<Void> signal,
        @Nullable Page heldPage,
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        Executor executor,
        BooleanSupplier readCancelled,
        BooleanSupplier stop,
        Consumer<Page> pageSink,
        ActionListener<Void> listener,
        DrainSession session,
        Object token
    ) {
        // Unlock before addListener so an already-done signal (DIRECT inline completion) can
        // claim the run. This task must not mutate drain state after this point.
        session.run.compareAndSet(token, null);
        signal.addListener(ActionListener.wrap(v -> {
            boolean consumed = heldPage == null;
            try {
                if (heldPage != null) {
                    if (buffer.noMoreInputs() || stop.getAsBoolean()) {
                        consumed = true;
                        heldPage.releaseBlocks();
                        completeDrain(session, listener);
                        return;
                    }
                    consumed = true;
                    pageSink.accept(heldPage);
                }
                submitResume(pages, buffer, executor, readCancelled, stop, pageSink, listener, session);
            } catch (Exception e) {
                if (consumed == false) {
                    heldPage.releaseBlocks();
                }
                failDrain(session, listener, e);
            }
        }, e -> {
            if (heldPage != null) {
                heldPage.releaseBlocks();
            }
            failDrain(session, listener, e);
        }));
    }

    private static void submitResume(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        Executor executor,
        BooleanSupplier readCancelled,
        BooleanSupplier stop,
        Consumer<Page> pageSink,
        ActionListener<Void> listener,
        DrainSession session
    ) {
        AbstractRunnable task = new AbstractRunnable() {
            @Override
            public boolean isForceExecution() {
                return true;
            }

            @Override
            protected void doRun() {
                drainBatch(pages, buffer, executor, readCancelled, stop, pageSink, listener, session);
            }

            @Override
            public void onFailure(Exception e) {
                failDrain(session, listener, e);
            }
        };
        try {
            executor.execute(task);
        } catch (Exception e) {
            failDrain(session, listener, e);
        }
    }

    static void completeDrain(DrainSession session, ActionListener<Void> listener) {
        if (session.completed.compareAndSet(false, true)) {
            listener.onResponse(null);
        }
    }

    static void failDrain(DrainSession session, ActionListener<Void> listener, Exception e) {
        if (session.completed.compareAndSet(false, true)) {
            listener.onFailure(e);
        }
    }

    /** Overlap records here; the token holder calls {@link #failDrain} after it drops the page. */
    static void recordDrainError(DrainSession session, Exception e) {
        session.error.compareAndSet(null, e);
    }

    /**
     * One drain session: exclusive run plus at-most-once completion. No queued/dirty mailbox;
     * each signal is one force-executed submit. {@code run} holds the current drainBatch token.
     * {@code error} is a stashed overlap until the holder notifies.
     */
    static final class DrainSession {
        final AtomicReference<Object> run = new AtomicReference<>();
        final AtomicBoolean completed = new AtomicBoolean();
        final AtomicReference<Exception> error = new AtomicReference<>();
    }

}
