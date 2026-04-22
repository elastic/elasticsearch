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
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.util.concurrent.Executor;

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
     * <p><b>Cancellation:</b> No timeout. Cancellation comes from
     * {@code buffer.finish(true)} setting {@code noMoreInputs}, which causes
     * {@code waitForSpace()} to return an already-completed listener.
     */
    public static void drainPagesAsync(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        Executor executor,
        ActionListener<Void> listener
    ) {
        drainBatch(pages, buffer, executor, listener);
    }

    private static void drainBatch(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        Executor executor,
        ActionListener<Void> listener
    ) {
        try {
            while (pages.hasNext() && buffer.noMoreInputs() == false) {
                SubscribableListener<Void> space = buffer.waitForSpace();
                if (space.isDone()) {
                    if (buffer.noMoreInputs()) break;
                    Page page = pages.next();
                    page.allowPassingToDifferentDriver();
                    buffer.addPage(page);
                } else {
                    space.addListener(ActionListener.wrap(v -> {
                        try {
                            executor.execute(() -> drainBatch(pages, buffer, executor, listener));
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }, listener::onFailure));
                    return;
                }
            }
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Drains pages from iterator into buffer asynchronously with a row budget.
     * Same async behavior as {@link #drainPagesAsync}: synchronous hot path, async cold path
     * on buffer-full. Reports the total number of rows drained via the listener.
     *
     * <p><b>Iterator ownership:</b> This method does NOT close the iterator.
     * The caller must close it regardless of outcome (e.g. via
     * {@link ActionListener#runAfter}).
     *
     * <p><b>Executor contract:</b> Same as {@link #drainPagesAsync}.
     *
     * @param rowLimit maximum rows to drain, or {@link FormatReader#NO_LIMIT} for unlimited
     */
    public static void drainPagesWithBudgetAsync(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        int rowLimit,
        Executor executor,
        ActionListener<Integer> listener
    ) {
        drainBatchWithBudget(pages, buffer, rowLimit, new int[] { 0 }, executor, listener);
    }

    private static void drainBatchWithBudget(
        CloseableIterator<Page> pages,
        AsyncExternalSourceBuffer buffer,
        int rowLimit,
        int[] totalRows,
        Executor executor,
        ActionListener<Integer> listener
    ) {
        try {
            while (pages.hasNext() && buffer.noMoreInputs() == false) {
                if (rowLimit != FormatReader.NO_LIMIT && totalRows[0] >= rowLimit) break;
                SubscribableListener<Void> space = buffer.waitForSpace();
                if (space.isDone()) {
                    if (buffer.noMoreInputs()) break;
                    Page page = pages.next();
                    totalRows[0] += page.getPositionCount();
                    page.allowPassingToDifferentDriver();
                    buffer.addPage(page);
                } else {
                    space.addListener(ActionListener.wrap(v -> {
                        try {
                            executor.execute(() -> drainBatchWithBudget(pages, buffer, rowLimit, totalRows, executor, listener));
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }, listener::onFailure));
                    return;
                }
            }
            listener.onResponse(totalRows[0]);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
