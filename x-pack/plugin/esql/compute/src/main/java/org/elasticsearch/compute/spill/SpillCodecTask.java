/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Serializes one page into chunks on the codec pool and appends them to a {@link ChunkSink}, one append outstanding.
 * <p>
 * The task holds its own reference to the page, a shallow copy, so the operator keeps its page whole and unmodified
 * while the task runs and releases its own reference when the task completes; what the breaker credits back then is
 * exactly what the blocks' last references free. The task's chunk buffers are charged to {@code breaker} when it is
 * created, so a task that started never allocates under pressure, and its serialized bytes in flight never exceed the
 * buffer depth times the chunk size: a withheld completion stalls the codec thread at a chunk boundary rather than growing
 * memory. The codec thread is the only thread that ever waits; the caller gets a listener. Cancellation wakes a waiting
 * task, which releases everything it holds and fails its listener.
 */
public final class SpillCodecTask extends AbstractRunnable {

    private final Page page;
    private final TransportVersion version;
    private final ChunkBuffers buffers;
    private final ChunkingOutputStream out;
    @Nullable
    private final Releasable permit;
    private final ActionListener<Long> listener;
    private final AtomicBoolean completed = new AtomicBoolean();
    private volatile boolean cancelled;

    private SpillCodecTask(
        Page page,
        TransportVersion version,
        ChunkSink sink,
        ChunkBuffers buffers,
        @Nullable Releasable permit,
        ActionListener<Long> listener
    ) {
        this.page = page;
        this.version = version;
        this.buffers = buffers;
        this.out = new ChunkingOutputStream(buffers, sink);
        this.permit = permit;
        this.listener = listener;
    }

    /**
     * Charges the buffers, takes the task's reference to the page and submits it. A rejection by the executor or a
     * failure to charge the buffers reaches the listener; nothing is retained in either case.
     *
     * @param page the page to serialize; the caller keeps its own reference and releases it when it likes. The page's
     *             blocks are made releasable from another thread, as the exchange sink does for the pages it ships.
     * @param breaker where the chunk buffers are charged: the query's own breaker, never a driver's local one, because the
     *                task completes on another thread and possibly after the driver has closed
     * @param permit the detachment permit this task holds, released when the task completes, or null
     * @param listener completed with the serialized length, on the codec thread or inline
     */
    public static SpillCodecTask submit(
        Executor codecExecutor,
        Page page,
        TransportVersion version,
        ChunkSink sink,
        CircuitBreaker breaker,
        int chunkSize,
        int depth,
        @Nullable Releasable permit,
        ActionListener<Long> listener
    ) {
        ChunkBuffers buffers;
        try {
            buffers = new ChunkBuffers(breaker, chunkSize, depth);
        } catch (CircuitBreakingException e) {
            Releasables.close(permit);
            listener.onFailure(e);
            return null;
        }
        page.allowPassingToDifferentDriver();
        Page copy = page.shallowCopy();
        SpillCodecTask task = new SpillCodecTask(copy, version, sink, buffers, permit, listener);
        try {
            codecExecutor.execute(task);
        } catch (RuntimeException e) {
            // an Elasticsearch pool routes a rejection to onRejection itself; a plain executor throws it here instead
            task.onRejection(e);
        }
        return task;
    }

    /**
     * Fails the task at its next chunk boundary, waking it if it waits on an append. Safe from any thread and at any time.
     */
    public void cancel() {
        cancelled = true;
        out.cancel();
    }

    @Override
    protected void doRun() throws Exception {
        if (cancelled) {
            throw new TaskCancelledException("spill cancelled");
        }
        long bytes;
        try (OutputStreamStreamOutput stream = new OutputStreamStreamOutput(out)) {
            stream.setTransportVersion(version);
            page.writeTo(stream);
            bytes = out.finish();
        }
        complete(bytes, null);
    }

    @Override
    public void onFailure(Exception e) {
        complete(0, e);
    }

    @Override
    public boolean isForceExecution() {
        return false;
    }

    /**
     * Runs last whatever happened, including an {@link Error} that bypassed {@link #onFailure}: the task must never end
     * holding its page, its buffers or its permit, nor leave its listener waiting.
     */
    @Override
    public void onAfter() {
        if (completed.get() == false) {
            complete(0, new IllegalStateException("spill codec task ended without completing"));
        }
    }

    private void complete(long bytes, @Nullable Exception failure) {
        if (completed.compareAndSet(false, true) == false) {
            return;
        }
        boolean notified = false;
        try {
            Exception outcome = failure;
            try {
                Releasables.close(out::close, page::releaseBlocks, buffers, permit);
            } catch (RuntimeException e) {
                // the listener is owed a completion even when cleanup fails; keep the first failure and attach this one
                outcome = outcome == null ? e : ExceptionsHelper.useOrSuppress(outcome, e);
            }
            notified = true;
            if (outcome == null) {
                listener.onResponse(bytes);
            } else {
                listener.onFailure(outcome);
            }
        } finally {
            if (notified == false) {
                // cleanup threw an Error; it propagates, but the listener must not be left waiting
                listener.onFailure(new IllegalStateException("spill codec task cleanup failed"));
            }
        }
    }

    /**
     * Bytes appended so far; exact once the listener has completed, a progress indication before.
     */
    public long bytesWritten() {
        return out.bytesWritten();
    }

    /**
     * Chunks appended so far; exact once the listener has completed, a progress indication before.
     */
    public int chunksAppended() {
        return out.chunksAppended();
    }
}
