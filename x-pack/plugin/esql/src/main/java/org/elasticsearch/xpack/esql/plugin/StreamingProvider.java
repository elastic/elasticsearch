/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlStreamCompletionBodyPart;
import org.elasticsearch.xpack.esql.action.EsqlStreamResponseBodyPart;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bridge between the {@link StreamingOutputOperator} (producer in the compute pipeline)
 * and the HTTP response (consumer via Netty chunked transfer encoding).
 * <p>
 * Pages produced by the operator are enqueued into a bounded buffer. When the buffer is
 * full, the operator blocks via {@code isBlocked()} until Netty consumes a chunk and
 * frees a slot. This provides natural backpressure from the HTTP client all the way
 * back to the compute pipeline.
 * <p>
 * The first chunk triggers sending the HTTP response headers. Subsequent chunks are
 * delivered as continuations via {@link ChunkedRestResponseBodyPart#getNextPart}.
 */
public class StreamingProvider {

    private static final org.elasticsearch.logging.Logger LOGGER = org.elasticsearch.logging.LogManager.getLogger(StreamingProvider.class);

    static final int MAX_BUFFERED_CHUNKS = 3;

    private final RestChannel channel;
    private final HttpChannel httpChannel;
    private final int pageSize;
    private volatile List<ColumnInfoImpl> columns;

    private final Object lock = new Object();
    private final java.util.ArrayDeque<StreamingChunk> chunkQueue = new java.util.ArrayDeque<>();
    private final AtomicInteger bufferedCount = new AtomicInteger();
    private final AtomicBoolean firstChunkEmitted = new AtomicBoolean(false);
    private final AtomicBoolean responseSent = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private volatile int totalRowCount;
    private volatile long tookMillis;
    private volatile boolean isPartial;
    private volatile EsqlExecutionInfo executionInfo;

    /**
     * Listener waiting for a slot to open in the buffer. Set by the operator when
     * the buffer is full; resolved when Netty consumes a chunk.
     */
    private SubscribableListener<Void> backpressureListener;

    /**
     * Listener from the REST layer waiting for the next chunk to become available.
     * Set by {@link EsqlStreamResponseBodyPart#getNextPart}; resolved when the operator
     * enqueues the next chunk or signals completion.
     */
    private ActionListener<ChunkedRestResponseBodyPart> nextPartListener;

    public StreamingProvider(RestChannel channel, int pageSize) {
        this.channel = channel;
        this.httpChannel = channel.request().getHttpChannel();
        this.pageSize = pageSize;
    }

    public int pageSize() {
        return pageSize;
    }

    public void setColumns(List<ColumnInfoImpl> columns) {
        this.columns = columns;
    }

    public List<ColumnInfoImpl> columns() {
        return columns;
    }

    public void setExecutionInfo(EsqlExecutionInfo executionInfo) {
        this.executionInfo = executionInfo;
    }

    /**
     * Called by the operator to emit a chunk of pages. If the buffer is full, the caller
     * should check {@link #isBufferFull()} beforehand; this method will still accept the
     * chunk but will not apply backpressure itself.
     */
    public void emitChunk(List<Page> pages, int offset, int rowCount, List<Page> pagesToRelease) {
        long t0 = System.nanoTime();
        boolean isFirst = firstChunkEmitted.compareAndSet(false, true);
        StreamingChunk chunk = new StreamingChunk(pages, columns, isFirst, offset, rowCount, pagesToRelease);

        synchronized (lock) {
            if (responseSent.compareAndSet(false, true)) {
                long t1 = System.nanoTime();
                EsqlStreamResponseBodyPart firstPart = new EsqlStreamResponseBodyPart(chunk, this, httpChannel);
                long t2 = System.nanoTime();
                channel.sendResponse(RestResponse.chunked(RestStatus.OK, firstPart, this::releaseAllChunks));
                long t3 = System.nanoTime();
                LOGGER.info(
                    "[TIMING] emitChunk(first): bodyPart={}ms, sendResponse={}ms, total={}ms, thread={}",
                    (t2 - t1) / 1_000_000,
                    (t3 - t2) / 1_000_000,
                    (t3 - t0) / 1_000_000,
                    Thread.currentThread().getName()
                );
            } else {
                chunkQueue.add(chunk);
                bufferedCount.incrementAndGet();
                ActionListener<ChunkedRestResponseBodyPart> waiting = nextPartListener;
                nextPartListener = null;
                if (waiting != null) {
                    StreamingChunk next = chunkQueue.poll();
                    if (next != null) {
                        bufferedCount.decrementAndGet();
                        notifyBackpressure();
                        waiting.onResponse(new EsqlStreamResponseBodyPart(next, this, httpChannel));
                    }
                }
                long t1 = System.nanoTime();
                LOGGER.info("[TIMING] emitChunk(queued): total={}ms, thread={}", (t1 - t0) / 1_000_000, Thread.currentThread().getName());
            }
        }
    }

    /**
     * Signals that the query has completed and no more chunks will be produced.
     *
     * @param totalRows total number of rows produced across all chunks
     */
    public void finish(int totalRows) {
        long t0 = System.nanoTime();
        this.totalRowCount = totalRows;
        if (executionInfo != null) {
            executionInfo.markEndQuery();
            this.tookMillis = executionInfo.overallTook().millis();
            this.isPartial = executionInfo.isPartial();
            this.executionInfo = null;
        }
        synchronized (lock) {
            completed.set(true);
            if (responseSent.compareAndSet(false, true)) {
                EsqlStreamCompletionBodyPart completionPart = new EsqlStreamCompletionBodyPart(totalRows, tookMillis, isPartial, columns);
                channel.sendResponse(RestResponse.chunked(RestStatus.OK, completionPart, this::releaseAllChunks));
            } else {
                ActionListener<ChunkedRestResponseBodyPart> waiting = nextPartListener;
                nextPartListener = null;
                if (waiting != null) {
                    StreamingChunk next = chunkQueue.poll();
                    if (next != null) {
                        bufferedCount.decrementAndGet();
                        waiting.onResponse(new EsqlStreamResponseBodyPart(next, this, httpChannel));
                    } else {
                        waiting.onResponse(new EsqlStreamCompletionBodyPart(totalRows, tookMillis, isPartial, null));
                    }
                }
            }
        }
        long t1 = System.nanoTime();
        LOGGER.info("[TIMING] StreamingProvider.finish: total={}ms, thread={}", (t1 - t0) / 1_000_000, Thread.currentThread().getName());
    }

    /**
     * Signals that the query failed. Sends an error to the client if no response was sent yet,
     * or completes the stream with the error if already streaming.
     */
    public void onFailure(Exception e) {
        synchronized (lock) {
            completed.set(true);
            ActionListener<ChunkedRestResponseBodyPart> waiting = nextPartListener;
            nextPartListener = null;
            if (waiting != null) {
                waiting.onFailure(e);
            }
        }
    }

    public boolean isCompleted() {
        return completed.get();
    }

    public boolean hasQueuedChunks() {
        synchronized (lock) {
            return chunkQueue.isEmpty() == false;
        }
    }

    /**
     * Called by the REST body part when it needs the next chunk. If a chunk is already
     * queued, delivers it immediately. Otherwise stores the listener for later resolution.
     */
    public void requestNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
        synchronized (lock) {
            StreamingChunk next = chunkQueue.poll();
            if (next != null) {
                bufferedCount.decrementAndGet();
                notifyBackpressure();
                listener.onResponse(new EsqlStreamResponseBodyPart(next, this, httpChannel));
            } else if (completed.get()) {
                listener.onResponse(new EsqlStreamCompletionBodyPart(totalRowCount, tookMillis, isPartial, null));
            } else {
                nextPartListener = listener;
            }
        }
    }

    public boolean isBufferFull() {
        return bufferedCount.get() >= MAX_BUFFERED_CHUNKS;
    }

    public record CompletionInfo(int totalRows, long tookMillis, boolean isPartial) {}

    /**
     * Atomically tries to claim the completion marker for folding into the current body part.
     * Returns the completion info if the provider is done and the queue is empty, null otherwise.
     * When claimed, {@link #requestNextPart} will not be called because the body part returns
     * {@code isLastPart() = true}.
     */
    public CompletionInfo tryClaimCompletion() {
        synchronized (lock) {
            if (completed.get() && chunkQueue.isEmpty()) {
                return new CompletionInfo(totalRowCount, tookMillis, isPartial);
            }
            return null;
        }
    }

    /**
     * Creates a backpressure listener that will be resolved when a buffer slot opens.
     * Called by the operator's {@code isBlocked()} method.
     */
    public SubscribableListener<Void> createBackpressureListener() {
        synchronized (lock) {
            if (bufferedCount.get() < MAX_BUFFERED_CHUNKS) {
                return null;
            }
            if (backpressureListener == null) {
                backpressureListener = new SubscribableListener<>();
            }
            return backpressureListener;
        }
    }

    private void notifyBackpressure() {
        SubscribableListener<Void> toNotify;
        synchronized (lock) {
            toNotify = backpressureListener;
            backpressureListener = null;
        }
        if (toNotify != null) {
            toNotify.onResponse(null);
        }
    }

    void releaseAllChunks() {
        synchronized (lock) {
            StreamingChunk chunk;
            while ((chunk = chunkQueue.poll()) != null) {
                for (Page p : chunk.pagesToRelease()) {
                    p.releaseBlocks();
                }
            }
            bufferedCount.set(0);
        }
    }

    /**
     * A chunk of pages ready to be serialized and sent to the client.
     *
     * @param isFirstChunk true if this is the first chunk and should include the column header line
     * @param offset starting row index within the first page
     * @param rowCount total number of rows to serialize from this chunk
     * @param pagesToRelease pages that are fully consumed by this chunk and should be released after serialization
     */
    public record StreamingChunk(
        List<Page> pages,
        List<ColumnInfoImpl> columns,
        boolean isFirstChunk,
        int offset,
        int rowCount,
        List<Page> pagesToRelease
    ) {}
}
