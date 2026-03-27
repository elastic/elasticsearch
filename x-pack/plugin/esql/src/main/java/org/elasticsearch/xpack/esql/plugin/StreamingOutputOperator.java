/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Sink operator that accumulates internal pages until the total row count reaches the requested
 * page size, then emits them as one chunk to the HTTP stream via {@link StreamingProvider}.
 * <p>
 * Each emitted chunk contains exactly {@code pageSize} rows (except possibly the last chunk),
 * achieved by passing an offset and row count to the serialization layer rather than slicing pages.
 * <p>
 * Backpressure is applied when the {@link StreamingProvider}'s bounded buffer is full:
 * the Driver is blocked via {@link #isBlocked()} until the client consumes a chunk.
 */
public class StreamingOutputOperator extends SinkOperator {

    private static final Logger logger = LogManager.getLogger(StreamingOutputOperator.class);

    private final StreamingProvider provider;
    private final Function<Page, Page> mapper;
    private final int pageSize;

    private final List<Page> accumulator = new ArrayList<>();
    private int accumulatedRows = 0;
    private int pendingOffset = 0;
    private int totalRows = 0;
    private boolean finishing = false;
    private volatile boolean finished = false;
    private volatile Exception streamingFailure;

    public StreamingOutputOperator(StreamingProvider provider, Function<Page, Page> mapper) {
        this.provider = provider;
        this.mapper = mapper;
        this.pageSize = provider.pageSize();
    }

    @Override
    public boolean isFinished() {
        if (finished) {
            return true;
        }
        if (streamingFailure != null) {
            finished = true;
            return true;
        }
        return false;
    }

    @Override
    public void finish() {
        if (finishing || finished) {
            return;
        }
        finishing = true;
        long t0 = System.nanoTime();
        while (accumulatedRows > 0) {
            flushExact(Math.min(pageSize, accumulatedRows));
        }
        long t1 = System.nanoTime();
        provider.finish(totalRows);
        long t2 = System.nanoTime();
        logger.info(
            "[TIMING] StreamingOp.finish: flush={}ms, providerFinish={}ms, totalRows={}, thread={}",
            (t1 - t0) / 1_000_000,
            (t2 - t1) / 1_000_000,
            totalRows,
            Thread.currentThread().getName()
        );
        finished = true;
    }

    @Override
    public boolean needsInput() {
        return finishing == false && finished == false && isBlocked().listener().isDone();
    }

    @Override
    public IsBlockedResult isBlocked() {
        Exception failure = streamingFailure;
        if (failure != null) {
            SubscribableListener<Void> failListener = new SubscribableListener<>();
            failListener.onFailure(failure);
            return new IsBlockedResult(failListener, "streaming failure");
        }
        if (finished || provider.isBufferFull() == false) {
            return Operator.NOT_BLOCKED;
        }
        SubscribableListener<Void> backpressure = provider.createBackpressureListener();
        if (backpressure == null) {
            return Operator.NOT_BLOCKED;
        }
        return new IsBlockedResult(backpressure, "streaming backpressure");
    }

    @Override
    protected void doAddInput(Page page) {
        long t0 = System.nanoTime();
        Page mapped = mapper.apply(page);
        long t1 = System.nanoTime();
        accumulator.add(mapped);
        accumulatedRows += mapped.getPositionCount();
        totalRows += mapped.getPositionCount();

        while (accumulatedRows >= pageSize && provider.isBufferFull() == false && streamingFailure == null) {
            flushExact(pageSize);
        }
        long t2 = System.nanoTime();
        logger.info(
            "[TIMING] StreamingOp.addInput: mapper={}ms, total={}ms, rows={}, thread={}",
            (t1 - t0) / 1_000_000,
            (t2 - t0) / 1_000_000,
            mapped.getPositionCount(),
            Thread.currentThread().getName()
        );
    }

    private void flushExact(int rowCount) {
        int offset = pendingOffset;
        int actualRowCount = Math.min(rowCount, accumulatedRows);
        int remaining = actualRowCount;
        int pagesNeeded = 0;

        int scan = offset;
        for (int i = 0; i < accumulator.size() && remaining > 0; i++) {
            int available = accumulator.get(i).getPositionCount() - scan;
            remaining -= Math.min(remaining, available);
            scan = 0;
            pagesNeeded++;
        }

        List<Page> chunkPages = new ArrayList<>(accumulator.subList(0, pagesNeeded));

        List<Page> pagesToRelease = new ArrayList<>();
        int consumed = actualRowCount;
        int newOffset = offset;
        int idx = 0;
        while (consumed > 0 && idx < accumulator.size()) {
            int available = accumulator.get(idx).getPositionCount() - newOffset;
            if (consumed >= available) {
                consumed -= available;
                pagesToRelease.add(accumulator.get(idx));
                idx++;
                newOffset = 0;
            } else {
                newOffset += consumed;
                consumed = 0;
            }
        }
        accumulator.subList(0, pagesToRelease.size()).clear();

        emitChunk(chunkPages, offset, actualRowCount, pagesToRelease);

        pendingOffset = newOffset;
        accumulatedRows -= actualRowCount;
    }

    private void emitChunk(List<Page> outputPages, int offset, int rowCount, List<Page> pagesToRelease) {
        try {
            provider.emitChunk(outputPages, offset, rowCount, pagesToRelease);
        } catch (Exception e) {
            logger.warn("failed to emit streaming chunk", e);
            streamingFailure = e;
            releasePages(pagesToRelease);
        }
    }

    private static void releasePages(List<Page> pages) {
        for (Page p : pages) {
            p.releaseBlocks();
        }
    }

    @Override
    public void close() {
        releasePages(accumulator);
        accumulator.clear();
    }

    @Override
    public String toString() {
        return "StreamingOutputOperator[pageSize=" + pageSize + "]";
    }

    public record StreamingOutputOperatorFactory(StreamingProvider provider, Function<Page, Page> mapper) implements SinkOperatorFactory {

        @Override
        public SinkOperator get(DriverContext driverContext) {
            return new StreamingOutputOperator(provider, mapper);
        }

        @Override
        public String describe() {
            return "StreamingOutputOperator[pageSize=" + provider.pageSize() + "]";
        }
    }
}
