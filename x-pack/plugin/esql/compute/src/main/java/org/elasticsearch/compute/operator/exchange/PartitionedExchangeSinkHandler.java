/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

/**
 * A partitioned exchange sink handler that routes pages to per-driver buffers based on
 * {@link Page#getPartitionId()}. Each consumer driver is assigned a contiguous range of
 * partition IDs and has its own {@link ExchangeBuffer}.
 *
 * <p>Producer drivers write pages via {@link ExchangeSink}s created with {@link #createExchangeSink(Runnable)}.
 * Consumer drivers fetch pages for their assigned partitions via
 * {@link #fetchPageAsync(int, boolean, ActionListener)}.
 *
 * @see ExchangeSinkHandler for the non-partitioned variant
 */
public final class PartitionedExchangeSinkHandler {

    /**
     * Per-driver buffers: pages for driver {@code d} are stored in {@code buffers[d]}.
     * Partition ID {@code p} maps to driver {@code p / partitionsPerDriver}.
     */
    private final ExchangeBuffer[] buffers;
    private final int numPartitions;
    private final int numDrivers;
    /** Number of partition IDs that map to each driver ({@code numPartitions / numDrivers}). */
    private final int partitionsPerDriver;

    /**
     * Per-driver listener queues. When a consumer calls {@link #fetchPageAsync}, its listener
     * is enqueued here. Listeners are notified when a page arrives in or the buffer finishes.
     */
    private final List<Queue<ActionListener<ExchangeResponse>>> listeners;
    /**
     * Per-driver semaphores to ensure only one thread at a time dequeues a listener and
     * polls a page from the buffer. Prevents race conditions in {@link #notifyListeners}.
     */
    private final Semaphore[] promised;

    /** Number of open sinks. When this reaches zero, all per-driver buffers are finished. */
    private final AtomicInteger outstandingSinks = new AtomicInteger();
    /** Fires when all per-driver buffers are finished (all pages consumed, all sinks closed). */
    private final SubscribableListener<Void> completionFuture;

    private final LongSupplier nowInMillis;
    private final AtomicLong lastUpdatedInMillis;
    private final BlockFactory blockFactory;

    /**
     * Creates a new partitioned exchange sink handler.
     *
     * @param blockFactory    the block factory for creating exchange responses
     * @param numPartitions   total number of partitions (e.g. 256)
     * @param numDrivers      number of consumer drivers
     * @param maxBufferSize   maximum buffer size per driver
     * @param nowInMillis     supplier for the current time in milliseconds
     */
    public PartitionedExchangeSinkHandler(
        BlockFactory blockFactory,
        int numPartitions,
        int numDrivers,
        int maxBufferSize,
        LongSupplier nowInMillis
    ) {
        if (numPartitions < 1) {
            throw new IllegalArgumentException("numPartitions must be at least 1; got=" + numPartitions);
        }
        if (numDrivers < 1) {
            throw new IllegalArgumentException("numDrivers must be at least 1; got=" + numDrivers);
        }
        if (numPartitions % numDrivers != 0) {
            throw new IllegalArgumentException(
                "numPartitions [" + numPartitions + "] must be evenly divisible by numDrivers [" + numDrivers + "]"
            );
        }
        this.blockFactory = blockFactory;
        this.numPartitions = numPartitions;
        this.numDrivers = numDrivers;
        this.partitionsPerDriver = numPartitions / numDrivers;
        this.buffers = new ExchangeBuffer[numDrivers];
        this.listeners = IntStream.range(0, numDrivers)
            .<Queue<ActionListener<ExchangeResponse>>>mapToObj(i -> new ConcurrentLinkedQueue<>())
            .toList();
        this.promised = new Semaphore[numDrivers];
        for (int i = 0; i < numDrivers; i++) {
            buffers[i] = new ExchangeBuffer(maxBufferSize);
            promised[i] = new Semaphore(1);
        }
        // The completion future fires when all buffers are finished
        this.completionFuture = new SubscribableListener<>();
        for (int i = 0; i < numDrivers; i++) {
            buffers[i].addCompletionListener(ActionListener.wrap(v -> checkAllBuffersComplete(), completionFuture::onFailure));
        }
        this.nowInMillis = nowInMillis;
        this.lastUpdatedInMillis = new AtomicLong(nowInMillis.getAsLong());
    }

    private void checkAllBuffersComplete() {
        for (ExchangeBuffer buffer : buffers) {
            if (buffer.isFinished() == false) {
                return;
            }
        }
        completionFuture.onResponse(null);
    }

    /**
     * Returns the driver index that owns the given partition ID.
     */
    int driverForPartition(int partitionId) {
        assert partitionId >= 0 && partitionId < numPartitions
            : "partitionId [" + partitionId + "] out of range [0, " + numPartitions + ")";
        return partitionId / partitionsPerDriver;
    }

    /**
     * Each router driver gets its own sink instance. The sink reads the partitionId from each
     * incoming page and routes it to the correct per-driver buffer. When the last sink finishes
     * (outstandingSinks drops to 0), all per-driver buffers are marked finished so consumer
     * drivers know no more data is coming.
     */
    private class PartitionedExchangeSinkImpl implements ExchangeSink {
        boolean finished;
        private final Runnable onPageFetched;
        private final SubscribableListener<Void> onFinished = new SubscribableListener<>();

        PartitionedExchangeSinkImpl(Runnable onPageFetched) {
            this.onPageFetched = onPageFetched;
            onChanged();
            // This sink is considered finished when ALL buffers are done, not just one
            for (ExchangeBuffer buffer : buffers) {
                buffer.addCompletionListener(onFinished);
            }
            outstandingSinks.incrementAndGet();
        }

        @Override
        public void addPage(Page page) {
            // The page must have been tagged with a valid partitionId by the HashAggregationOperator
            int partitionId = page.getPartitionId();
            if (partitionId < 0 || partitionId >= numPartitions) {
                page.releaseBlocks();
                throw new IllegalArgumentException(
                    "Page has invalid partitionId [" + partitionId + "]; expected [0, " + numPartitions + ")"
                );
            }
            // Route to the driver that owns this partition range
            int driverIndex = driverForPartition(partitionId);
            onPageFetched.run();
            buffers[driverIndex].addPage(page);
            // Wake up any consumer waiting for data on this driver's buffer
            notifyListeners(driverIndex);
        }

        @Override
        public void finish() {
            if (finished == false) {
                finished = true;
                onFinished.onResponse(null);
                onChanged();
                // When all sinks are closed, mark every buffer as finished and wake consumers
                if (outstandingSinks.decrementAndGet() == 0) {
                    for (int i = 0; i < numDrivers; i++) {
                        buffers[i].finish(false);
                        notifyListeners(i);
                    }
                }
            }
        }

        @Override
        public boolean isFinished() {
            return onFinished.isDone();
        }

        @Override
        public void addCompletionListener(ActionListener<Void> listener) {
            onFinished.addListener(listener);
        }

        @Override
        public IsBlockedResult waitForWriting() {
            // Check all buffers; block if any is full.
            // This is a conservative approach -- we block the sink if ANY target buffer is full.
            for (ExchangeBuffer buffer : buffers) {
                IsBlockedResult result = buffer.waitForWriting();
                if (result.listener().isDone() == false) {
                    return result;
                }
            }
            return org.elasticsearch.compute.operator.Operator.NOT_BLOCKED;
        }
    }

    /**
     * Fetches pages and status for a specific driver asynchronously.
     *
     * @param driverIndex    the driver index (0 to numDrivers-1)
     * @param sourceFinished if true, this handler can finish as the source has enough pages
     * @param listener       the listener notified when pages are ready or this driver's buffer is finished
     */
    public void fetchPageAsync(int driverIndex, boolean sourceFinished, ActionListener<ExchangeResponse> listener) {
        if (sourceFinished) {
            buffers[driverIndex].finish(true);
        }
        listeners.get(driverIndex).add(listener);
        onChanged();
        notifyListeners(driverIndex);
    }

    /**
     * Drains the listener queue for a specific driver, pairing each listener with a page
     * (or a "finished" signal) from the driver's buffer. The semaphore ensures that only
     * one thread at a time performs the poll-and-respond for a given driver, avoiding
     * double-delivery or lost pages.
     */
    private void notifyListeners(int driverIndex) {
        ExchangeBuffer buffer = buffers[driverIndex];
        Queue<ActionListener<ExchangeResponse>> driverListeners = listeners.get(driverIndex);
        while (driverListeners.isEmpty() == false && (buffer.size() > 0 || buffer.noMoreInputs())) {
            if (promised[driverIndex].tryAcquire() == false) {
                break;
            }
            final ActionListener<ExchangeResponse> listener;
            final ExchangeResponse response;
            try {
                listener = driverListeners.poll();
                if (listener == null) {
                    continue;
                }
                response = new ExchangeResponse(blockFactory, buffer.pollPage(), buffer.isFinished());
            } finally {
                promised[driverIndex].release();
            }
            onChanged();
            ActionListener.respondAndRelease(listener, response);
        }
    }

    /**
     * Add a completion listener that is notified when all buffers are finished.
     */
    public void addCompletionListener(ActionListener<Void> listener) {
        completionFuture.addListener(listener);
    }

    /**
     * Returns true if all exchange buffers are finished.
     */
    public boolean isFinished() {
        return completionFuture.isDone();
    }

    /**
     * Fails this sink exchange handler.
     */
    void onFailure(Exception failure) {
        completionFuture.onFailure(failure);
        for (int i = 0; i < numDrivers; i++) {
            buffers[i].finish(true);
            notifyListeners(i);
        }
    }

    /**
     * Create a new exchange sink for exchanging data.
     *
     * @param onPageFetched a {@link Runnable} called when a page is fetched
     */
    public ExchangeSink createExchangeSink(Runnable onPageFetched) {
        return new PartitionedExchangeSinkImpl(onPageFetched);
    }

    /**
     * Whether this handler has sinks attached or available pages in any buffer.
     */
    boolean hasData() {
        if (outstandingSinks.get() > 0) {
            return true;
        }
        for (ExchangeBuffer buffer : buffers) {
            if (buffer.size() > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether any buffer has listeners waiting for data.
     */
    boolean hasListeners() {
        for (var l : listeners) {
            if (l.isEmpty() == false) {
                return true;
            }
        }
        return false;
    }

    private void onChanged() {
        lastUpdatedInMillis.accumulateAndGet(nowInMillis.getAsLong(), Math::max);
    }

    /**
     * The time in millis when this handler was last updated.
     */
    long lastUpdatedTimeInMillis() {
        return lastUpdatedInMillis.get();
    }

    /**
     * Returns the number of pages available in a specific driver's buffer.
     */
    public int bufferSize(int driverIndex) {
        return buffers[driverIndex].size();
    }

    /**
     * Returns the total number of partitions.
     */
    public int numPartitions() {
        return numPartitions;
    }

    /**
     * Returns the number of consumer drivers.
     */
    public int numDrivers() {
        return numDrivers;
    }
}
