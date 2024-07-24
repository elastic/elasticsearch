/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * {@link AsyncOperator} performs an external computation specified in {@link #performAsync(Page, ActionListener)}.
 * This operator acts as a client and operates on a per-page basis to reduce communication overhead.
 * @see #performAsync(Page, ActionListener)
 */
public abstract class AsyncOperator implements Operator {

    private volatile SubscribableListener<Void> blockedFuture;

    private final Map<Long, Page> buffers = ConcurrentCollections.newConcurrentMap();
    private final FailureCollector failureCollector = new FailureCollector();
    private final DriverContext driverContext;

    private final int maxOutstandingRequests;
    private final LongAdder totalTimeInNanos = new LongAdder();
    private boolean finished = false;
    private volatile boolean closed = false;

    /*
     * The checkpoint tracker is used to maintain the order of emitted pages after passing through this async operator.
     * - Generates a new sequence number for each incoming page
     * - Uses the processed checkpoint for pages that have completed this computation
     * - Uses the persisted checkpoint for pages that have already been emitted to the next operator
     */
    private final LocalCheckpointTracker checkpoint = new LocalCheckpointTracker(
        SequenceNumbers.NO_OPS_PERFORMED,
        SequenceNumbers.NO_OPS_PERFORMED
    );

    /**
     * Create an operator that performs an external computation
     *
     * @param maxOutstandingRequests the maximum number of outstanding requests
     */
    public AsyncOperator(DriverContext driverContext, int maxOutstandingRequests) {
        this.driverContext = driverContext;
        this.maxOutstandingRequests = maxOutstandingRequests;
    }

    @Override
    public boolean needsInput() {
        final long outstandingPages = checkpoint.getMaxSeqNo() - checkpoint.getPersistedCheckpoint();
        return outstandingPages < maxOutstandingRequests;
    }

    @Override
    public void addInput(Page input) {
        if (failureCollector.hasFailure()) {
            input.releaseBlocks();
            return;
        }
        final long seqNo = checkpoint.generateSeqNo();
        driverContext.addAsyncAction();
        boolean success = false;
        try {
            final ActionListener<Page> listener = ActionListener.wrap(output -> {
                buffers.put(seqNo, output);
                onSeqNoCompleted(seqNo);
            }, e -> {
                releasePageOnAnyThread(input);
                failureCollector.unwrapAndCollect(e);
                onSeqNoCompleted(seqNo);
            });
            final long startNanos = System.nanoTime();
            performAsync(input, ActionListener.runAfter(listener, () -> {
                driverContext.removeAsyncAction();
                totalTimeInNanos.add(System.nanoTime() - startNanos);
            }));
            success = true;
        } finally {
            if (success == false) {
                driverContext.removeAsyncAction();
            }
        }
    }

    private void releasePageOnAnyThread(Page page) {
        page.allowPassingToDifferentDriver();
        page.releaseBlocks();
    }

    /**
     * Performs an external computation and notify the listener when the result is ready.
     *
     * @param inputPage the input page
     * @param listener  the listener
     */
    protected abstract void performAsync(Page inputPage, ActionListener<Page> listener);

    protected abstract void doClose();

    private void onSeqNoCompleted(long seqNo) {
        checkpoint.markSeqNoAsProcessed(seqNo);
        if (checkpoint.getPersistedCheckpoint() < checkpoint.getProcessedCheckpoint()) {
            notifyIfBlocked();
        }
        if (closed || failureCollector.hasFailure()) {
            discardPages();
        }
    }

    private void notifyIfBlocked() {
        if (blockedFuture != null) {
            final SubscribableListener<Void> future;
            synchronized (this) {
                future = blockedFuture;
                this.blockedFuture = null;
            }
            if (future != null) {
                future.onResponse(null);
            }
        }
    }

    private void checkFailure() {
        Exception e = failureCollector.getFailure();
        if (e != null) {
            discardPages();
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    private void discardPages() {
        long nextCheckpoint;
        while ((nextCheckpoint = checkpoint.getPersistedCheckpoint() + 1) <= checkpoint.getProcessedCheckpoint()) {
            Page page = buffers.remove(nextCheckpoint);
            checkpoint.markSeqNoAsPersisted(nextCheckpoint);
            if (page != null) {
                releasePageOnAnyThread(page);
            }
        }
    }

    @Override
    public final void close() {
        finish();
        closed = true;
        discardPages();
        doClose();
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        if (finished && checkpoint.getPersistedCheckpoint() == checkpoint.getMaxSeqNo()) {
            checkFailure();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Page getOutput() {
        checkFailure();
        long persistedCheckpoint = checkpoint.getPersistedCheckpoint();
        if (persistedCheckpoint < checkpoint.getProcessedCheckpoint()) {
            persistedCheckpoint++;
            Page page = buffers.remove(persistedCheckpoint);
            checkpoint.markSeqNoAsPersisted(persistedCheckpoint);
            return page;
        } else {
            return null;
        }
    }

    @Override
    public SubscribableListener<Void> isBlocked() {
        // TODO: Add an exchange service between async operation instead?
        if (finished) {
            return Operator.NOT_BLOCKED;
        }
        long persistedCheckpoint = checkpoint.getPersistedCheckpoint();
        if (persistedCheckpoint == checkpoint.getMaxSeqNo() || persistedCheckpoint < checkpoint.getProcessedCheckpoint()) {
            return Operator.NOT_BLOCKED;
        }
        synchronized (this) {
            persistedCheckpoint = checkpoint.getPersistedCheckpoint();
            if (persistedCheckpoint == checkpoint.getMaxSeqNo() || persistedCheckpoint < checkpoint.getProcessedCheckpoint()) {
                return Operator.NOT_BLOCKED;
            }
            if (blockedFuture == null) {
                blockedFuture = new SubscribableListener<>();
            }
            return blockedFuture;
        }
    }

    @Override
    public final Operator.Status status() {
        return status(
            Math.max(0L, checkpoint.getMaxSeqNo()),
            Math.max(0L, checkpoint.getProcessedCheckpoint()),
            TimeValue.timeValueNanos(totalTimeInNanos.sum()).millis()
        );
    }

    protected Operator.Status status(long receivedPages, long completedPages, long totalTimeInMillis) {
        return new Status(receivedPages, completedPages, totalTimeInMillis);
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "async_operator",
            Status::new
        );

        final long receivedPages;
        final long completedPages;
        final long totalTimeInMillis;

        protected Status(long receivedPages, long completedPages, long totalTimeInMillis) {
            this.receivedPages = receivedPages;
            this.completedPages = completedPages;
            this.totalTimeInMillis = totalTimeInMillis;
        }

        protected Status(StreamInput in) throws IOException {
            this.receivedPages = in.readVLong();
            this.completedPages = in.readVLong();
            this.totalTimeInMillis = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(receivedPages);
            out.writeVLong(completedPages);
            out.writeVLong(totalTimeInMillis);
        }

        public long receivedPages() {
            return receivedPages;
        }

        public long completedPages() {
            return completedPages;
        }

        public long totalTimeInMillis() {
            return totalTimeInMillis;
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            innerToXContent(builder);
            return builder.endObject();
        }

        protected final XContentBuilder innerToXContent(XContentBuilder builder) throws IOException {
            builder.field("received_pages", receivedPages);
            builder.field("completed_pages", completedPages);
            builder.field("total_time_in_millis", totalTimeInMillis);
            if (totalTimeInMillis >= 0) {
                builder.field("total_time", TimeValue.timeValueMillis(totalTimeInMillis));
            }
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return receivedPages == status.receivedPages
                && completedPages == status.completedPages
                && totalTimeInMillis == status.totalTimeInMillis;
        }

        @Override
        public int hashCode() {
            return Objects.hash(receivedPages, completedPages, totalTimeInMillis);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_14_0;
        }
    }
}
