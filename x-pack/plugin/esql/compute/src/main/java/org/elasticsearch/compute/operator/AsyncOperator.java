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
import org.elasticsearch.common.util.concurrent.ThreadContext;
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
 * {@link AsyncOperator} performs an external computation specified in
 * {@link #performAsync(Page, ActionListener)}. This operator acts as a client
 * to reduce communication overhead and fetches a {@code Fetched} at a time.
 * It's the responsibility of subclasses to transform that {@code Fetched} into
 * output.
 * <p>
 *     This operator will also take care of merging response headers from the thread context into the main thread,
 *     which <b>must</b> be the one that closes this.
 * </p>
 *
 * @see #performAsync(Page, ActionListener)
 */
public abstract class AsyncOperator<Fetched> implements Operator {

    private volatile SubscribableListener<Void> blockedFuture;

    private final Map<Long, Fetched> buffers = ConcurrentCollections.newConcurrentMap();
    private final FailureCollector failureCollector = new FailureCollector();
    private final DriverContext driverContext;

    private final int maxOutstandingRequests;
    private final ResponseHeadersCollector responseHeadersCollector;
    private final LongAdder processNanos = new LongAdder();

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
    public AsyncOperator(DriverContext driverContext, ThreadContext threadContext, int maxOutstandingRequests) {
        this.driverContext = driverContext;
        this.maxOutstandingRequests = maxOutstandingRequests;
        this.responseHeadersCollector = new ResponseHeadersCollector(threadContext);
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
            final ActionListener<Fetched> listener = ActionListener.wrap(output -> {
                buffers.put(seqNo, output);
                onSeqNoCompleted(seqNo);
            }, e -> {
                releasePageOnAnyThread(input);
                failureCollector.unwrapAndCollect(e);
                onSeqNoCompleted(seqNo);
            });
            final long startNanos = System.nanoTime();
            performAsync(input, ActionListener.runAfter(listener, () -> {
                responseHeadersCollector.collect();
                driverContext.removeAsyncAction();
                processNanos.add(System.nanoTime() - startNanos);
            }));
            success = true;
        } finally {
            if (success == false) {
                driverContext.removeAsyncAction();
            }
        }
    }

    protected static void releasePageOnAnyThread(Page page) {
        page.allowPassingToDifferentDriver();
        page.releaseBlocks();
    }

    protected abstract void releaseFetchedOnAnyThread(Fetched result);

    /**
     * Performs an external computation and notify the listener when the result is ready.
     *
     * @param inputPage the input page
     * @param listener  the listener
     */
    protected abstract void performAsync(Page inputPage, ActionListener<Fetched> listener);

    protected abstract void doClose();

    private void onSeqNoCompleted(long seqNo) {
        checkpoint.markSeqNoAsProcessed(seqNo);
        if (checkpoint.getPersistedCheckpoint() < checkpoint.getProcessedCheckpoint()) {
            notifyIfBlocked();
        }
        if (closed || failureCollector.hasFailure()) {
            discardResults();
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
            discardResults();
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private void discardResults() {
        long nextCheckpoint;
        while ((nextCheckpoint = checkpoint.getPersistedCheckpoint() + 1) <= checkpoint.getProcessedCheckpoint()) {
            Fetched result = buffers.remove(nextCheckpoint);
            checkpoint.markSeqNoAsPersisted(nextCheckpoint);
            if (result != null) {
                releaseFetchedOnAnyThread(result);
            }
        }
    }

    @Override
    public final void close() {
        finish();
        closed = true;
        discardResults();
        responseHeadersCollector.finish();
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

    /**
     * Get a {@link Fetched} from the buffer.
     * @return a result if one is ready or {@code null} if none are available.
     */
    public final Fetched fetchFromBuffer() {
        checkFailure();
        long persistedCheckpoint = checkpoint.getPersistedCheckpoint();
        if (persistedCheckpoint < checkpoint.getProcessedCheckpoint()) {
            persistedCheckpoint++;
            Fetched result = buffers.remove(persistedCheckpoint);
            checkpoint.markSeqNoAsPersisted(persistedCheckpoint);
            return result;
        } else {
            return null;
        }
    }

    @Override
    public IsBlockedResult isBlocked() {
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
            return new IsBlockedResult(blockedFuture, getClass().getSimpleName());
        }
    }

    @Override
    public final Operator.Status status() {
        return status(Math.max(0L, checkpoint.getMaxSeqNo()), Math.max(0L, checkpoint.getProcessedCheckpoint()), processNanos.sum());
    }

    protected Operator.Status status(long receivedPages, long completedPages, long processNanos) {
        return new Status(receivedPages, completedPages, processNanos);
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "async_operator",
            Status::new
        );

        final long receivedPages;
        final long completedPages;
        final long processNanos;

        protected Status(long receivedPages, long completedPages, long processNanos) {
            this.receivedPages = receivedPages;
            this.completedPages = completedPages;
            this.processNanos = processNanos;
        }

        protected Status(StreamInput in) throws IOException {
            this.receivedPages = in.readVLong();
            this.completedPages = in.readVLong();
            this.processNanos = in.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_ASYNC_NANOS)
                ? in.readVLong()
                : TimeValue.timeValueMillis(in.readVLong()).nanos();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(receivedPages);
            out.writeVLong(completedPages);
            out.writeVLong(
                out.getTransportVersion().onOrAfter(TransportVersions.ESQL_PROFILE_ASYNC_NANOS)
                    ? processNanos
                    : TimeValue.timeValueNanos(processNanos).millis()
            );
        }

        public long receivedPages() {
            return receivedPages;
        }

        public long completedPages() {
            return completedPages;
        }

        public long procesNanos() {
            return processNanos;
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
            builder.field("process_nanos", processNanos);
            if (builder.humanReadable()) {
                builder.field("process_time", TimeValue.timeValueNanos(processNanos));
            }
            builder.field("received_pages", receivedPages);
            builder.field("completed_pages", completedPages);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return receivedPages == status.receivedPages && completedPages == status.completedPages && processNanos == status.processNanos;
        }

        @Override
        public int hashCode() {
            return Objects.hash(receivedPages, completedPages, processNanos);
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
