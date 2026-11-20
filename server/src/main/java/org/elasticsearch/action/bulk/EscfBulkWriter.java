/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Indexes rows into one index as pre-built ESCF batches, for one task. Each thread that writes has its own {@link Producer},
 * which fills a batch without a shared lock. A batch goes to the writer when it reaches the target size, or right away when no
 * bulk is in flight. So batches only grow while the destination is slower than the producers. The writer sends the batches in
 * order, as bulk slots free up.
 * <p>
 * Bulks and callbacks run in the thread context of the thread that created the writer. The memory of the batches is charged to
 * the task's breaker and to the node's coordinating {@link IndexingPressure} until their bulk is acknowledged. When either trips,
 * the writer sends every batch it holds and the producers wait for those rows to be sent. If the writer holds no rows, the trip
 * fails it. Its {@link ResponseHandler} chooses which failed items fail the writer. A failure, or {@link #close()}, drops the
 * rows not sent yet.
 * <p>
 * The bulks fail while the cluster does not support batch indexing, so callers check
 * {@link ShardBatchIndexer#isBatchIndexingSupported} before they create a writer.
 */
public final class EscfBulkWriter implements Releasable {

    /**
     * @param maxBulksInFlight how many bulks may wait for an acknowledgement; with {@code 1} and one producer, rows are indexed
     *                         in order
     * @param targetBulkBytes  the size at which a batch takes no more rows, at most {@link #MAX_TARGET_BULK_BYTES}
     */
    public record Policy(int maxBulksInFlight, long targetBulkBytes) {
        public static final long MAX_TARGET_BULK_BYTES = ByteSizeValue.ofMb(64).getBytes();
        /** Caps the bulks in flight of a {@link #fromIndexingPressure default} writer until rejected bulks are retried. */
        public static final int MAX_DEFAULT_BULKS_IN_FLIGHT = 8;

        public Policy {
            if (maxBulksInFlight < 1) {
                throw new IllegalArgumentException("maxBulksInFlight must be at least 1 but was [" + maxBulksInFlight + "]");
            }
            if (targetBulkBytes < 1 || targetBulkBytes > MAX_TARGET_BULK_BYTES) {
                throw new IllegalArgumentException(
                    "targetBulkBytes must be between 1 and [" + MAX_TARGET_BULK_BYTES + "] but was [" + targetBulkBytes + "]"
                );
            }
        }

        /**
         * A policy for writers that do not need their rows in order. Batches take the size of a streaming bulk chunk above the
         * low watermark, and a writer holds at most the low watermark in flight, so it alone never makes streaming bulks split.
         */
        public static Policy fromIndexingPressure(IndexingPressure indexingPressure) {
            long targetBulkBytes = Math.clamp(indexingPressure.splitBulkLowWatermarkSize(), 1, MAX_TARGET_BULK_BYTES);
            long maxBulksInFlight = Math.clamp(indexingPressure.splitBulkLowWatermark() / targetBulkBytes, 1, MAX_DEFAULT_BULKS_IN_FLIGHT);
            return new Policy((int) maxBulksInFlight, targetBulkBytes);
        }
    }

    /**
     * Rows added at once, such as the rows of a page.
     */
    public interface Rows {
        int count();

        /**
         * Appends the rows to partition {@code 0} of {@code batch}.
         */
        void append(EscfBatchBuilder batch) throws IOException;

        /**
         * The request that indexes row {@code row}. The writer sets its source.
         */
        IndexRequest request(int row);
    }

    /**
     * Reads the response of each bulk, to count its items or to choose which failed items fail the writer.
     */
    @FunctionalInterface
    public interface ResponseHandler {
        /**
         * Fails the writer on the first failed item.
         */
        ResponseHandler FAIL_ON_FAILED_ITEM = EscfBulkWriter::firstFailure;

        /**
         * Called on the service's executor for each bulk that is acknowledged, maybe for several bulks at once. A bulk that fails
         * as a whole fails the writer without a call.
         *
         * @return the failure that fails the writer, or {@code null} to go on
         */
        @Nullable
        Exception onResponse(BulkResponse response);
    }

    /**
     * The rows of one bulk and the memory charged for them.
     */
    private final class Chunk implements Releasable {
        private final EscfBatchBuilder builder = new EscfBatchBuilder(recycler);
        private final List<IndexRequest> requests = new ArrayList<>();
        private final IndexingPressure.Coordinating pressure = service.indexingPressure().createCoordinatingOperation(false);
        // the rows counted after indexing pressure rejected them
        @Nullable
        private IndexingPressure.Coordinating forcedPressure;
        private long builderBytes;
        private long requestBytes;
        // the context its bulk carries is counted in indexing pressure
        private boolean contextCounted;
        // its rows tripped the breaker or indexing pressure
        private boolean tripped;

        long bytes() {
            return builderBytes + requestBytes;
        }

        IndexingPressure.Coordinating forcedPressure() {
            if (forcedPressure == null) {
                forcedPressure = service.indexingPressure().createCoordinatingOperation(true);
            }
            return forcedPressure;
        }

        /**
         * Gives back what indexing pressure counts for these rows. Called once, when they are acknowledged or dropped.
         */
        Releasable releasePressure() {
            return () -> Releasables.close(pressure, forcedPressure);
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    /**
     * Adds rows from one thread at a time. Must be closed once it adds no more rows.
     */
    public final class Producer implements Releasable {
        // Guarded by this. The writer takes it to take back or drop the batch.
        @Nullable
        private Chunk open;
        private boolean closed;

        private Producer() {}

        /**
         * Adds {@code rows} and sends what may go. With {@code newBulk} they start a new batch, so that a bulk never holds the
         * same document twice. Callers check {@link #hasCapacity()} first, so that a producer goes at most one page over a limit.
         */
        public void add(Rows rows, boolean newBulk) {
            boolean tripped;
            try {
                tripped = append(rows, newBulk);
            } catch (Exception e) {
                // outside this producer's monitor, since failing and sending take other monitors
                fail(e);
                return;
            }
            if (tripped) {
                takeBack();
            } else {
                giveBackIfIdle();
            }
            flush();
        }

        /**
         * Appends {@code rows} and returns whether they tripped the breaker or indexing pressure.
         */
        private synchronized boolean append(Rows rows, boolean newBulk) throws IOException {
            assert closed == false : "rows added to a closed producer";
            if (failure != null) {
                drop();
                return false;
            }
            if (rows.count() == 0) {
                return false;
            }
            if (newBulk) {
                giveBack();
            }
            if (open == null) {
                open = new Chunk();
            }
            Chunk chunk = open;
            List<IndexRequest> requests = new ArrayList<>(rows.count());
            long requestBytes = 0;
            boolean tripped;
            try {
                rows.append(chunk.builder);
                for (int i = 0; i < rows.count(); i++) {
                    IndexRequest request = rows.request(i);
                    assert index.equals(request.index()) : "row for [" + request.index() + "] added to a writer of [" + index + "]";
                    requests.add(request);
                    requestBytes += request.ramBytesUsed();
                }
                // promoting a column can shrink the builder: the chunk keeps what it charged until its bulk is acknowledged
                long builderBytes = Math.max(chunk.builderBytes, chunk.builder.ramBytesUsed());
                tripped = charge(chunk, requests.size(), builderBytes - chunk.builderBytes + requestBytes);
                chunk.builderBytes = builderBytes;
            } catch (Exception e) {
                // the rows may be partly in the batch, so it goes with them
                drop();
                throw e;
            }
            chunk.requests.addAll(requests);
            chunk.requestBytes += requestBytes;
            chunk.tripped |= tripped;
            if (chunk.tripped || chunk.bytes() >= policy.targetBulkBytes()) {
                giveBack();
            }
            return tripped;
        }

        /**
         * Whether this producer may add rows. If not, {@code onCapacity} is called when that may have changed. It may not while
         * a full batch waits for a free slot, or while rows that tripped the breaker or indexing pressure are not acknowledged.
         * Once the writer failed it always may, since the rows are dropped. It only takes short locks.
         */
        public boolean hasCapacity() {
            return EscfBulkWriter.this.hasCapacity();
        }

        @Override
        public void close() {
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                giveBack();
            }
            producerClosed(this);
        }

        private synchronized void giveBackIfIdle() {
            boolean idle;
            synchronized (EscfBulkWriter.this) {
                idle = bulksInFlight == 0;
            }
            if (idle) {
                giveBack();
            }
        }

        private synchronized void giveBack() {
            if (open != null) {
                handOver(open);
                open = null;
            }
        }

        private synchronized void drop() {
            if (open != null) {
                long bytes = open.bytes();
                Releasable pressure = open.releasePressure();
                open.close();
                open = null;
                release(bytes, pressure);
            }
        }
    }

    private static final String BREAKER_LABEL = "escf_bulk_writer";

    private final EscfBulkService service;
    private final Client client;
    private final Supplier<ThreadContext.StoredContext> context;
    // what indexing pressure counts for the context each bulk carries
    private final long contextBytes;
    private final CircuitBreaker breaker;
    private final Recycler<BytesRef> recycler;
    private final String index;
    private final Policy policy;
    private final ResponseHandler onResponse;
    private final Runnable onCapacity;
    // the bytes charged for the rows this writer holds
    private final AtomicLong charged = new AtomicLong();

    // Guarded by this
    private final List<Producer> producers = new ArrayList<>();
    // the batches given by the producers, in order
    private final ArrayDeque<Chunk> chunks = new ArrayDeque<>();
    private int bulksInFlight;
    // the batches whose rows tripped the breaker or indexing pressure, given to the writer and not settled
    private int tripped;
    @Nullable
    private ActionListener<Void> onDone;
    private boolean done;

    private volatile Exception failure;

    EscfBulkWriter(
        EscfBulkService service,
        Client client,
        CircuitBreaker breaker,
        Recycler<BytesRef> recycler,
        String index,
        Policy policy,
        ResponseHandler onResponse,
        Runnable onCapacity
    ) {
        this.service = service;
        this.client = client;
        this.context = client.threadPool().getThreadContext().newRestorableContext(false);
        this.contextBytes = client.threadPool().getThreadContext().estimatedRequestContextBytes();
        this.breaker = breaker;
        this.recycler = recycler;
        this.index = index;
        this.policy = policy;
        this.onResponse = onResponse;
        this.onCapacity = onCapacity;
    }

    public synchronized Producer newProducer() {
        assert done == false : "producer created after the writer is done";
        Producer producer = new Producer();
        producers.add(producer);
        return producer;
    }

    /**
     * Completes {@code listener} once every producer is closed and every bulk has settled, with the first failure if any. Create
     * the producers first: a writer with none completes right away.
     */
    public void finish(ActionListener<Void> listener) {
        synchronized (this) {
            if (onDone != null || done) {
                throw new IllegalStateException("writer for [" + index + "] already finished");
            }
            onDone = listener;
        }
        maybeDone();
    }

    /**
     * Drops the rows not sent yet, for a task that failed or was cancelled. The bulks already sent still index their rows, and a
     * pending {@link #finish} waits for them and for every producer to close. Does nothing once the writer is done.
     */
    @Override
    public void close() {
        fail(new IllegalStateException("writer of [" + index + "] closed"));
        maybeDone();
    }

    /**
     * See {@link Producer#hasCapacity()}.
     */
    synchronized boolean hasCapacity() {
        if (failure != null) {
            return true;
        }
        // otherwise a bulk of this writer settles first
        return tripped == 0 && (chunks.isEmpty() || bulksInFlight < policy.maxBulksInFlight());
    }

    @Nullable
    public Exception failure() {
        return failure;
    }

    /**
     * Charges {@code bytes} for {@code operations} rows of {@code chunk} to the breaker and to indexing pressure, and returns
     * whether either tripped. The rows are in memory already, so a trip still charges them while the writer holds other rows,
     * since their bulks free memory when they settle. A trip while it holds none throws: waiting would free nothing.
     */
    private boolean charge(Chunk chunk, int operations, long bytes) {
        // first, so that a producer that trips on these bytes sees that the writer holds rows
        charged.addAndGet(bytes);
        boolean tripped;
        try {
            tripped = chargeOrForce(
                bytes,
                CircuitBreakingException.class,
                () -> breaker.addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL),
                () -> breaker.addWithoutBreaking(bytes)
            );
        } catch (RuntimeException e) {
            charged.addAndGet(-bytes);
            throw e;
        }
        long pressureBytes = chunk.contextCounted ? bytes : bytes + contextBytes;
        try {
            tripped |= chargeOrForce(
                bytes,
                EsRejectedExecutionException.class,
                () -> chunk.pressure.increment(operations, pressureBytes),
                () -> chunk.forcedPressure().increment(operations, pressureBytes)
            );
        } catch (RuntimeException e) {
            breaker.addWithoutBreaking(-bytes);
            charged.addAndGet(-bytes);
            throw e;
        }
        chunk.contextCounted = true;
        return tripped;
    }

    /**
     * Runs {@code charge} and returns whether it tripped with {@code trip}, in which case {@code force} runs while the writer
     * holds other rows. A trip that finds none retries once, since it may have been on rows released since: a release frees its
     * memory before the writer's count drops.
     */
    private boolean chargeOrForce(long bytes, Class<? extends RuntimeException> trip, Runnable charge, Runnable force) {
        for (boolean retried = false;; retried = true) {
            try {
                charge.run();
                return false;
            } catch (RuntimeException e) {
                if (trip.isInstance(e) == false) {
                    throw e;
                }
                if (holdsOtherRows(bytes)) {
                    force.run();
                    return true;
                }
                if (retried) {
                    throw e;
                }
            }
        }
    }

    private boolean holdsOtherRows(long bytes) {
        return charged.get() - bytes > 0;
    }

    private void release(long bytes, Releasable pressure) {
        // the memory first, so that a producer that sees the count drop finds it free
        pressure.close();
        if (bytes > 0) {
            breaker.addWithoutBreaking(-bytes);
            charged.addAndGet(-bytes);
        }
    }

    private void handOver(Chunk chunk) {
        boolean dropped;
        synchronized (this) {
            dropped = failure != null;
            if (dropped == false) {
                chunks.add(chunk);
                if (chunk.tripped) {
                    tripped++;
                }
            }
        }
        if (dropped) {
            long bytes = chunk.bytes();
            Releasable pressure = chunk.releasePressure();
            chunk.close();
            release(bytes, pressure);
        }
    }

    private void producerClosed(Producer producer) {
        synchronized (this) {
            producers.remove(producer);
        }
        flush();
        maybeDone();
    }

    /**
     * Sends the batches that may go.
     */
    private void flush() {
        while (true) {
            Chunk chunk;
            synchronized (this) {
                if (chunks.isEmpty() || failure != null || bulksInFlight >= policy.maxBulksInFlight()) {
                    return;
                }
                chunk = chunks.poll();
                bulksInFlight++;
            }
            send(chunk);
        }
    }

    private void send(Chunk chunk) {
        long bytes = chunk.bytes();
        Releasable pressure = chunk.releasePressure();
        boolean tripped = chunk.tripped;
        ActionListener<BulkResponse> listener = ActionListener.notifyOnce(new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse response) {
                onBulkDone(bytes, pressure, tripped, response, null);
            }

            @Override
            public void onFailure(Exception e) {
                onBulkDone(bytes, pressure, tripped, null, e);
            }
        });
        BulkRequest bulk = new BulkRequest();
        // the rows are counted in indexing pressure already
        bulk.incrementalState(new BulkRequest.IncrementalState(Map.of(), true));
        try (chunk) {
            EscfBatch batch = chunk.builder.buildPartition(0);
            // the bulk owns the batch from here on
            bulk.setPreBuiltBatches(Map.of(index, batch));
            for (int i = 0; i < chunk.requests.size(); i++) {
                IndexRequest request = chunk.requests.get(i);
                request.indexSource().setSourceRow(batch, i, XContentType.JSON);
                bulk.add(request);
            }
        } catch (Exception e) {
            bulk.releasePreBuiltBatches();
            listener.onFailure(e);
            return;
        }
        try (var ignored = context.get()) {
            // the bulk action does not release the batch of a bulk it rejects before taking it
            ActionListener.run(ActionListener.runBefore(listener, bulk::releasePreBuiltBatches), l -> client.bulk(bulk, l));
        }
    }

    /**
     * Settles a bulk on the service's executor, so that the thread that completed it never waits on a producer.
     */
    private void onBulkDone(long bytes, Releasable pressure, boolean wasTripped, @Nullable BulkResponse response, @Nullable Exception e) {
        service.executeOrRun(() -> {
            try (var ignored = context.get()) {
                Exception failure = e != null ? e : readResponse(response);
                if (failure != null) {
                    fail(failure);
                }
                release(bytes, pressure);
            } finally {
                // after failing and releasing, so that finish sees the failure and nothing charged
                synchronized (this) {
                    bulksInFlight--;
                    if (wasTripped) {
                        tripped--;
                    }
                }
            }
            wake();
        });
    }

    /**
     * Calls the producers back, takes their batches back if nothing is in flight, and sends what may go.
     */
    private void wake() {
        synchronized (this) {
            if (done) {
                return;
            }
        }
        try (var ignored = context.get()) {
            try {
                onCapacity.run();
            } catch (Exception e) {
                fail(e);
            }
            takeBackIfIdle();
            flush();
        } catch (Exception e) {
            fail(e);
        }
        maybeDone();
    }

    /**
     * Takes back the producers' batches when nothing is in flight, so that a producer that stopped adding holds no memory.
     */
    private void takeBackIfIdle() {
        synchronized (this) {
            if (chunks.isEmpty() == false || bulksInFlight > 0) {
                return;
            }
        }
        takeBack();
    }

    /**
     * Takes back the producers' batches, to send them as slots free up.
     */
    private void takeBack() {
        List<Producer> holding;
        synchronized (this) {
            if (failure != null) {
                return;
            }
            holding = new ArrayList<>(producers);
        }
        for (Producer producer : holding) {
            producer.giveBack();
        }
    }

    private void maybeDone() {
        ActionListener<Void> listener;
        Exception e;
        synchronized (this) {
            if (onDone == null || producers.isEmpty() == false || bulksInFlight > 0 || chunks.isEmpty() == false) {
                return;
            }
            listener = onDone;
            onDone = null;
            done = true;
            e = failure;
        }
        try (var ignored = context.get()) {
            if (e == null) {
                listener.onResponse(null);
            } else {
                listener.onFailure(e);
            }
        }
    }

    /**
     * Records the failure, drops every batch not sent and calls the producers back, so that those waiting go on. Must not be
     * called under a producer's monitor.
     */
    private void fail(Exception e) {
        long dropped = 0;
        List<Releasable> pressure = new ArrayList<>();
        List<Producer> holding;
        synchronized (this) {
            if (done || failure != null) {
                return;
            }
            failure = e;
            for (Chunk chunk : chunks) {
                dropped += chunk.bytes();
                pressure.add(chunk.releasePressure());
                if (chunk.tripped) {
                    tripped--;
                }
                chunk.close();
            }
            chunks.clear();
            // under the lock that maybeDone checks, so that finish never completes while these rows are still charged
            release(dropped, () -> Releasables.close(pressure));
            holding = new ArrayList<>(producers);
        }
        for (Producer producer : holding) {
            producer.drop();
        }
        service.execute(this::wake);
    }

    @Nullable
    private Exception readResponse(BulkResponse response) {
        try {
            return onResponse.onResponse(response);
        } catch (Exception e) {
            return e;
        }
    }

    @Nullable
    private static Exception firstFailure(BulkResponse response) {
        if (response.hasFailures() == false) {
            return null;
        }
        for (BulkItemResponse item : response.getItems()) {
            if (item.isFailed()) {
                return item.getFailure().getCause();
            }
        }
        return null;
    }
}
