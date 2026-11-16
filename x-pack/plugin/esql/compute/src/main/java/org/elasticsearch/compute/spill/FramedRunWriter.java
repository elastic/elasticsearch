/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.spill.spi.SpillObject;
import org.elasticsearch.compute.spill.spi.SpillScope;
import org.elasticsearch.compute.spill.spi.SpillWriter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Writes one framed run: records are serialized on the codec pool, one at a time and in order, into the chunked
 * stream of {@link ChunkingOutputStream}, with the index kept in memory and charged, and written with the footer at
 * finish. The caller never waits: {@link #append} queues the record and its listener completes, on the codec thread,
 * once the record's bytes are in the stream, which is when the caller may drop its own reference to the source.
 * <p>
 * The chunk buffers are charged to {@code breaker} when the writer is created, which is the headroom a spilling operator
 * acquires lazily: a writer that exists never allocates under pressure. The index is bounded by the maxima and charged
 * as it grows. A failure, a cancellation or an oversized record poisons the writer: every later append and the finish
 * fail, the backend writer is aborted and the buffers and the index charge go back.
 */
public final class FramedRunWriter {

    /**
     * @param kind one of the layout's kinds
     * @param chunkSize payload bytes per chunk
     * @param depth chunk buffers charged up front; the serialized bytes in flight never exceed depth times chunk size
     */
    public record Options(int kind, int chunkSize, int depth, FramedRunMaxima maxima) {
        public Options {
            if (chunkSize <= 0 || chunkSize > maxima.chunkBytes()) {
                throw new IllegalArgumentException("chunk size [" + chunkSize + "] outside (0, " + maxima.chunkBytes() + "]");
            }
            if (depth <= 0) {
                throw new IllegalArgumentException("depth [" + depth + "] must be positive");
            }
        }
    }

    private record Pending(
        int partition,
        @Nullable CheckedConsumer<StreamOutput, IOException> payload,
        @Nullable Releasable onDone,
        @Nullable ActionListener<Long> recordListener,
        @Nullable ActionListener<FramedRun> finishListener
    ) {
        boolean isFinish() {
            return finishListener != null;
        }

        void fail(Exception e) {
            Releasables.closeExpectNoException(onDone);
            if (finishListener != null) {
                finishListener.onFailure(e);
            } else {
                recordListener.onFailure(e);
            }
        }
    }

    private static final Logger logger = LogManager.getLogger(FramedRunWriter.class);

    private final String name;
    private final SpillWriter writer;
    private final long createdNanos = System.nanoTime();
    private final CircuitBreaker breaker;
    private final Executor codecExecutor;
    private final Options options;
    private final TransportVersion transportVersion;
    private final ChunkBuffers buffers;
    private final ChunkingOutputStream out;
    private final OutputStreamStreamOutput stream;

    private final ArrayDeque<Pending> queue = new ArrayDeque<>();
    private final AtomicBoolean draining = new AtomicBoolean();
    private final AtomicBoolean released = new AtomicBoolean();
    private final Object indexLock = new Object();
    private volatile Exception failure;
    private volatile boolean finished;
    private final AtomicBoolean finishRequested = new AtomicBoolean();
    private boolean headerWritten;

    // the index; codec thread only
    private long[] offsets = new long[64];
    private int[] lengths = new int[64];
    private int[] partitions = new int[64];
    private volatile int records;
    private int maxPartition = -1;
    private long indexCharged;

    private FramedRunWriter(
        String name,
        SpillWriter writer,
        CircuitBreaker breaker,
        Executor codecExecutor,
        Options options,
        ChunkBuffers buffers
    ) {
        this.name = name;
        this.writer = writer;
        this.breaker = breaker;
        this.codecExecutor = codecExecutor;
        this.options = options;
        this.transportVersion = TransportVersion.current();
        this.buffers = buffers;
        this.out = new ChunkingOutputStream(buffers, writer, true);
        this.stream = new OutputStreamStreamOutput(out);
        this.stream.setTransportVersion(transportVersion);
    }

    /**
     * Creates the backend writer and charges the chunk buffers; the listener gets the run writer, or the failure with
     * nothing retained.
     *
     * @param breaker the query's root view, since the writer's buffers and index outlive the driver's turn and its
     *                callbacks run on the codec pool
     */
    public static void create(
        SpillScope scope,
        String name,
        CircuitBreaker breaker,
        Executor codecExecutor,
        Options options,
        ActionListener<FramedRunWriter> listener
    ) {
        ChunkBuffers buffers;
        try {
            buffers = new ChunkBuffers(breaker, options.chunkSize() + FramedRunLayout.CHUNK_TRAILER_BYTES, options.depth());
        } catch (CircuitBreakingException e) {
            listener.onFailure(e);
            return;
        }
        scope.createWriter(name, ActionListener.wrap(writer -> {
            FramedRunWriter runWriter;
            try {
                runWriter = new FramedRunWriter(name, writer, breaker, codecExecutor, options, buffers);
            } catch (RuntimeException e) {
                buffers.close();
                writer.abort(ActionListener.noop());
                listener.onFailure(e);
                return;
            }
            listener.onResponse(runWriter);
        }, e -> {
            buffers.close();
            listener.onFailure(e);
        }));
    }

    public String name() {
        return name;
    }

    public TransportVersion transportVersion() {
        return transportVersion;
    }

    /**
     * Records written so far, a progress indication from any thread.
     */
    public int records() {
        return records;
    }

    /**
     * Payload bytes handed to the store so far, in whole chunks: the status figure of a run still being written.
     */
    public long bytesWritten() {
        return out.bytesWritten();
    }

    @Nullable
    public Exception failure() {
        return failure;
    }

    /**
     * Queues one record. {@code payload} runs on the codec thread with a stream whose transport version is this writer's;
     * {@code onDone} is released right after it, success or failure; the listener gets the payload's logical offset in
     * the run. A record larger than the maxima fails and poisons the writer. Appends and the finish are issued by one
     * thread, the operator's; they are not serialized against each other here.
     */
    public void append(
        int partition,
        CheckedConsumer<StreamOutput, IOException> payload,
        @Nullable Releasable onDone,
        ActionListener<Long> listener
    ) {
        if (partition < 0) {
            Releasables.close(onDone);
            listener.onFailure(new IllegalArgumentException("partition [" + partition + "] must not be negative"));
            return;
        }
        if (finishRequested.get()) {
            // after the finish nothing may touch the buffers, which the finish closes; a late record would wait on them forever
            Releasables.close(onDone);
            listener.onFailure(new IllegalStateException("spill run [" + name + "] is finished"));
            return;
        }
        enqueue(new Pending(partition, payload, onDone, listener, null));
    }

    /**
     * Writes the index and the footer after every queued record and publishes the run. Nothing may be appended after it.
     */
    public void finish(ActionListener<FramedRun> listener) {
        if (finishRequested.compareAndSet(false, true) == false) {
            listener.onFailure(new IllegalStateException("spill run [" + name + "] finish already requested"));
            return;
        }
        enqueue(new Pending(-1, null, null, null, listener));
    }

    /**
     * Fails the writer at its next chunk boundary, fails every queued record and discards what was written; safe from
     * any thread, idempotent, and the listener completes once the backend has let go.
     */
    public void abort(ActionListener<Void> listener) {
        fail(new TaskCancelledException("spill run [" + name + "] aborted"));
        writer.abort(ActionListener.runBefore(listener, this::release));
    }

    private void enqueue(Pending pending) {
        Exception failed = failure;
        if (failed != null) {
            pending.fail(failedEarlier(failed));
            return;
        }
        synchronized (queue) {
            queue.addLast(pending);
        }
        drain();
    }

    private IllegalStateException failedEarlier(Exception cause) {
        return new IllegalStateException("spill run [" + name + "] failed earlier", cause);
    }

    private void drain() {
        if (draining.compareAndSet(false, true) == false) {
            return;
        }
        AbstractRunnable task = new AbstractRunnable() {
            @Override
            protected void doRun() {
                drainQueue();
            }

            @Override
            public void onRejection(Exception e) {
                draining.set(false);
                fail(e);
            }

            @Override
            public void onFailure(Exception e) {
                draining.set(false);
                fail(e);
            }
        };
        try {
            codecExecutor.execute(task);
        } catch (RuntimeException e) {
            task.onRejection(e);
        }
    }

    /**
     * Runs on the codec thread: records in order, the finish last; re-checks the queue after releasing the drain flag so
     * that an append racing the exit is not left behind.
     */
    private void drainQueue() {
        while (true) {
            Pending pending;
            synchronized (queue) {
                pending = queue.pollFirst();
            }
            if (pending == null) {
                draining.set(false);
                synchronized (queue) {
                    if (queue.isEmpty() || draining.compareAndSet(false, true) == false) {
                        return;
                    }
                }
                continue;
            }
            Exception failed = failure;
            if (failed != null) {
                pending.fail(failedEarlier(failed));
            } else if (pending.isFinish()) {
                doFinish(pending.finishListener());
            } else {
                doRecord(pending);
            }
        }
    }

    private void writeHeaderOnce() throws IOException {
        if (headerWritten == false) {
            out.appendRaw(header().encode());
            headerWritten = true;
        }
    }

    private FramedRunLayout.Header header() {
        return new FramedRunLayout.Header(FramedRunLayout.FORMAT_VERSION, options.kind(), transportVersion.id(), options.chunkSize());
    }

    private void doRecord(Pending pending) {
        long offset;
        try {
            writeHeaderOnce();
            if (records >= options.maxima().indexEntries()) {
                throw new IllegalStateException("spill run [" + name + "] holds [" + records + "] records, the maximum");
            }
            stream.writeVInt(pending.partition());
            offset = out.logicalPosition();
            pending.payload().accept(stream);
            long length = out.logicalPosition() - offset;
            if (length > options.maxima().recordBytes()) {
                throw new IllegalStateException(
                    "spill run ["
                        + name
                        + "] record of ["
                        + length
                        + "] bytes exceeds the maximum of ["
                        + options.maxima().recordBytes()
                        + "]"
                );
            }
            index(pending.partition(), offset, (int) length);
        } catch (Exception e) {
            fail(e);
            pending.fail(e);
            return;
        }
        Releasables.closeExpectNoException(pending.onDone());
        pending.recordListener().onResponse(offset);
    }

    private void index(int partition, long offset, int length) {
        if (partition >= options.maxima().partitions()) {
            throw new IllegalStateException("spill run [" + name + "] partition [" + partition + "] exceeds the maximum");
        }
        if (offsets == null) {
            // the index went with the seal: an append after finish is the caller's breach, named rather than a null
            throw new IllegalStateException("spill run [" + name + "] indexed after its finish");
        }
        if (records == offsets.length) {
            int grown = Math.toIntExact(Math.max(records + 1L, Math.min(offsets.length * 2L, options.maxima().indexEntries())));
            long add = (long) (grown - offsets.length) * FramedRunLayout.INDEX_ENTRY_BYTES;
            // the charge and its return in release() take the same lock, since an abort may release from another thread
            synchronized (indexLock) {
                if (released.get()) {
                    throw failedEarlier(failure == null ? new IllegalStateException("released") : failure);
                }
                breaker.addEstimateBytesAndMaybeBreak(add, "spill run index");
                indexCharged += add;
            }
            offsets = Arrays.copyOf(offsets, grown);
            lengths = Arrays.copyOf(lengths, grown);
            partitions = Arrays.copyOf(partitions, grown);
        }
        offsets[records] = offset;
        lengths[records] = length;
        partitions[records] = partition;
        maxPartition = Math.max(maxPartition, partition);
        records = records + 1;
    }

    private void doFinish(ActionListener<FramedRun> listener) {
        try {
            writeHeaderOnce();
            long logicalBytes = out.finish();
            long indexOffset = FramedRunLayout.HEADER_BYTES + FramedRunLayout.physicalBodyLength(logicalBytes, options.chunkSize());
            int partitionCount = maxPartition + 1;
            int indexBytes = Math.toIntExact(
                (long) partitionCount * FramedRunLayout.DIRECTORY_ENTRY_BYTES + (long) records * FramedRunLayout.INDEX_ENTRY_BYTES
                    + FramedRunLayout.INDEX_CRC_BYTES
            );
            // the encoded index is one array; charged before it exists, returned once it has been appended
            breaker.addEstimateBytesAndMaybeBreak(indexBytes, "spill run index encoding");
            try {
                out.appendRaw(encodeIndex(partitionCount, indexBytes));
            } finally {
                breaker.addWithoutBreaking(-indexBytes);
            }
            dropIndex();
            FramedRunLayout.Footer footer = new FramedRunLayout.Footer(records, logicalBytes, indexOffset, partitionCount);
            out.appendRaw(footer.encode());
            FramedRunLayout.Header header = header();
            writer.finish(new ActionListener<>() {
                @Override
                public void onResponse(SpillObject object) {
                    finished = true;
                    release();
                    // the one line a spill event leaves: never row data, the run's name carries the query's id
                    logger.debug(
                        () -> Strings.format(
                            "spill run [%s] sealed: [%d] records, [%d] bytes, [%d] partitions, [%s] since its writer was created",
                            name,
                            footer.records(),
                            footer.logicalBytes(),
                            partitionCount,
                            TimeValue.timeValueNanos(System.nanoTime() - createdNanos)
                        )
                    );
                    listener.onResponse(new FramedRun(name, object, header, footer));
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            fail(e);
            listener.onFailure(e);
        }
    }

    /**
     * The index grouped by partition: a directory of (first entry, count) per partition, then the entries, each
     * (partition, offset, length), then a CRC over all of it. A counting sort, since partitions are small integers.
     */
    private byte[] encodeIndex(int partitionCount, int indexBytes) {
        long[] counts = new long[partitionCount];
        for (int i = 0; i < records; i++) {
            counts[partitions[i]]++;
        }
        long[] first = new long[partitionCount];
        long running = 0;
        for (int p = 0; p < partitionCount; p++) {
            first[p] = running;
            running += counts[p];
        }
        int directoryBytes = partitionCount * FramedRunLayout.DIRECTORY_ENTRY_BYTES;
        int entriesBytes = records * FramedRunLayout.INDEX_ENTRY_BYTES;
        byte[] bytes = new byte[indexBytes];
        for (int p = 0; p < partitionCount; p++) {
            ByteUtils.writeLongLE(first[p], bytes, p * FramedRunLayout.DIRECTORY_ENTRY_BYTES);
            ByteUtils.writeLongLE(counts[p], bytes, p * FramedRunLayout.DIRECTORY_ENTRY_BYTES + 8);
        }
        long[] next = first.clone();
        for (int i = 0; i < records; i++) {
            int p = partitions[i];
            int at = directoryBytes + Math.toIntExact(next[p]++) * FramedRunLayout.INDEX_ENTRY_BYTES;
            ByteUtils.writeIntLE(p, bytes, at);
            ByteUtils.writeLongLE(offsets[i], bytes, at + 4);
            ByteUtils.writeIntLE(lengths[i], bytes, at + 12);
        }
        ByteUtils.writeIntLE(FramedRunLayout.crc32(bytes, 0, directoryBytes + entriesBytes), bytes, directoryBytes + entriesBytes);
        return bytes;
    }

    /**
     * Poisons the writer: wakes a waiting codec thread, fails what is queued, aborts the backend writer and returns
     * everything the writer holds. Idempotent; the first failure is the one reported.
     */
    private void fail(Exception e) {
        if (finished) {
            // a published run cannot fail; an abort after it is the SPI's no-op
            return;
        }
        if (failure == null) {
            failure = e;
        }
        out.cancel();
        List<Pending> dropped = new ArrayList<>();
        synchronized (queue) {
            dropped.addAll(queue);
            queue.clear();
        }
        for (Pending pending : dropped) {
            pending.fail(failedEarlier(e));
        }
        writer.abort(ActionListener.running(this::release));
    }

    /**
     * Drops the index arrays with their charge once the index is in the run: nothing reads them afterwards, the reader
     * loads the index from the object, and a sealed writer may be kept through a whole replay. Codec thread only, like
     * the arrays; the charge's return takes the lock an abort's release takes.
     */
    private void dropIndex() {
        offsets = null;
        lengths = null;
        partitions = null;
        synchronized (indexLock) {
            breaker.addWithoutBreaking(-indexCharged);
            indexCharged = 0;
        }
    }

    /**
     * The bytes the index arrays hold, for tests: zero once the run is sealed. Read off the codec thread, after the
     * appends and the finish the test awaited, whose completions order the writes before this read.
     */
    long indexBytesRetained() {
        long[] held = offsets;
        return held == null ? 0 : (long) held.length * FramedRunLayout.INDEX_ENTRY_BYTES;
    }

    /**
     * Returns the buffers and the index charge; once, from whichever thread ends the writer. The stream is not touched:
     * the codec thread may still be inside it after an abort, and the buffers' charge goes back regardless.
     */
    private void release() {
        synchronized (indexLock) {
            if (released.compareAndSet(false, true)) {
                buffers.close();
                breaker.addWithoutBreaking(-indexCharged);
                indexCharged = 0;
            }
        }
    }
}
