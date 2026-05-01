/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinates parallel parsing of a sequential (non-seekable) decompressed stream
 * by chunking it at record boundaries and dispatching each chunk to a parser thread.
 * <p>
 * This is the streaming counterpart to {@link ParallelParsingCoordinator}, designed for
 * stream-only compression codecs (gzip, zstd without seekable frames) where the
 * decompressed data is only available as a sequential {@link InputStream}.
 * <p>
 * Architecture:
 * <ol>
 *   <li>A segmentator thread reads the decompressed stream into byte buffers from a pool</li>
 *   <li>Each buffer is split at the last record boundary (newline) to form a complete chunk</li>
 *   <li>Chunks are dispatched to parser threads via a bounded queue</li>
 *   <li>Parser threads parse each chunk independently using the format reader</li>
 *   <li>Results are yielded in chunk order via per-slot page queues</li>
 * </ol>
 */
public final class StreamingParallelParsingCoordinator {

    private static final Logger logger = LogManager.getLogger(StreamingParallelParsingCoordinator.class);

    private StreamingParallelParsingCoordinator() {}

    /**
     * Creates a parallel-parsing iterator over a sequential decompressed stream.
     *
     * @param reader              the segmentable format reader (provides record boundary semantics)
     * @param decompressedStream  the sequential decompressed input stream
     * @param projectedColumns    columns to project
     * @param batchSize           rows per page
     * @param parallelism         number of parallel parser threads
     * @param executor            executor for segmentator and parser threads
     * @param errorPolicy         error handling policy
     * @return an iterator that yields pages in chunk order
     */
    public static CloseableIterator<Page> parallelRead(
        SegmentableFormatReader reader,
        InputStream decompressedStream,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor,
        ErrorPolicy errorPolicy
    ) throws IOException {
        ErrorPolicy effectivePolicy = errorPolicy != null ? errorPolicy : ErrorPolicy.STRICT;

        if (parallelism <= 1) {
            FormatReadContext ctx = FormatReadContext.builder()
                .projectedColumns(projectedColumns)
                .batchSize(batchSize)
                .errorPolicy(effectivePolicy)
                .build();
            return reader.read(new InputStreamStorageObject(decompressedStream), ctx);
        }

        return new StreamingParallelIterator(
            reader,
            decompressedStream,
            projectedColumns,
            batchSize,
            parallelism,
            executor,
            effectivePolicy
        );
    }

    private static final class StreamingParallelIterator implements CloseableIterator<Page> {

        private static final Page POISON = new Page(0);
        private static final long CLOSE_TIMEOUT_SECONDS = 60;

        /**
         * The reader used by parser threads. Initially the caller-supplied reader; the segmentator
         * thread replaces it with a schema-bound variant after inferring the schema from the first
         * chunk (see {@link #runSegmentator}). The replacement happens-before the first chunk is
         * dispatched (via the {@link ArrayBlockingQueue} synchronization on {@link #chunkQueue}),
         * so parser threads always observe the schema-bound reader by the time they pick up work.
         */
        private volatile SegmentableFormatReader reader;
        private final List<String> projectedColumns;
        private final int batchSize;
        private final ErrorPolicy errorPolicy;

        private final ArrayBlockingQueue<byte[]> bufferPool;
        private final ArrayBlockingQueue<Chunk> chunkQueue;
        private final int windowSize;
        private final int chunkSize;
        private final ArrayBlockingQueue<Page>[] pageQueues;

        private final AtomicReference<Throwable> firstError = new AtomicReference<>();
        private final CountDownLatch allDone;
        private final AtomicInteger chunksDispatched = new AtomicInteger();
        /**
         * Bounds the gap between dispatched and consumed chunks. {@link #pageQueues} is indexed by
         * {@code chunk.index % windowSize}; without this semaphore a fast parser could recycle a
         * buffer (and a slow segmentator could dispatch a new chunk) before the consumer drained
         * the previous chunk's slot, interleaving pages from two generations into the same queue.
         * Acquired by the segmentator before {@link #dispatchChunk}, released by the consumer when
         * a chunk's POISON has been observed.
         */
        private final Semaphore consumerSlots;

        private int currentChunk = 0;
        private Page buffered = null;
        private volatile boolean closed = false;

        StreamingParallelIterator(
            SegmentableFormatReader reader,
            InputStream decompressedStream,
            List<String> projectedColumns,
            int batchSize,
            int parallelism,
            Executor executor,
            ErrorPolicy errorPolicy
        ) {
            this.reader = reader;
            this.projectedColumns = projectedColumns;
            this.batchSize = batchSize;
            this.errorPolicy = errorPolicy;
            this.windowSize = parallelism + 1;

            this.chunkSize = Math.toIntExact(reader.minimumSegmentSize());

            this.bufferPool = new ArrayBlockingQueue<>(windowSize);
            for (int i = 0; i < windowSize; i++) {
                bufferPool.add(new byte[chunkSize]);
            }

            this.chunkQueue = new ArrayBlockingQueue<>(parallelism);
            this.consumerSlots = new Semaphore(windowSize);

            @SuppressWarnings("unchecked")
            ArrayBlockingQueue<Page>[] queues = new ArrayBlockingQueue[windowSize];
            this.pageQueues = queues;
            for (int i = 0; i < windowSize; i++) {
                pageQueues[i] = new ArrayBlockingQueue<>(16);
            }

            // 1 segmentator + N parsers
            this.allDone = new CountDownLatch(1 + parallelism);

            try {
                executor.execute(() -> runSegmentator(decompressedStream, this.chunkSize));
            } catch (RejectedExecutionException e) {
                firstError.compareAndSet(null, e);
                allDone.countDown();
            }

            for (int i = 0; i < parallelism; i++) {
                try {
                    executor.execute(this::runParser);
                } catch (RejectedExecutionException e) {
                    firstError.compareAndSet(null, e);
                    allDone.countDown();
                }
            }
        }

        private void runSegmentator(InputStream stream, int chunkSize) {
            byte[] carry = null;
            int carryLen = 0;
            int chunkIndex = 0;

            try {
                while (closed == false && firstError.get() == null) {
                    byte[] buf;
                    try {
                        buf = bufferPool.take();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    int offset = 0;
                    if (carry != null) {
                        System.arraycopy(carry, 0, buf, 0, carryLen);
                        offset = carryLen;
                        carry = null;
                        carryLen = 0;
                    }

                    int bytesRead = readFromStream(stream, buf, offset, buf.length - offset);
                    if (bytesRead <= 0 && offset == 0) {
                        recycleBuffer(buf);
                        break;
                    }

                    int totalBytes = offset + Math.max(bytesRead, 0);
                    boolean isEof = bytesRead < 0 || totalBytes < buf.length;

                    int lastNewline = findLastNewline(buf, totalBytes);

                    if (lastNewline < 0) {
                        if (isEof) {
                            if (chunkIndex == 0) {
                                bindSchemaFromFirstChunk(buf, totalBytes);
                            }
                            dispatchChunk(chunkIndex++, buf, totalBytes, true);
                        } else {
                            // Single record larger than chunk size — grow a temporary buffer
                            byte[] grown = growUntilNewline(stream, buf, totalBytes, chunkSize);
                            int grownNewline = findLastNewline(grown, grown.length);
                            if (grownNewline < 0) {
                                if (chunkIndex == 0) {
                                    bindSchemaFromFirstChunk(grown, grown.length);
                                }
                                dispatchChunk(chunkIndex++, grown, grown.length, true);
                            } else {
                                int validLen = grownNewline + 1;
                                carryLen = grown.length - validLen;
                                carry = new byte[carryLen];
                                System.arraycopy(grown, validLen, carry, 0, carryLen);
                                if (chunkIndex == 0) {
                                    bindSchemaFromFirstChunk(grown, validLen);
                                }
                                dispatchChunk(chunkIndex++, grown, validLen, false);
                            }
                            // The original buffer was consumed by grow; don't recycle it
                        }
                        if (isEof) break;
                        continue;
                    }

                    int validLen = isEof ? totalBytes : lastNewline + 1;

                    if (isEof == false && validLen < totalBytes) {
                        carryLen = totalBytes - validLen;
                        carry = new byte[carryLen];
                        System.arraycopy(buf, validLen, carry, 0, carryLen);
                    }

                    if (chunkIndex == 0) {
                        bindSchemaFromFirstChunk(buf, validLen);
                    }
                    dispatchChunk(chunkIndex++, buf, validLen, isEof);
                    if (isEof) break;
                }
            } catch (Exception e) {
                firstError.compareAndSet(null, e);
            } finally {
                try {
                    stream.close();
                } catch (IOException ignored) {}

                int parserCount = windowSize - 1;
                for (int i = 0; i < parserCount; i++) {
                    try {
                        chunkQueue.put(Chunk.POISON);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                allDone.countDown();
            }
        }

        /**
         * Infers the schema from the first chunk and swaps {@link #reader} for the schema-bound
         * variant returned by {@link FormatReader#withSchema(List)}; parser threads thereafter
         * skip per-chunk inference. Same approach as ClickHouse / DuckDB / Spark.
         */
        private void bindSchemaFromFirstChunk(byte[] buffer, int length) throws IOException {
            ByteArrayStorageObject firstChunkObj = new ByteArrayStorageObject(
                StoragePath.of("mem://chunk-schema-probe"),
                buffer,
                0,
                length
            );
            SourceMetadata metadata = reader.metadata(firstChunkObj);
            List<Attribute> schema = metadata == null ? null : metadata.schema();
            if (schema == null) {
                return;
            }
            FormatReader bound = reader.withSchema(schema);
            if (bound == reader) {
                return;
            }
            if (bound instanceof SegmentableFormatReader segBound) {
                this.reader = segBound;
            } else {
                throw new IllegalStateException(
                    "FormatReader#withSchema returned a non-SegmentableFormatReader: " + bound.getClass().getName()
                );
            }
        }

        private void dispatchChunk(int index, byte[] buffer, int length, boolean last) {
            // Wait until the consumer has released a slot — pageQueues are indexed by
            // chunk.index % windowSize, so dispatching chunk N+windowSize before chunk N's
            // POISON has been observed would push pages from two generations into the same
            // queue and break ordering.
            try {
                consumerSlots.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                firstError.compareAndSet(null, e);
                return;
            }
            chunksDispatched.incrementAndGet();
            try {
                chunkQueue.put(new Chunk(index, buffer, length, last));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                firstError.compareAndSet(null, e);
            }
        }

        private void runParser() {
            try {
                while (closed == false && firstError.get() == null) {
                    Chunk chunk;
                    try {
                        chunk = chunkQueue.take();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    if (chunk == Chunk.POISON) {
                        break;
                    }

                    int queueSlot = chunk.index % windowSize;
                    ArrayBlockingQueue<Page> queue = pageQueues[queueSlot];
                    try {
                        ByteArrayStorageObject chunkObj = new ByteArrayStorageObject(
                            StoragePath.of("mem://chunk-" + chunk.index),
                            chunk.buffer,
                            0,
                            chunk.length
                        );

                        // Both record-boundary flags are true for every chunk:
                        // - firstSplit: the segmentator slices on \n carry-over, so each chunk
                        // starts on a complete record; no leading partial line to skip.
                        // - lastSplit: the segmentator only dispatches a chunk after locating its
                        // trailing \n via findLastNewline (or grows the buffer until one is found),
                        // so the chunk's final byte is always a record terminator. The original
                        // chunk.last flag was set only on the EOF chunk, leaving every interior
                        // chunk wrapped in TrimLastPartialLineInputStream's per-byte tail scan —
                        // pure overhead on already-aligned data. Setting lastSplit=true everywhere
                        // lets line-oriented readers (NDJSON) skip that scan.
                        FormatReadContext ctx = FormatReadContext.builder()
                            .projectedColumns(projectedColumns)
                            .batchSize(batchSize)
                            .errorPolicy(errorPolicy)
                            .firstSplit(true)
                            .lastSplit(true)
                            .build();

                        try (CloseableIterator<Page> pages = reader.read(chunkObj, ctx)) {
                            while (pages.hasNext()) {
                                if (firstError.get() != null || closed) {
                                    break;
                                }
                                queue.put(pages.next());
                            }
                        }
                    } catch (Exception e) {
                        firstError.compareAndSet(null, e);
                    } finally {
                        recycleBuffer(chunk.buffer);
                        enqueuePoison(queue);
                    }
                }
            } finally {
                allDone.countDown();
            }
        }

        private void recycleBuffer(byte[] buf) {
            if (buf.length <= chunkSize) {
                bufferPool.offer(buf);
            }
        }

        private static int readFromStream(InputStream stream, byte[] buf, int offset, int length) throws IOException {
            int totalRead = 0;
            while (totalRead < length) {
                int n = stream.read(buf, offset + totalRead, length - totalRead);
                if (n < 0) {
                    return totalRead == 0 ? -1 : totalRead;
                }
                totalRead += n;
            }
            return totalRead;
        }

        private static int findLastNewline(byte[] buf, int length) {
            for (int i = length - 1; i >= 0; i--) {
                if (buf[i] == '\n') {
                    return i;
                }
            }
            return -1;
        }

        private static byte[] growUntilNewline(InputStream stream, byte[] existing, int existingLen, int growBy) throws IOException {
            byte[] grown = new byte[existingLen + growBy];
            System.arraycopy(existing, 0, grown, 0, existingLen);

            int offset = existingLen;
            while (true) {
                int n = stream.read(grown, offset, grown.length - offset);
                if (n < 0) {
                    if (offset == existingLen) {
                        return Arrays.copyOf(existing, existingLen);
                    }
                    return Arrays.copyOf(grown, offset);
                }
                // Check for newline in the newly read bytes
                for (int i = offset; i < offset + n; i++) {
                    if (grown[i] == '\n') {
                        return Arrays.copyOf(grown, offset + n);
                    }
                }
                offset += n;
                if (offset >= grown.length) {
                    grown = Arrays.copyOf(grown, grown.length + growBy);
                }
            }
        }

        private static void enqueuePoison(ArrayBlockingQueue<Page> queue) {
            boolean poisoned = false;
            while (poisoned == false) {
                try {
                    queue.put(POISON);
                    poisoned = true;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (buffered != null) {
                return true;
            }
            try {
                buffered = takeNextPage();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for streaming parallel parse results", e);
            }
            return buffered != null;
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            Page result = buffered;
            buffered = null;
            return result;
        }

        private Page takeNextPage() throws InterruptedException {
            while (true) {
                checkError();

                if (currentChunk >= chunksDispatched.get()) {
                    // Segmentator hasn't dispatched more chunks yet; briefly wait
                    if (allDone.await(10, TimeUnit.MILLISECONDS)) {
                        // All threads done; check if more chunks were dispatched
                        if (currentChunk >= chunksDispatched.get()) {
                            checkError();
                            return null;
                        }
                    }
                    continue;
                }

                int slot = currentChunk % windowSize;
                ArrayBlockingQueue<Page> queue = pageQueues[slot];
                Page page = queue.poll(100, TimeUnit.MILLISECONDS);

                if (page == null) {
                    continue;
                }
                if (page == POISON) {
                    currentChunk++;
                    consumerSlots.release();
                    continue;
                }
                return page;
            }
        }

        private void checkError() {
            Throwable t = firstError.get();
            if (t != null) {
                if (t instanceof RuntimeException re) {
                    throw re;
                }
                throw new RuntimeException("Streaming parallel parsing failed", t);
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            // Wake the segmentator if it is parked on consumerSlots.acquire(); a stale permit count
            // is harmless because the segmentator also checks `closed` after acquiring.
            consumerSlots.release(Integer.MAX_VALUE / 2);
            drainAllQueues();
            try {
                if (allDone.await(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS) == false) {
                    logger.warn("Timed out waiting for streaming parallel parsing threads to finish after [{}]s", CLOSE_TIMEOUT_SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            drainAllQueues();
        }

        private void drainAllQueues() {
            // Drain chunk queue
            Chunk chunk;
            while ((chunk = chunkQueue.poll()) != null) {
                if (chunk != Chunk.POISON) {
                    recycleBuffer(chunk.buffer);
                }
            }
            // Drain page queues
            for (ArrayBlockingQueue<Page> queue : pageQueues) {
                Page p;
                while ((p = queue.poll()) != null) {
                    if (p != POISON && p.getPositionCount() > 0) {
                        p.releaseBlocks();
                    }
                }
            }
        }
    }

    private record Chunk(int index, byte[] buffer, int length, boolean last) {
        static final Chunk POISON = new Chunk(-1, new byte[0], 0, true);
    }

    /**
     * Minimal StorageObject wrapping an InputStream for the parallelism=1 fallback path.
     */
    private static final class InputStreamStorageObject implements StorageObject {
        private final InputStream stream;

        InputStreamStorageObject(InputStream stream) {
            this.stream = stream;
        }

        @Override
        public InputStream newStream() {
            return stream;
        }

        @Override
        public InputStream newStream(long position, long length) {
            throw new UnsupportedOperationException("Streaming storage object does not support random access");
        }

        @Override
        public long length() {
            throw new UnsupportedOperationException("Streaming storage object has unknown length");
        }

        @Override
        public java.time.Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public org.elasticsearch.xpack.esql.datasources.spi.StoragePath path() {
            return StoragePath.of("stream://decompressed");
        }
    }
}
