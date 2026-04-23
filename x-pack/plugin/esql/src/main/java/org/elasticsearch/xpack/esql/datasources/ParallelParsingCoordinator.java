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
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinates parallel parsing of a single file by splitting it into byte-range
 * segments and dispatching each segment to a parser thread. Results are reassembled
 * in segment order so that row ordering is preserved.
 * <p>
 * Inspired by ClickHouse's {@code ParallelParsingInputFormat}. The approach:
 * <ol>
 *   <li>A segmentator divides the file into N byte-range segments at record boundaries</li>
 *   <li>Each segment is parsed independently on a separate executor thread</li>
 *   <li>The coordinator yields pages in segment order via a {@link CloseableIterator}</li>
 * </ol>
 * <p>
 * This coordinator only works with {@link SegmentableFormatReader} implementations
 * (line-oriented formats like CSV and NDJSON). Columnar formats have their own
 * row-group-level parallelism.
 */
public final class ParallelParsingCoordinator {

    private static final Logger logger = LogManager.getLogger(ParallelParsingCoordinator.class);

    private ParallelParsingCoordinator() {}

    /**
     * Creates a parallel-parsing iterator over a single storage object.
     * <p>
     * The file is divided into {@code parallelism} segments at record boundaries.
     * Each segment is parsed independently and results are yielded in order.
     * If the file is too small for meaningful parallelism (below the reader's
     * {@link SegmentableFormatReader#minimumSegmentSize()} per segment), falls
     * back to single-threaded reading.
     *
     * @param reader            the segmentable format reader
     * @param storageObject     the file to read
     * @param projectedColumns  columns to project
     * @param batchSize         rows per page
     * @param parallelism       number of parallel parser threads
     * @param executor          executor for parser threads
     * @return an iterator that yields pages in segment order
     */
    public static CloseableIterator<Page> parallelRead(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor
    ) throws IOException {
        return parallelRead(reader, storageObject, projectedColumns, batchSize, parallelism, executor, null);
    }

    /**
     * Creates a parallel-parsing iterator with an explicit error policy.
     *
     * @param errorPolicy error handling policy for per-segment parsing, or {@code null} for defaults
     */
    public static CloseableIterator<Page> parallelRead(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        List<String> projectedColumns,
        int batchSize,
        int parallelism,
        Executor executor,
        ErrorPolicy errorPolicy
    ) throws IOException {
        long fileLength = storageObject.length();
        long minSegment = reader.minimumSegmentSize();

        ErrorPolicy effectivePolicy = errorPolicy != null ? errorPolicy : ErrorPolicy.STRICT;
        FormatReadContext baseCtx = FormatReadContext.builder()
            .projectedColumns(projectedColumns)
            .batchSize(batchSize)
            .errorPolicy(effectivePolicy)
            .build();
        if (parallelism <= 1 || fileLength < minSegment * 2) {
            return reader.read(storageObject, baseCtx);
        }

        List<long[]> segments = computeSegments(reader, storageObject, fileLength, parallelism, minSegment);

        if (segments.size() <= 1) {
            return reader.read(storageObject, baseCtx);
        }

        return new OrderedParallelIterator(reader, storageObject, projectedColumns, batchSize, segments, executor, effectivePolicy);
    }

    /**
     * Computes byte-range segments for the file by probing record boundaries.
     * Each segment is a {@code [offset, length]} pair. The first segment starts
     * at offset 0; subsequent segments start at the record boundary found after
     * the nominal split point.
     */
    public static List<long[]> computeSegments(
        SegmentableFormatReader reader,
        StorageObject storageObject,
        long fileLength,
        int parallelism,
        long minSegment
    ) throws IOException {
        long nominalSize = fileLength / parallelism;
        if (nominalSize < minSegment) {
            nominalSize = minSegment;
        }

        List<Long> boundaries = new ArrayList<>();
        boundaries.add(0L);

        long pos = nominalSize;
        while (pos < fileLength) {
            long remaining = fileLength - pos;
            if (remaining < minSegment) {
                break;
            }
            try (InputStream stream = storageObject.newStream(pos, remaining)) {
                long skipped = reader.findNextRecordBoundary(stream);
                if (skipped < 0) {
                    break;
                }
                long boundary = pos + skipped;
                if (boundary >= fileLength) {
                    break;
                }
                if (fileLength - boundary < minSegment) {
                    break;
                }
                boundaries.add(boundary);
            }
            pos = boundaries.get(boundaries.size() - 1) + nominalSize;
        }

        List<long[]> segments = new ArrayList<>(boundaries.size());
        for (int i = 0; i < boundaries.size(); i++) {
            long start = boundaries.get(i);
            long end = (i + 1 < boundaries.size()) ? boundaries.get(i + 1) : fileLength;
            segments.add(new long[] { start, end - start });
        }
        return segments;
    }

    /**
     * Iterator that dispatches segment parsing to an executor and yields pages
     * in strict segment order. Each segment's pages are fully consumed before
     * moving to the next segment.
     * <p>
     * Parser threads push pages into per-segment queues. The consumer thread
     * (the driver calling {@code next()}) drains queues in order.
     */
    private static final class OrderedParallelIterator implements CloseableIterator<Page> {

        private static final Page POISON = new Page(0);
        private static final long CLOSE_TIMEOUT_SECONDS = 60;

        private final SegmentableFormatReader reader;
        private final StorageObject storageObject;
        private final List<String> projectedColumns;
        private final int batchSize;
        private final ErrorPolicy errorPolicy;

        private final List<BlockingQueue<Page>> segmentQueues;
        private final AtomicReference<Throwable> firstError = new AtomicReference<>();
        private final CountDownLatch allDone;

        private int currentSegment = 0;
        private Page buffered = null;
        private volatile boolean closed = false;

        OrderedParallelIterator(
            SegmentableFormatReader reader,
            StorageObject storageObject,
            List<String> projectedColumns,
            int batchSize,
            List<long[]> segments,
            Executor executor,
            ErrorPolicy errorPolicy
        ) {
            this.reader = reader;
            this.storageObject = storageObject;
            this.projectedColumns = projectedColumns;
            this.batchSize = batchSize;
            this.errorPolicy = errorPolicy;
            this.allDone = new CountDownLatch(segments.size());

            this.segmentQueues = new ArrayList<>(segments.size());
            for (int i = 0; i < segments.size(); i++) {
                segmentQueues.add(new ArrayBlockingQueue<>(16));
            }

            for (int i = 0; i < segments.size(); i++) {
                final int segIdx = i;
                final long[] seg = segments.get(i);
                try {
                    executor.execute(() -> parseSegment(segIdx, seg[0], seg[1], segments.size()));
                } catch (RejectedExecutionException e) {
                    firstError.compareAndSet(null, e);
                    enqueuePoison(segmentQueues.get(segIdx));
                    allDone.countDown();
                }
            }
        }

        private void parseSegment(int segmentIndex, long offset, long length, int totalSegments) {
            BlockingQueue<Page> queue = segmentQueues.get(segmentIndex);
            try {
                boolean lastSplit = segmentIndex == totalSegments - 1;
                StorageObject segObj = new RangeStorageObject(storageObject, offset, length);

                // All segments start at record boundaries (probed by computeSegments),
                // so firstSplit is true for every segment: no line needs to be skipped.
                FormatReadContext ctx = FormatReadContext.builder()
                    .projectedColumns(projectedColumns)
                    .batchSize(batchSize)
                    .errorPolicy(errorPolicy)
                    .firstSplit(true)
                    .lastSplit(lastSplit)
                    .build();
                CloseableIterator<Page> pages = reader.read(segObj, ctx);
                try (pages) {
                    while (pages.hasNext()) {
                        if (firstError.get() != null || closed) {
                            break;
                        }
                        Page page = pages.next();
                        queue.put(page);
                    }
                }
            } catch (Exception e) {
                firstError.compareAndSet(null, e);
            } finally {
                enqueuePoison(queue);
                allDone.countDown();
            }
        }

        private static void enqueuePoison(BlockingQueue<Page> queue) {
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
                throw new RuntimeException("Interrupted while waiting for parallel parse results", e);
            }
            return buffered != null;
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new java.util.NoSuchElementException();
            }
            Page result = buffered;
            buffered = null;
            return result;
        }

        private Page takeNextPage() throws InterruptedException {
            while (currentSegment < segmentQueues.size()) {
                checkError();
                BlockingQueue<Page> queue = segmentQueues.get(currentSegment);
                Page page = queue.take();
                if (page == POISON) {
                    currentSegment++;
                    continue;
                }
                return page;
            }
            checkError();
            return null;
        }

        private void checkError() {
            Throwable t = firstError.get();
            if (t != null) {
                if (t instanceof RuntimeException re) {
                    throw re;
                }
                throw new RuntimeException("Parallel parsing failed", t);
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            drainAllQueues();
            try {
                if (allDone.await(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS) == false) {
                    logger.warn("Timed out waiting for parallel parsing threads to finish after [{}]s", CLOSE_TIMEOUT_SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            drainAllQueues();
        }

        private void drainAllQueues() {
            for (BlockingQueue<Page> queue : segmentQueues) {
                Page p;
                while ((p = queue.poll()) != null) {
                    if (p != POISON && p.getPositionCount() > 0) {
                        p.releaseBlocks();
                    }
                }
            }
        }
    }
}
