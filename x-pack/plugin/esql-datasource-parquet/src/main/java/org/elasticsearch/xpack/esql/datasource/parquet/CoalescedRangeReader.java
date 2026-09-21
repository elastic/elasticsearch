/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Merges adjacent byte ranges and fetches them via {@link StorageObject#readBytesAsync} or
 * {@link StorageObject#readBytes}. After all merged ranges complete, individual sub-ranges are
 * sliced from the coalesced buffers.
 *
 * <p>This is the I/O coalescing layer for the optimized Parquet reader. It reduces the number of
 * remote requests (e.g., S3 GETs) by merging nearby byte ranges and issuing them concurrently.
 */
final class CoalescedRangeReader {

    private static final Logger logger = LogManager.getLogger(CoalescedRangeReader.class);

    static final long DEFAULT_MAX_COALESCE_GAP = 1024 * 1024;

    /**
     * Upper bound on how far coalescing may extend a merged range, so a densely packed wide row
     * group does not become one very large contiguous array and request. This is a coalescing
     * bound, not an allocation bound: a single constituent larger than this keeps its own
     * oversized range. Matches {@link ParquetStorageObjectAdapter#MAX_WINDOW_SIZE} so merge GETs
     * and window GETs share the same 10 MiB in-flight ceiling. Permits drop when the GET completes;
     * coalesced buffers stay until that row group is decoded. {@code C × B} budgets concurrent GET
     * size, not retained prefetch. Using the adapter's 4 MiB
     * {@link ParquetStorageObjectAdapter#DEFAULT_WINDOW_SIZE} here would turn a representative
     * 152 MiB row group from roughly 16 requests into roughly 38.
     */
    static final long MAX_MERGED_RANGE_BYTES = ParquetStorageObjectAdapter.MAX_WINDOW_SIZE;

    /**
     * A byte range within a file: {@code [offset, offset + length)}.
     */
    record ByteRange(long offset, long length) implements Comparable<ByteRange> {
        ByteRange {
            if (offset < 0) {
                throw new IllegalArgumentException("offset must be non-negative, got: " + offset);
            }
            if (length < 0) {
                throw new IllegalArgumentException("length must be non-negative, got: " + length);
            }
            if (offset > Long.MAX_VALUE - length) {
                throw new IllegalArgumentException("range end overflows a long: offset [" + offset + "], length [" + length + "]");
            }
        }

        @Override
        public int compareTo(ByteRange other) {
            return Long.compare(this.offset, other.offset);
        }

        long end() {
            return offset + length;
        }
    }

    private CoalescedRangeReader() {}

    /**
     * Result of a coalesced read: the slices delivered to each original {@link ByteRange}, plus a
     * {@link Releasable} that owns the underlying buffers. The caller must close
     * {@link #release()} when the slices are no longer needed (typically at row-group rollover) so
     * the breaker-accounted bytes are released eagerly instead of waiting for GC. The {@code release}
     * closes every {@link DirectReadBuffer} allocated for the coalesced read.
     */
    record CoalescedRangeResult(Map<ByteRange, ByteBuffer> ranges, Releasable release) {}

    /**
     * Merges adjacent/overlapping ranges whose gap is below {@code maxCoalesceGap}, then fetches
     * each merged range in parallel via {@link StorageObject#readBytesAsync}. On completion, slices
     * individual requested ranges from the coalesced buffers and delivers them to the listener.
     *
     * <p>The {@link DirectReadBuffer}s returned by each underlying read are surfaced as a single
     * composite {@link Releasable} on {@link CoalescedRangeResult#release()}; the caller owns them
     * from that point on and must close the result to release the breaker charge.
     *
     * @param storageObject the storage object to read from
     * @param ranges the byte ranges to fetch (need not be sorted)
     * @param maxCoalesceGap maximum gap in bytes between two ranges to merge them
     * @param breaker circuit breaker charged for each merged-range buffer
     * @param executor executor for async dispatch
     * @param listener receives the per-range slices plus the composite {@link Releasable}
     * @return a handle that cancels in-flight range GETs; no-op after the reads complete
     */
    static Releasable readCoalesced(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        CircuitBreaker breaker,
        Executor executor,
        ActionListener<CoalescedRangeResult> listener
    ) {
        return readCoalesced(storageObject, ranges, maxCoalesceGap, breaker, null, null, executor, listener);
    }

    static Releasable readCoalesced(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        CircuitBreaker breaker,
        @Nullable ParquetIoWatermark ioWatermark,
        Executor executor,
        ActionListener<CoalescedRangeResult> listener
    ) {
        return readCoalesced(storageObject, ranges, maxCoalesceGap, breaker, ioWatermark, null, null, executor, listener);
    }

    static Releasable readCoalesced(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        CircuitBreaker breaker,
        @Nullable ParquetIoWatermark ioWatermark,
        @Nullable ParquetIoWatermark.AdmitHold admitHold,
        Executor executor,
        ActionListener<CoalescedRangeResult> listener
    ) {
        return readCoalesced(storageObject, ranges, maxCoalesceGap, breaker, ioWatermark, admitHold, null, executor, listener);
    }

    /**
     * @param footerBytes optional footer-tail cache. When a merged range is a subset of a cached
     *                    suffix, the bytes are <em>copied</em> into a breaker {@link DirectReadBuffer}
     *                    and no GET is issued (no watermark / admit-hold GET accounting). {@code null}
     *                    is today's GET path. Never aliases the LRU {@code byte[]}. A miss, a short
     *                    cached suffix, or a lookup failure falls through to {@code startReadBytesAsync}.
     */
    static Releasable readCoalesced(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        CircuitBreaker breaker,
        @Nullable ParquetIoWatermark ioWatermark,
        @Nullable ParquetIoWatermark.AdmitHold admitHold,
        @Nullable FooterByteCache footerBytes,
        Executor executor,
        ActionListener<CoalescedRangeResult> listener
    ) {
        if (ranges.isEmpty()) {
            listener.onResponse(new CoalescedRangeResult(Map.of(), () -> {}));
            return () -> {};
        }

        List<MergedRange> merged = mergeRanges(ranges, maxCoalesceGap);

        Map<ByteRange, ByteBuffer> results = new HashMap<>(ranges.size());
        // One DirectReadBuffer per successful merged-range read. Mutated only under the same
        // lock as {@code results}. On overall success the entire list is surfaced as the
        // CoalescedRangeResult's release; on overall failure each successful buffer is closed
        // immediately so the failure path leaves no outstanding breaker reservation.
        List<Releasable> buffers = new ArrayList<>(merged.size());
        List<Releasable> inflight = Collections.synchronizedList(new ArrayList<>(merged.size()));
        AtomicInteger remaining = new AtomicInteger(merged.size());
        AtomicReference<Exception> firstFailure = new AtomicReference<>();

        // Bridge the circuit breaker to the SPI's factory once, here at the boundary, so
        // backends do not need to know about CircuitBreaker at all. The watermark wrapper
        // charges actual allocated bytes beside REQUEST so footer estimates cannot drift.
        DirectBufferFactory factory = ParquetIoWatermark.bufferFactory(breaker, ioWatermark, admitHold);
        // Cache hits copy into a breaker buffer only: they are not a GET, so they must not
        // charge the I/O watermark or consume admit-hold GET budget.
        DirectBufferFactory cacheFactory = DirectBufferFactory.forBreaker(breaker);

        for (MergedRange mr : merged) {
            FooterCacheHit hit = lookupFooterCacheHit(storageObject, mr, footerBytes);
            if (hit != null) {
                inflight.add(() -> {});
                try {
                    executor.execute(() -> {
                        try {
                            DirectReadBuffer copied = copyFooterCacheHit(hit, cacheFactory);
                            try {
                                synchronized (results) {
                                    buffers.add(copied);
                                    DirectReadBuffer owned = copied;
                                    copied = null;
                                    sliceConstituents(owned.buffer(), mr, results);
                                }
                            } finally {
                                if (copied != null) {
                                    copied.close();
                                }
                            }
                        } catch (Throwable t) {
                            Exception e = t instanceof Exception ex ? ex : new ElasticsearchException(t);
                            recordFailure(firstFailure, e, inflight);
                        } finally {
                            complete(remaining, firstFailure, buffers, results, listener);
                        }
                    });
                } catch (Exception e) {
                    recordFailure(firstFailure, e, inflight);
                    complete(remaining, firstFailure, buffers, results, listener);
                }
                continue;
            }
            Releasable handle = storageObject.startReadBytesAsync(mr.offset, mr.length, factory, executor, new ActionListener<>() {
                @Override
                public void onResponse(DirectReadBuffer result) {
                    try {
                        synchronized (results) {
                            // Track the buffer before slicing so a short-read (or any slice) failure
                            // still hands ownership to the terminal complete(), which closes it
                            // along with its siblings.
                            buffers.add(result);
                            sliceConstituents(result.buffer(), mr, results);
                        }
                    } catch (Throwable t) {
                        // Do not rethrow. {@code result} is already in {@code buffers}, so the terminal
                        // complete() will close it. Rethrowing would let the SPI's default readBytesAsync
                        // catch also close {@code result}, double-releasing the buffer. Folding
                        // every throwable (not just Exception) into firstFailure guarantees a failure is
                        // delivered: with the finally below already calling complete(), letting an Error
                        // through instead would deliver a spurious success with truncated slices.
                        Exception e = t instanceof Exception ex ? ex : new ElasticsearchException(t);
                        recordFailure(firstFailure, e, inflight);
                    } finally {
                        complete(remaining, firstFailure, buffers, results, listener);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // The backend has already released its buffer on the failure path; nothing
                    // to clean up for this merged range. Siblings that succeeded are released by
                    // complete() below. The first failure also closes remaining inflight handles so
                    // sibling GETs do not keep Netty slots and storage permits until they finish.
                    recordFailure(firstFailure, e, inflight);
                    complete(remaining, firstFailure, buffers, results, listener);
                }
            });
            inflight.add(handle);
            if (firstFailure.get() != null) {
                // This GET was started after a sibling already failed (typically a synchronous
                // onFailure from an earlier startReadBytesAsync). Close its handle now: the CAS
                // abort above ran before this handle was added to inflight.
                closeQuietly(handle);
            }
        }
        return () -> Releasables.close(inflight);
    }

    /**
     * Synchronously fetches adjacent/overlapping ranges after coalescing them, then slices the
     * individual requested ranges from the coalesced buffers.
     *
     * <p>Each merged-range buffer is breaker-accounted and remains owned by the returned result.
     * Every allocated buffer is released if allocation, I/O, or slicing fails.
     */
    static CoalescedRangeResult readCoalescedSync(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        CircuitBreaker breaker
    ) throws IOException {
        return readCoalescedSync(storageObject, ranges, maxCoalesceGap, breaker, null);
    }

    static CoalescedRangeResult readCoalescedSync(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        CircuitBreaker breaker,
        @Nullable ParquetIoWatermark ioWatermark
    ) throws IOException {
        return readCoalescedSync(storageObject, ranges, maxCoalesceGap, breaker, ioWatermark, null);
    }

    static CoalescedRangeResult readCoalescedSync(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        CircuitBreaker breaker,
        @Nullable ParquetIoWatermark ioWatermark,
        @Nullable FooterByteCache footerBytes
    ) throws IOException {
        if (ranges.isEmpty()) {
            return new CoalescedRangeResult(Map.of(), () -> {});
        }

        List<MergedRange> merged = mergeRanges(ranges, maxCoalesceGap);
        for (MergedRange mr : merged) {
            if (mr.length() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("merged range length must fit in an int for synchronous reads, got: " + mr.length());
            }
        }

        Map<ByteRange, ByteBuffer> results = new HashMap<>(ranges.size());
        List<Releasable> buffers = new ArrayList<>(merged.size());
        DirectBufferFactory factory = ParquetIoWatermark.bufferFactory(breaker, ioWatermark);
        DirectBufferFactory cacheFactory = DirectBufferFactory.forBreaker(breaker);
        try {
            for (MergedRange mr : merged) {
                FooterCacheHit hit = lookupFooterCacheHit(storageObject, mr, footerBytes);
                if (hit != null) {
                    DirectReadBuffer copied = copyFooterCacheHit(hit, cacheFactory);
                    buffers.add(copied);
                    sliceConstituents(copied.buffer(), mr, results);
                    continue;
                }
                int length = (int) mr.length();
                DirectReadBuffer result = factory.allocateWritableWindow(length);
                buffers.add(result);
                ByteBuffer buffer = result.buffer();
                int read = 0;
                while (read < length) {
                    int currentRead = storageObject.readBytes(mr.offset() + read, buffer);
                    if (currentRead < 0) {
                        break;
                    }
                    if (currentRead == 0) {
                        throw new IOException(
                            "Read made no progress at offset ["
                                + (mr.offset() + read)
                                + "] with ["
                                + buffer.remaining()
                                + "] bytes remaining"
                        );
                    }
                    read += currentRead;
                }
                buffer.position(0).limit(read);
                sliceConstituents(buffer, mr, results);
            }
        } catch (Throwable t) {
            try {
                Releasables.close(buffers);
            } catch (Throwable releaseFailure) {
                t.addSuppressed(releaseFailure);
            }
            throw t;
        }
        return new CoalescedRangeResult(results, () -> Releasables.close(buffers));
    }

    /**
     * Slices each constituent {@link ByteRange} out of the coalesced {@code buffer} and stores the
     * resulting view in {@code results}. Package-private and free of I/O so the short-read boundary
     * math is directly testable.
     *
     * <p>A short read delivers a buffer whose {@code remaining()} is below the merged range
     * length. That is rejected up front with a descriptive {@link IllegalArgumentException}
     * rather than letting {@link ByteBuffer#position}/{@link ByteBuffer#limit} throw a terse
     * bounds error mid-loop (and rather than delivering a truncated slice). The caller folds
     * the failure into the coalesced read.
     */
    static void sliceConstituents(ByteBuffer buffer, MergedRange mr, Map<ByteRange, ByteBuffer> results) {
        int delivered = buffer.remaining();
        if (delivered < mr.length()) {
            throw new IllegalArgumentException(
                "Short read: received [" + delivered + "] bytes but merged range requires [" + mr.length() + "]"
            );
        }
        for (ByteRange original : mr.constituents()) {
            int relativeOffset = (int) (original.offset() - mr.offset());
            ByteBuffer slice = buffer.duplicate();
            slice.position(relativeOffset);
            slice.limit(relativeOffset + (int) original.length());
            results.put(original, slice.slice());
        }
    }

    // CAS winner closes remaining inflight GET handles; the batch cannot succeed after firstFailure.
    private static void recordFailure(AtomicReference<Exception> firstFailure, Exception e, List<Releasable> inflight) {
        if (firstFailure.compareAndSet(null, e)) {
            try {
                abortInflight(inflight);
            } catch (RuntimeException abortFailure) {
                e.addSuppressed(abortFailure);
            }
        } else {
            Exception first = firstFailure.get();
            if (first != null && first != e) {
                first.addSuppressed(e);
            }
        }
    }

    private static void abortInflight(List<Releasable> inflight) {
        final Releasable[] handles;
        synchronized (inflight) {
            handles = inflight.toArray(Releasable[]::new);
        }
        Releasables.close(handles);
    }

    private static void closeQuietly(Releasable handle) {
        try {
            handle.close();
        } catch (RuntimeException ignored) {
            // Same as abortInflight: cancel of a just-started handle must not hide firstFailure.
        }
    }

    private static void complete(
        AtomicInteger remaining,
        AtomicReference<Exception> firstFailure,
        List<Releasable> buffers,
        Map<ByteRange, ByteBuffer> results,
        ActionListener<CoalescedRangeResult> listener
    ) {
        if (remaining.decrementAndGet() == 0) {
            Exception failure = firstFailure.get();
            if (failure != null) {
                Releasables.close(buffers);
                listener.onFailure(failure);
            } else {
                listener.onResponse(new CoalescedRangeResult(results, () -> Releasables.close(buffers)));
            }
        }
    }

    /**
     * Hit iff {@code [fileAbsOffset, fileAbsOffset + len)} sits inside the cached suffix
     * {@code [fileLength - cached.length, fileLength)}. Coordinates are file-absolute
     * ({@link StorageObject#offsetForFooterCache} + {@link FooterByteCache.Key#keyFor}).
     */
    @Nullable
    private static FooterCacheHit lookupFooterCacheHit(StorageObject storageObject, MergedRange mr, @Nullable FooterByteCache footerBytes) {
        if (footerBytes == null || mr.length() <= 0L || mr.length() > Integer.MAX_VALUE) {
            return null;
        }
        final FooterByteCache.Key key;
        final long fileAbsOffset;
        try {
            key = FooterByteCache.Key.keyFor(storageObject);
            fileAbsOffset = storageObject.offsetForFooterCache(mr.offset());
        } catch (Exception e) {
            logger.debug("footer cache lookup skipped", e);
            return null;
        }
        byte[] cached = footerBytes.get(key);
        if (cached == null || cached.length == 0) {
            return null;
        }
        long fileLength = key.fileLength();
        if (cached.length > fileLength || fileAbsOffset < 0L) {
            return null;
        }
        long cacheStart = fileLength - cached.length;
        final long rangeEnd;
        try {
            rangeEnd = Math.addExact(fileAbsOffset, mr.length());
        } catch (ArithmeticException e) {
            return null;
        }
        if (fileAbsOffset < cacheStart || rangeEnd > fileLength) {
            return null;
        }
        return new FooterCacheHit(cached, Math.toIntExact(fileAbsOffset - cacheStart), (int) mr.length());
    }

    /**
     * Copies cached bytes into a breaker-accounted buffer. Never aliases the LRU {@code byte[]}.
     */
    private static DirectReadBuffer copyFooterCacheHit(FooterCacheHit hit, DirectBufferFactory factory) throws IOException {
        DirectReadBuffer dest = factory.allocateWritableWindow(hit.copyLen());
        try {
            dest.buffer().put(hit.cached(), hit.copyOffset(), hit.copyLen());
            dest.buffer().flip();
            DirectReadBuffer delivered = dest;
            dest = null;
            return delivered;
        } finally {
            if (dest != null) {
                dest.close();
            }
        }
    }

    private record FooterCacheHit(byte[] cached, int copyOffset, int copyLen) {}

    /**
     * Sorts ranges by offset and merges adjacent/overlapping ranges whose gap is within threshold
     * without extending a multi-constituent merged range beyond {@link #MAX_MERGED_RANGE_BYTES}.
     * Constituents are never split, so one constituent may exceed the cap.
     */
    static List<MergedRange> mergeRanges(List<ByteRange> ranges, long maxCoalesceGap) {
        if (ranges.size() == 1) {
            return List.of(new MergedRange(ranges.getFirst().offset, ranges.getFirst().length, List.of(ranges.getFirst())));
        }

        List<ByteRange> sorted = new ArrayList<>(ranges);
        sorted.sort(Comparator.comparingLong(ByteRange::offset));

        List<MergedRange> result = new ArrayList<>();
        long groupStart = sorted.getFirst().offset;
        long groupEnd = sorted.getFirst().end();
        List<ByteRange> constituents = new ArrayList<>();
        constituents.add(sorted.getFirst());

        for (int i = 1; i < sorted.size(); i++) {
            ByteRange current = sorted.get(i);
            long mergedEnd = Math.max(groupEnd, current.end());
            if (current.offset - groupEnd <= maxCoalesceGap && mergedEnd - groupStart <= MAX_MERGED_RANGE_BYTES) {
                groupEnd = mergedEnd;
                constituents.add(current);
            } else {
                result.add(new MergedRange(groupStart, groupEnd - groupStart, List.copyOf(constituents)));
                groupStart = current.offset;
                groupEnd = current.end();
                constituents.clear();
                constituents.add(current);
            }
        }
        result.add(new MergedRange(groupStart, groupEnd - groupStart, List.copyOf(constituents)));
        return result;
    }

    /**
     * A merged range that covers one or more original {@link ByteRange}s.
     */
    record MergedRange(long offset, long length, List<ByteRange> constituents) {}
}
