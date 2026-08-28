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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Merges adjacent byte ranges and fetches them in parallel via {@link StorageObject#readBytesAsync}.
 * After all merged ranges complete, individual sub-ranges are sliced from the coalesced buffers.
 *
 * <p>This is the I/O coalescing layer for the optimized Parquet reader. It reduces the number of
 * remote requests (e.g., S3 GETs) by merging nearby byte ranges and issuing them concurrently.
 */
final class CoalescedRangeReader {

    static final long DEFAULT_MAX_COALESCE_GAP = 1024 * 1024;

    /**
     * A byte range within a file: {@code [offset, offset + length)}.
     */
    record ByteRange(long offset, long length) implements Comparable<ByteRange> {
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
     * closes every {@link DirectReadBuffer} obtained from {@link StorageObject#readBytesAsync}.
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
     */
    static void readCoalesced(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        CircuitBreaker breaker,
        Executor executor,
        ActionListener<CoalescedRangeResult> listener
    ) {
        if (ranges.isEmpty()) {
            listener.onResponse(new CoalescedRangeResult(Map.of(), () -> {}));
            return;
        }

        List<MergedRange> merged = mergeRanges(ranges, maxCoalesceGap);

        Map<ByteRange, ByteBuffer> results = new HashMap<>(ranges.size());
        // One DirectReadBuffer per successful merged-range read. Mutated only under the same
        // lock as {@code results}. On overall success the entire list is surfaced as the
        // CoalescedRangeResult's release; on overall failure each successful buffer is closed
        // immediately so the failure path leaves no outstanding breaker reservation.
        List<Releasable> buffers = new ArrayList<>(merged.size());
        AtomicInteger remaining = new AtomicInteger(merged.size());
        AtomicReference<Exception> firstFailure = new AtomicReference<>();

        // Bridge the circuit breaker to the SPI's factory once, here at the boundary, so
        // backends do not need to know about CircuitBreaker at all.
        DirectBufferFactory factory = DirectBufferFactory.forBreaker(breaker);

        for (MergedRange mr : merged) {
            storageObject.readBytesAsync(mr.offset, mr.length, factory, executor, new ActionListener<>() {
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
                        if (firstFailure.compareAndSet(null, e) == false) {
                            firstFailure.get().addSuppressed(e);
                        }
                    } finally {
                        complete();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // The backend has already released its buffer on the failure path; nothing
                    // to clean up for this merged range. Siblings that succeeded are released by
                    // complete() below.
                    if (firstFailure.compareAndSet(null, e) == false) {
                        firstFailure.get().addSuppressed(e);
                    }
                    complete();
                }

                private void complete() {
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
            });
        }
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

    /**
     * Sorts ranges by offset and merges adjacent/overlapping ranges whose gap is within threshold.
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
            if (current.offset - groupEnd <= maxCoalesceGap) {
                groupEnd = Math.max(groupEnd, current.end());
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
