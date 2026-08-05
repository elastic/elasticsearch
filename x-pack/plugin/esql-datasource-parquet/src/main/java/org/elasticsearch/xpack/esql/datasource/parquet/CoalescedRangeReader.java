/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.action.ActionListener;
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
     * {@link Releasable} that owns the underlying direct memory. The caller must close
     * {@link #release()} when the slices are no longer needed (typically at row-group rollover) so
     * the breaker-accounted bytes are returned to the allocator eagerly instead of waiting for the
     * JVM {@code Cleaner}. The {@code release} closes every {@link DirectReadBuffer} obtained from
     * {@link StorageObject#readBytesAsync}, which decrements each backing {@code ArrowBuf}'s
     * reference count and returns the memory to {@code allocator}.
     */
    record CoalescedRangeResult(Map<ByteRange, ByteBuffer> ranges, Releasable release) {}

    /**
     * Merges adjacent/overlapping ranges whose gap is below {@code maxCoalesceGap}, then fetches
     * each merged range in parallel via {@link StorageObject#readBytesAsync}. On completion, slices
     * individual requested ranges from the coalesced buffers and delivers them to the listener.
     *
     * <p>The {@link DirectReadBuffer}s returned by each underlying read are surfaced as a single
     * composite {@link Releasable} on {@link CoalescedRangeResult#release()}; the caller owns them
     * from that point on and must close the result to release the native memory back to
     * {@code allocator}.
     *
     * @param storageObject the storage object to read from
     * @param ranges the byte ranges to fetch (need not be sorted)
     * @param maxCoalesceGap maximum gap in bytes between two ranges to merge them
     * @param allocator allocator used by the storage object to back each merged-range buffer
     * @param executor executor for async dispatch
     * @param listener receives the per-range slices plus the composite {@link Releasable}
     */
    static void readCoalesced(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        BufferAllocator allocator,
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

        // Bridge the Arrow allocator to the SPI's allocator-agnostic factory once, here at the
        // boundary, so backends do not need to know about BufferAllocator at all.
        DirectBufferFactory factory = DirectBufferFactory.forAllocator(allocator);

        for (MergedRange mr : merged) {
            storageObject.readBytesAsync(mr.offset, mr.length, factory, executor, new ActionListener<>() {
                @Override
                public void onResponse(DirectReadBuffer result) {
                    synchronized (results) {
                        buffers.add(result);
                        ByteBuffer buffer = result.buffer();
                        for (ByteRange original : mr.constituents) {
                            int relativeOffset = (int) (original.offset - mr.offset);
                            ByteBuffer slice = buffer.duplicate();
                            slice.position(relativeOffset);
                            slice.limit(relativeOffset + (int) original.length);
                            results.put(original, slice.slice());
                        }
                    }
                    complete();
                }

                @Override
                public void onFailure(Exception e) {
                    // The backend has already released its ArrowBuf on the failure path; nothing
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
