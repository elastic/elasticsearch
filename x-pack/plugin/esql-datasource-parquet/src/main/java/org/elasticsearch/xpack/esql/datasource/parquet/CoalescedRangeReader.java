/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.action.ActionListener;
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
     * Merges adjacent/overlapping ranges whose gap is below {@code maxCoalesceGap}, then fetches
     * each merged range in parallel via {@link StorageObject#readBytesAsync}. On completion, slices
     * individual requested ranges from the coalesced buffers and delivers them to the listener.
     *
     * @param storageObject the storage object to read from
     * @param ranges the byte ranges to fetch (need not be sorted)
     * @param maxCoalesceGap maximum gap in bytes between two ranges to merge them
     * @param executor executor for async dispatch
     * @param listener receives a map from each original range to its data
     */
    static void readCoalesced(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        Executor executor,
        ActionListener<Map<ByteRange, ByteBuffer>> listener
    ) {
        if (ranges.isEmpty()) {
            listener.onResponse(Map.of());
            return;
        }

        List<MergedRange> merged = mergeRanges(ranges, maxCoalesceGap);

        Map<ByteRange, ByteBuffer> results = new HashMap<>(ranges.size());
        AtomicInteger remaining = new AtomicInteger(merged.size());
        AtomicReference<Exception> firstFailure = new AtomicReference<>();

        for (MergedRange mr : merged) {
            storageObject.readBytesAsync(mr.offset, mr.length, executor, new ActionListener<>() {
                @Override
                public void onResponse(ByteBuffer buffer) {
                    synchronized (results) {
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
                    if (firstFailure.compareAndSet(null, e) == false) {
                        firstFailure.get().addSuppressed(e);
                    }
                    complete();
                }

                private void complete() {
                    if (remaining.decrementAndGet() == 0) {
                        Exception failure = firstFailure.get();
                        if (failure != null) {
                            listener.onFailure(failure);
                        } else {
                            listener.onResponse(results);
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
