/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.operator.SuppressedFailures;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.StorageRetryCancellation;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.HeapFootprint;
import org.elasticsearch.xpack.esql.datasources.spi.NodeByteBudget;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIoAffinity;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

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
     * and window GETs share the same (just under) 8 MiB in-flight ceiling. Permits drop when the GET completes;
     * coalesced buffers stay until that row group is decoded. {@code C × B} budgets concurrent GET
     * size, not retained prefetch. Using the adapter's 4 MiB
     * {@link ParquetStorageObjectAdapter#DEFAULT_WINDOW_SIZE} here would turn a representative
     * 152 MiB row group from roughly 19 requests into roughly 38.
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
        return readCoalesced(
            storageObject,
            ranges,
            maxCoalesceGap,
            breaker,
            ioWatermark,
            admitHold,
            footerBytes,
            admitHold != null ? ParquetIoWatermark.ByteGate.GROUP_HOLD : ParquetIoWatermark.ByteGate.UNGATED,
            executor,
            listener
        );
    }

    static Releasable readCoalesced(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        CircuitBreaker breaker,
        @Nullable ParquetIoWatermark ioWatermark,
        @Nullable ParquetIoWatermark.AdmitHold admitHold,
        @Nullable FooterByteCache footerBytes,
        ParquetIoWatermark.ByteGate byteGate,
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

        // GROUP_HOLD reuses the caller's footer-estimate hold. UNGATED forceAdds. PER_GET
        // draws one unit ticket covering every miss; a null hold is never treated as PER_GET.
        DirectBufferFactory factory = byteGate == ParquetIoWatermark.ByteGate.GROUP_HOLD
            ? ParquetIoWatermark.bufferFactory(breaker, ioWatermark, admitHold)
            : ParquetIoWatermark.bufferFactory(breaker, ioWatermark, null);
        // Cache hits copy into a breaker buffer only: they are not a GET, so they must not
        // charge the I/O watermark or consume admit-hold GET budget.
        DirectBufferFactory cacheFactory = DirectBufferFactory.forBreaker(breaker);

        List<MergedRange> gets = new ArrayList<>();
        List<MergedRange> hitRanges = new ArrayList<>();
        List<FooterCacheHit> hits = new ArrayList<>();
        for (MergedRange mr : merged) {
            FooterCacheHit hit = lookupFooterCacheHit(storageObject, mr, footerBytes);
            if (hit != null) {
                hitRanges.add(mr);
                hits.add(hit);
            } else {
                gets.add(mr);
            }
        }
        StorageIoAffinity.Scope scope = StorageIoAffinity.current();
        if (scope != null && scope.countGets) {
            scope.lease().addUnissued(gets.size());
        }

        AtomicReference<ParquetIoWatermark.AdmitHold> unitHold = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean();

        for (int i = 0; i < hitRanges.size(); i++) {
            MergedRange mr = hitRanges.get(i);
            FooterCacheHit hit = hits.get(i);
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
                        complete(remaining, firstFailure, buffers, results, listener, unitHold);
                    }
                });
            } catch (Exception e) {
                recordFailure(firstFailure, e, inflight);
                complete(remaining, firstFailure, buffers, results, listener, unitHold);
            }
        }
        if (gets.isEmpty() == false && byteGate == ParquetIoWatermark.ByteGate.PER_GET && ioWatermark != null) {
            admitUnitThenIssueGets(
                storageObject,
                gets,
                breaker,
                ioWatermark,
                executor,
                results,
                buffers,
                inflight,
                remaining,
                firstFailure,
                listener,
                unitHold,
                cancelled,
                scope
            );
        } else if (gets.isEmpty() == false) {
            issueGets(
                storageObject,
                gets,
                factory,
                executor,
                results,
                buffers,
                inflight,
                remaining,
                firstFailure,
                listener,
                unitHold,
                scope
            );
        }
        return () -> {
            cancelled.set(true);
            if (ioWatermark != null) {
                ioWatermark.nodeByteBudget().wakeWaiters();
            }
            Releasables.close(inflight);
        };
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
        return readCoalescedSync(
            storageObject,
            ranges,
            maxCoalesceGap,
            breaker,
            ioWatermark,
            footerBytes,
            ParquetIoWatermark.ByteGate.UNGATED
        );
    }

    static CoalescedRangeResult readCoalescedSync(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        CircuitBreaker breaker,
        @Nullable ParquetIoWatermark ioWatermark,
        @Nullable FooterByteCache footerBytes,
        ParquetIoWatermark.ByteGate byteGate
    ) throws IOException {
        return readCoalescedSync(storageObject, ranges, maxCoalesceGap, breaker, ioWatermark, footerBytes, byteGate, null);
    }

    static CoalescedRangeResult readCoalescedSync(
        StorageObject storageObject,
        List<ByteRange> ranges,
        long maxCoalesceGap,
        CircuitBreaker breaker,
        @Nullable ParquetIoWatermark ioWatermark,
        @Nullable FooterByteCache footerBytes,
        ParquetIoWatermark.ByteGate byteGate,
        @Nullable ParquetIoWatermark.AdmitHold admitHold
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
        DirectBufferFactory cacheFactory = DirectBufferFactory.forBreaker(breaker);
        StorageIoAffinity.Scope scope = StorageIoAffinity.current();
        ParquetIoWatermark.AdmitHold unitHold = null;
        try {
            List<MergedRange> misses = new ArrayList<>();
            List<MergedRange> hitRanges = new ArrayList<>();
            List<FooterCacheHit> hits = new ArrayList<>();
            long unitBytes = 0L;
            for (MergedRange mr : merged) {
                FooterCacheHit hit = lookupFooterCacheHit(storageObject, mr, footerBytes);
                if (hit != null) {
                    hitRanges.add(mr);
                    hits.add(hit);
                } else {
                    misses.add(mr);
                    if (byteGate == ParquetIoWatermark.ByteGate.PER_GET && ioWatermark != null) {
                        unitBytes = Math.addExact(unitBytes, HeapFootprint.byteArrayBytes(mr.length()));
                    }
                }
            }
            if (unitBytes > 0L) {
                unitHold = ioWatermark.wrap(admitUnitSync(ioWatermark, unitBytes, requireLease(scope)));
            }
            ParquetIoWatermark.AdmitHold factoryHold = switch (byteGate) {
                case GROUP_HOLD -> admitHold;
                case PER_GET -> unitHold;
                case UNGATED -> null;
            };
            DirectBufferFactory factory = factoryHold != null
                ? ParquetIoWatermark.bufferFactory(breaker, ioWatermark, factoryHold)
                : ParquetIoWatermark.bufferFactory(breaker, ioWatermark);
            for (int i = 0; i < hitRanges.size(); i++) {
                DirectReadBuffer copied = copyFooterCacheHit(hits.get(i), cacheFactory);
                buffers.add(copied);
                sliceConstituents(copied.buffer(), hitRanges.get(i), results);
            }
            for (MergedRange mr : misses) {
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
        } finally {
            if (unitHold != null) {
                unitHold.drop();
            }
        }
        return new CoalescedRangeResult(results, () -> Releasables.close(buffers));
    }

    private static RowGroupIo requireLease(StorageIoAffinity.Scope scope) {
        if (scope == null) {
            throw new IllegalStateException("PER_GET admission requires a row-group lease");
        }
        return scope.lease();
    }

    /**
     * One ticket covering every coalesced GET in this call. Look-ahead {@link NodeByteBudget#tryAdmit}
     * is attempted first; otherwise the caller waits on {@link NodeByteBudget#admitAsync} until grant
     * or cancel. There is no timeout and no charge-on-expiry. Abandoning the wait cancels the
     * ticket so a late grant cannot leak bytes.
     */
    private static NodeByteBudget.Hold admitUnitSync(ParquetIoWatermark ioWatermark, long unitBytes, RowGroupIo lease) {
        NodeByteBudget budget = ioWatermark.nodeByteBudget();
        NodeByteBudget.Hold hold = budget.tryAdmit(unitBytes);
        if (hold != null) {
            return hold;
        }
        AtomicBoolean abandoned = new AtomicBoolean();
        AtomicReference<NodeByteBudget.Hold> granted = new AtomicReference<>();
        BooleanSupplier cancel = composeCancel(abandoned, lease);
        PlainActionFuture<NodeByteBudget.Hold> future = new PlainActionFuture<>();
        budget.admitAsync(unitBytes, lease, cancel, Runnable::run).addListener(ActionListener.wrap(grantedHold -> {
            if (abandoned.get()) {
                grantedHold.close();
                return;
            }
            granted.set(grantedHold);
            if (abandoned.get()) {
                grantedHold.close();
                return;
            }
            future.onResponse(grantedHold);
        }, e -> {
            if (abandoned.get()) {
                return;
            }
            future.onFailure(e);
        }));
        try {
            return future.actionGet();
        } catch (RuntimeException e) {
            abandoned.set(true);
            budget.wakeWaiters();
            NodeByteBudget.Hold late = granted.get();
            if (late != null) {
                late.close();
            }
            throw e;
        }
    }

    /**
     * Captures the ambient cancel supplier at ticket creation so grant/release threads do not
     * sample a different thread's signal, and so {@link StorageRetryCancellation#isCancelled()}
     * cannot recurse through this supplier when it is installed as CURRENT.
     */
    private static BooleanSupplier composeCancel(AtomicBoolean abandoned, RowGroupIo lease) {
        BooleanSupplier ambient = StorageRetryCancellation.current();
        BooleanSupplier captured = ambient == null ? () -> false : ambient;
        return () -> abandoned.get() || captured.getAsBoolean() || lease.isCancelled();
    }

    private static void admitUnitThenIssueGets(
        StorageObject storageObject,
        List<MergedRange> gets,
        CircuitBreaker breaker,
        ParquetIoWatermark ioWatermark,
        Executor executor,
        Map<ByteRange, ByteBuffer> results,
        List<Releasable> buffers,
        List<Releasable> inflight,
        AtomicInteger remaining,
        AtomicReference<Exception> firstFailure,
        ActionListener<CoalescedRangeResult> listener,
        AtomicReference<ParquetIoWatermark.AdmitHold> unitHold,
        AtomicBoolean cancelled,
        StorageIoAffinity.Scope scope
    ) {
        try {
            RowGroupIo lease = requireLease(scope);
            boolean countGets = scope.countGets;
            long unitBytes = 0L;
            for (MergedRange mr : gets) {
                unitBytes = Math.addExact(unitBytes, HeapFootprint.byteArrayBytes(mr.length()));
            }
            BooleanSupplier cancel = composeCancel(cancelled, lease);
            NodeByteBudget.Hold immediate = ioWatermark.nodeByteBudget().tryAdmit(unitBytes);
            if (immediate != null) {
                unitHold.set(ioWatermark.wrap(immediate));
                issueGets(
                    storageObject,
                    gets,
                    ParquetIoWatermark.bufferFactory(breaker, ioWatermark, unitHold.get()),
                    executor,
                    results,
                    buffers,
                    inflight,
                    remaining,
                    firstFailure,
                    listener,
                    unitHold,
                    scope
                );
                return;
            }
            ioWatermark.nodeByteBudget().admitAsync(unitBytes, lease, cancel, executor).addListener(ActionListener.wrap(hold -> {
                if (cancel.getAsBoolean()) {
                    hold.close();
                    failUnissuedGets(
                        gets,
                        remaining,
                        firstFailure,
                        buffers,
                        results,
                        listener,
                        unitHold,
                        scope,
                        NodeByteBudget.cancelled()
                    );
                    return;
                }
                try {
                    StorageRetryCancellation.runWithCancellation(cancel, () -> {
                        try (StorageIoAffinity.Scope ignored = StorageIoAffinity.open(lease, countGets)) {
                            unitHold.set(ioWatermark.wrap(hold));
                            issueGets(
                                storageObject,
                                gets,
                                ParquetIoWatermark.bufferFactory(breaker, ioWatermark, unitHold.get()),
                                executor,
                                results,
                                buffers,
                                inflight,
                                remaining,
                                firstFailure,
                                listener,
                                unitHold,
                                scope
                            );
                        }
                    });
                } catch (Exception e) {
                    hold.close();
                    recordFailure(firstFailure, e, inflight);
                    failUnissuedGets(gets, remaining, firstFailure, buffers, results, listener, unitHold, scope, null);
                }
            }, e -> {
                recordFailure(firstFailure, e, inflight);
                failUnissuedGets(gets, remaining, firstFailure, buffers, results, listener, unitHold, scope, null);
            }));
        } catch (Exception e) {
            failUnissuedGets(gets, remaining, firstFailure, buffers, results, listener, unitHold, scope, e);
        }
    }

    private static void failUnissuedGets(
        List<MergedRange> gets,
        AtomicInteger remaining,
        AtomicReference<Exception> firstFailure,
        List<Releasable> buffers,
        Map<ByteRange, ByteBuffer> results,
        ActionListener<CoalescedRangeResult> listener,
        AtomicReference<ParquetIoWatermark.AdmitHold> unitHold,
        @Nullable StorageIoAffinity.Scope scope,
        @Nullable Exception failure
    ) {
        if (failure != null) {
            recordFailure(firstFailure, failure, List.of());
        }
        if (scope != null && scope.countGets) {
            scope.lease().forgetUnissued(gets.size());
        }
        for (int i = 0; i < gets.size(); i++) {
            complete(remaining, firstFailure, buffers, results, listener, unitHold);
        }
    }

    private static void issueGets(
        StorageObject storageObject,
        List<MergedRange> gets,
        DirectBufferFactory factory,
        Executor executor,
        Map<ByteRange, ByteBuffer> results,
        List<Releasable> buffers,
        List<Releasable> inflight,
        AtomicInteger remaining,
        AtomicReference<Exception> firstFailure,
        ActionListener<CoalescedRangeResult> listener,
        AtomicReference<ParquetIoWatermark.AdmitHold> unitHold,
        @Nullable StorageIoAffinity.Scope scope
    ) {
        boolean abortUnissued = false;
        int startedGets = 0;
        for (MergedRange mr : gets) {
            if (abortUnissued) {
                complete(remaining, firstFailure, buffers, results, listener, unitHold);
                continue;
            }
            Releasable handle;
            try {
                handle = storageObject.startReadBytesAsync(mr.offset, mr.length, factory, executor, new ActionListener<>() {
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
                            complete(remaining, firstFailure, buffers, results, listener, unitHold);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        recordFailure(firstFailure, e, inflight);
                        complete(remaining, firstFailure, buffers, results, listener, unitHold);
                    }
                });
            } catch (RuntimeException e) {
                recordFailure(firstFailure, e, inflight);
                complete(remaining, firstFailure, buffers, results, listener, unitHold);
                if (scope != null && scope.countGets) {
                    scope.lease().forgetUnissued(gets.size() - startedGets);
                }
                abortUnissued = true;
                continue;
            }
            startedGets++;
            inflight.add(handle);
            if (firstFailure.get() != null) {
                // This GET was started after a sibling already failed (typically a synchronous
                // onFailure from an earlier startReadBytesAsync). Close its handle now: the CAS
                // abort above ran before this handle was added to inflight.
                closeQuietly(handle);
            }
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
            if (first != null) {
                SuppressedFailures.attach(first, e);
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
        ActionListener<CoalescedRangeResult> listener,
        AtomicReference<ParquetIoWatermark.AdmitHold> unitHold
    ) {
        if (remaining.decrementAndGet() == 0) {
            ParquetIoWatermark.AdmitHold hold = unitHold.get();
            if (hold != null) {
                hold.drop();
            }
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
