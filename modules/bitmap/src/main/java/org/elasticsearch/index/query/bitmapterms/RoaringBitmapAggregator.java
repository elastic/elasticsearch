/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Map;

/** Collects exact numeric values into one Roaring bitmap per owning bucket. */
final class RoaringBitmapAggregator extends MetricsAggregator {

    // Roaring's size calculation walks every container, so doing it for every insertion makes sparse
    // 64-bit collection quadratic. Reserve a conservative per-value estimate in small O(1) batches
    // between reconciliations, then reconcile the whole running total against ramBytesUsed() (which
    // itself corrects for JVM object/array overhead Roaring's own estimate excludes -- see
    // InternalRoaringBitmap's IntMutableBitmap/LongMutableBitmap#ramBytesUsed). Reconciliation
    // recomputes the estimate from scratch each time, so its accuracy tracks how closely
    // ramBytesUsed() approximates real heap use, not the reconciliation interval.
    static final int BREAKER_RESERVATION_VALUES = 1 << 10;
    private static final int MEMORY_RECONCILIATION_INTERVAL = 1 << 18;
    // A new sparse container/high-word entry's corrected ramBytesUsed() growth (Roaring's own reported
    // growth times InternalRoaringBitmap's overhead-correction factor) is roughly 56 bytes/value for
    // INT and 288 bytes/value for LONG in the worst case. These reservation rates keep comfortable
    // headroom above that so a small change in Roaring's container internals doesn't require also
    // shrinking the safety margin to zero.
    static final long INT_BYTES_PER_VALUE = 80;
    static final long LONG_BYTES_PER_VALUE = 384;

    private final ValuesSource.Numeric valuesSource;
    private final InternalRoaringBitmap.BitmapFormat width;
    private final LongObjectPagedHashMap<AccountedBitmap> bitmaps;
    private long accountedBitmapBytes;
    private int valuesUntilNextBreakerReservation;
    private int valuesSinceMemoryReconciliation;

    RoaringBitmapAggregator(
        String name,
        ValuesSource.Numeric valuesSource,
        InternalRoaringBitmap.BitmapFormat width,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = valuesSource;
        this.width = width;
        this.bitmaps = new LongObjectPagedHashMap<>(1, bigArrays());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        SortedNumericLongValues values = valuesSource.longValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc) == false) {
                    return;
                }
                AccountedBitmap accountedBitmap = bitmaps.get(owningBucketOrd);
                if (accountedBitmap == null) {
                    InternalRoaringBitmap.MutableBitmap bitmap = InternalRoaringBitmap.mutable(width);
                    accountedBitmap = new AccountedBitmap(bitmap, bitmap.ramBytesUsed());
                    bitmaps.put(owningBucketOrd, accountedBitmap);
                    addRequestCircuitBreakerBytes(accountedBitmap.accountedBytes);
                    accountedBitmapBytes += accountedBitmap.accountedBytes;
                }
                int valueCount = values.docValueCount();
                for (int i = 0; i < valueCount; i++) {
                    long value = values.nextValue();
                    if (value < 0) {
                        throw new IllegalArgumentException(
                            "[roaring_bitmap] aggregation only supports non-negative values, but field produced [" + value + "]"
                        );
                    }
                    reserveBreakerBytes();
                    accountedBitmap.bitmap.add(value);
                    if (++valuesSinceMemoryReconciliation == MEMORY_RECONCILIATION_INTERVAL) {
                        accountBitmapMemory();
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
        if (width == InternalRoaringBitmap.BitmapFormat.UNMAPPED) {
            return InternalRoaringBitmap.unmapped(name, metadata());
        }
        AccountedBitmap accountedBitmap = bitmaps.get(owningBucketOrd);
        if (accountedBitmap == null) {
            return InternalRoaringBitmap.empty(name, width, metadata());
        }
        accountBitmapMemory(accountedBitmap);
        accountedBitmap.bitmap.optimize();
        accountBitmapMemory(accountedBitmap);

        // ByteArrayOutputStream and toByteArray temporarily coexist with the live bitmap. Reserve
        // room for both copies before serializing, then release the temporary reservation.
        long serializationBytes = 2L * accountedBitmap.accountedBytes;
        addRequestCircuitBreakerBytes(serializationBytes);
        byte[] serialized;
        try {
            serialized = accountedBitmap.bitmap.serialize();
        } finally {
            addRequestCircuitBreakerBytes(-serializationBytes);
        }
        // Reserve the retained array's length. MetricsAggregator#buildAggregations calls this once per
        // owning bucket ord and close() only runs once the whole tree is built, so under a parent
        // `terms` aggregation these reservations accumulate across buckets for the whole build.
        //
        // This does NOT cover the array's full lifetime: AggregatorCollector calls
        // releaseAggregations() immediately after buildTopLevel(), and AggregatorBase#close() then
        // drops all of requestBytesUsed while the array stays reachable from the result tree. That
        // gap is not specific to this aggregation -- every InternalAggregation outlives its
        // aggregator -- so it is left open rather than worked around here.
        addRequestCircuitBreakerBytes(serialized.length);
        return new InternalRoaringBitmap(name, width, serialized, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        if (width == InternalRoaringBitmap.BitmapFormat.UNMAPPED) {
            return InternalRoaringBitmap.unmapped(name, metadata());
        }
        return InternalRoaringBitmap.empty(name, width, metadata());
    }

    @Override
    protected void doPostCollection() {
        accountBitmapMemory();
    }

    private void reserveBreakerBytes() {
        if (valuesUntilNextBreakerReservation == 0) {
            long reservation = BREAKER_RESERVATION_VALUES * bytesPerValue();
            addRequestCircuitBreakerBytes(reservation);
            accountedBitmapBytes += reservation;
            valuesUntilNextBreakerReservation = BREAKER_RESERVATION_VALUES;
        }
        valuesUntilNextBreakerReservation--;
    }

    private long bytesPerValue() {
        return switch (width) {
            case INT -> INT_BYTES_PER_VALUE;
            case LONG -> LONG_BYTES_PER_VALUE;
            case UNMAPPED -> throw new IllegalStateException("cannot reserve bitmap memory for an unmapped field");
        };
    }

    private void accountBitmapMemory() {
        long currentBytes = 0;
        for (LongObjectPagedHashMap.Cursor<AccountedBitmap> cursor : bitmaps) {
            cursor.value.updateAccountedBytes();
            currentBytes += cursor.value.accountedBytes;
        }
        long growth = currentBytes - accountedBitmapBytes;
        if (growth != 0) {
            addRequestCircuitBreakerBytes(growth);
        }
        accountedBitmapBytes = currentBytes;
        valuesUntilNextBreakerReservation = 0;
        valuesSinceMemoryReconciliation = 0;
    }

    private void accountBitmapMemory(AccountedBitmap bitmap) {
        long growth = bitmap.updateAccountedBytes();
        if (growth != 0) {
            addRequestCircuitBreakerBytes(growth);
            accountedBitmapBytes += growth;
        }
    }

    @Override
    protected void doClose() {
        // AggregatorBase registers this instance as releasable before this constructor allocates the
        // map. A cranky breaker may fail that allocation and still close the partially built instance.
        if (bitmaps != null) {
            bitmaps.close();
        }
    }

    private static final class AccountedBitmap {
        private final InternalRoaringBitmap.MutableBitmap bitmap;
        private long accountedBytes;

        private AccountedBitmap(InternalRoaringBitmap.MutableBitmap bitmap, long accountedBytes) {
            this.bitmap = bitmap;
            this.accountedBytes = accountedBytes;
        }

        private long updateAccountedBytes() {
            long currentBytes = bitmap.ramBytesUsed();
            long growth = currentBytes - accountedBytes;
            accountedBytes = currentBytes;
            return growth;
        }
    }
}
