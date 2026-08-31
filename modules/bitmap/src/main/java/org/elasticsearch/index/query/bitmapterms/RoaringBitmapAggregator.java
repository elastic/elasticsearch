/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.Map;

/**
 * Collects exact numeric values into a single Roaring bitmap.
 * <p>
 * {@link RoaringBitmapAggregatorFactory} rejects a {@link org.elasticsearch.search.aggregations.CardinalityUpperBound}
 * above one, so this aggregator is only ever asked to collect and build owning bucket ord 0 and holds one bitmap
 * rather than one per bucket.
 */
final class RoaringBitmapAggregator extends MetricsAggregator {

    // Circuit breaker accounting runs in two tiers because MutableBitmap#ramBytesUsed() is expensive,
    // so measuring on every insertion would make collection quadratic. Both tiers go through the
    // MutableBitmap API: what a value costs in bytes is that class's concern, not this one's.
    //
    // 1. Estimate, every BREAKER_RESERVATION_VALUES values: reserve
    // MutableBitmap#estimateGrowthBytes for the batch. That is O(1), so it can run often, and it
    // bounds how much unreserved growth can pile up -- a runaway aggregation trips the breaker
    // during a batch rather than after it has already allocated.
    // 2. Measure, every VALUES_PER_MEASUREMENT values: replace every estimate made so far with
    // MutableBitmap#ramBytesUsed(). The estimate is a worst case, so without this correction a
    // bitmap that compresses well would trip the breaker on reservations it never needed.
    //
    // reservedBytes is the single source of truth: it always holds what has been handed to the
    // breaker, estimated or measured, so a measurement only has to release the difference.
    //
    // TODO drop the estimate tier once ramBytesUsed() is cheap; measuring the real delta per value
    // would then replace the rate, the batching and the interval.
    static final int BREAKER_RESERVATION_VALUES = 1 << 10;
    private static final int VALUES_PER_MEASUREMENT = 1 << 18;

    private final ValuesSource.Numeric valuesSource;
    private final InternalRoaringBitmap.BitmapFormat width;
    private final String termsField;
    private final int termLength;
    private InternalRoaringBitmap.MutableBitmap bitmap;
    private long reservedBytes;
    private int valuesUntilNextEstimate;
    private int valuesUntilNextMeasurement = VALUES_PER_MEASUREMENT;

    RoaringBitmapAggregator(
        String name,
        ValuesSource.Numeric valuesSource,
        ValuesSourceConfig config,
        InternalRoaringBitmap.BitmapFormat width,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = valuesSource;
        this.width = width;
        this.termsField = termsFieldIfAvailable(config);
        this.termLength = switch (width) {
            case INT -> Integer.BYTES;
            case LONG -> Long.BYTES;
            // Never read: UNMAPPED implies a null values source, which termsFieldIfAvailable rejects,
            // so the terms path -- the only caller of decodeTerm -- is unreachable for it.
            case UNMAPPED -> -1;
        };
    }

    private void checkCancelled() {
        if (context.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }
        if (context.searcher() instanceof ContextIndexSearcher searcher) {
            searcher.checkCancelled();
        }
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        LeafReader reader = aggCtx.getLeafReaderContext().reader();
        if (termsField != null && reader.getLiveDocs() == null) {
            collectTerms(reader.terms(termsField));
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        SortedNumericLongValues values = valuesSource.longValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                assert owningBucketOrd == 0 : "cardinality is restricted to one, but collected ord [" + owningBucketOrd + "]";
                if (values.advanceExact(doc) == false) {
                    return;
                }
                InternalRoaringBitmap.MutableBitmap target = getOrCreateBitmap();
                int valueCount = values.docValueCount();
                for (int i = 0; i < valueCount; i++) {
                    long value = values.nextValue();
                    if (value < 0) {
                        throw negativeValue(value);
                    }
                    accountForValue();
                    target.add(value);
                }
            }
        };
    }

    private String termsFieldIfAvailable(ValuesSourceConfig config) {
        if (valuesSource == null
            || parent() != null
            || config.alignsWithSearchIndex() == false
            || (topLevelQuery() != null && topLevelQuery().getClass() != MatchAllDocsQuery.class)) {
            return null;
        }
        if (config.fieldType() instanceof NumberFieldMapper.NumberFieldType numberFieldType && numberFieldType.isIndexedWithTerms()) {
            return numberFieldType.name();
        }
        return null;
    }

    boolean usesTermsIndex() {
        return termsField != null;
    }

    private void collectTerms(Terms terms) throws IOException {
        if (terms == null) {
            return;
        }
        checkCancelled();
        BytesRef min = terms.getMin();
        if (min == null) {
            return;
        }
        long minValue = decodeTerm(min);
        if (minValue < 0) {
            throw negativeValue(minValue);
        }

        InternalRoaringBitmap.MutableBitmap target = getOrCreateBitmap();
        TermsEnum termsEnum = terms.iterator();
        BytesRef term;
        while ((term = termsEnum.next()) != null) {
            accountForValue();
            target.add(decodeTerm(term));
        }
        measureBitmap();
    }

    private long decodeTerm(BytesRef term) {
        assert term.length == termLength
            : "[roaring_bitmap] aggregation expected indexed terms of length [" + termLength + "] but found [" + term.length + "]";
        return switch (width) {
            case INT -> NumericUtils.sortableBytesToInt(term.bytes, term.offset);
            case LONG -> NumericUtils.sortableBytesToLong(term.bytes, term.offset);
            case UNMAPPED -> throw new IllegalStateException("cannot decode indexed terms for an unmapped field");
        };
    }

    private InternalRoaringBitmap.MutableBitmap getOrCreateBitmap() {
        if (bitmap == null) {
            bitmap = InternalRoaringBitmap.mutable(width);
            long initialBytes = bitmap.ramBytesUsed();
            addRequestCircuitBreakerBytes(initialBytes);
            reservedBytes += initialBytes;
        }
        return bitmap;
    }

    private static IllegalArgumentException negativeValue(long value) {
        return new IllegalArgumentException(
            "[roaring_bitmap] aggregation only supports non-negative values, but field produced [" + value + "]"
        );
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
        assert owningBucketOrd == 0 : "cardinality is restricted to one, but built ord [" + owningBucketOrd + "]";
        if (width == InternalRoaringBitmap.BitmapFormat.UNMAPPED || bitmap == null) {
            return buildEmptyAggregation();
        }
        checkCancelled();
        // Measure before reading reservedBytes below, and again after optimize() so the reservation
        // follows the bitmap shrinking as array containers become run containers.
        measureBitmap();
        bitmap.optimize();
        measureBitmap();
        checkCancelled();

        // ByteArrayOutputStream and toByteArray temporarily coexist with the live bitmap. Reserve
        // room for both copies before serializing, then release the temporary reservation.
        long serializationBytes = 2L * reservedBytes;
        addRequestCircuitBreakerBytes(serializationBytes);
        byte[] serialized;
        try {
            serialized = bitmap.serialize();
        } finally {
            addRequestCircuitBreakerBytes(-serializationBytes);
        }
        // Reserve the retained array's length.
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
        measureBitmap();
    }

    /**
     * Accounts for one value about to be added: reserves the pessimistic estimate in batches, and
     * periodically replaces the accumulated estimates with a measurement of the real bitmap.
     */
    private void accountForValue() {
        if (--valuesUntilNextEstimate <= 0) {
            // Cancellation rides along with the batch because both want the same cheap periodic hook.
            checkCancelled();
            reserveEstimateFor(BREAKER_RESERVATION_VALUES);
            valuesUntilNextEstimate = BREAKER_RESERVATION_VALUES;
        }
        if (--valuesUntilNextMeasurement <= 0) {
            measureBitmap();
        }
    }

    /** Reserves the bitmap's own worst-case estimate for {@code valueCount} values, without measuring it. */
    private void reserveEstimateFor(long valueCount) {
        long reservation = getOrCreateBitmap().estimateGrowthBytes(valueCount);
        addRequestCircuitBreakerBytes(reservation);
        reservedBytes += reservation;
    }

    /**
     * Replaces every reservation made so far -- estimated or measured -- with the bitmap's real size,
     * releasing the difference. No estimate is left standing afterwards, so the next value starts a
     * fresh batch.
     */
    private void measureBitmap() {
        if (bitmap == null) {
            return;
        }
        long measuredBytes = bitmap.ramBytesUsed();
        long difference = measuredBytes - reservedBytes;
        if (difference != 0) {
            addRequestCircuitBreakerBytes(difference);
        }
        reservedBytes = measuredBytes;
        valuesUntilNextEstimate = 0;
        valuesUntilNextMeasurement = VALUES_PER_MEASUREMENT;
    }
}
