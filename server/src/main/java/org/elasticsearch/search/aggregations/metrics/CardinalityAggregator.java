/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An aggregator that computes approximate counts of unique values.
 */
public class CardinalityAggregator extends NumericMetricsAggregator.SingleValue {

    private final int precision;
    private final CardinalityAggregatorFactory.ExecutionMode executionMode;
    private final ValuesSource valuesSource;
    private final HyperLogLogPlusPlus counts;

    private Collector collector;

    private int emptyCollectorsUsed;
    private int numericCollectorsUsed;
    private int ordinalsCollectorsUsed;
    private int ordinalsCollectorsOverheadTooHigh;
    private int stringHashingCollectorsUsed;

    public CardinalityAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int precision,
        CardinalityAggregatorFactory.ExecutionMode executionMode,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        assert valuesSourceConfig.hasValues();
        this.valuesSource = valuesSourceConfig.getValuesSource();
        this.precision = precision;
        this.counts = new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
        this.executionMode = executionMode;
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    private Collector pickCollector(LeafReaderContext ctx) throws IOException {
        if (valuesSource instanceof ValuesSource.Numeric source) {
            numericCollectorsUsed++;
            if (source.isFloatingPoint()) {
                SortedNumericDoubleValues values = source.doubleValues(ctx);
                NumericDoubleValues singleton = FieldData.unwrapSingleton(values);
                if (singleton != null) {
                    return new DirectSingleValuesCollector(counts, MurmurHash3SingleValues.hash(singleton));
                } else {
                    return new DirectMultiValuesCollector(counts, MurmurHash3MultiValues.hash(values));
                }
            } else {
                SortedNumericDocValues values = source.longValues(ctx);
                NumericDocValues singleton = DocValues.unwrapSingleton(values);
                if (singleton != null) {
                    return new DirectSingleValuesCollector(counts, MurmurHash3SingleValues.hash(singleton));
                } else {
                    return new DirectMultiValuesCollector(counts, MurmurHash3MultiValues.hash(values));
                }
            }
        }

        if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals source) {
            final SortedSetDocValues ordinalValues = source.ordinalsValues(ctx);
            final long maxOrd = ordinalValues.getValueCount();
            if (maxOrd == 0) {
                emptyCollectorsUsed++;
                return new EmptyCollector();
            }

            if (executionMode.useSegmentOrdinals(maxOrd, precision)) {
                ordinalsCollectorsUsed++;
                return new OrdinalsCollector(counts, ordinalValues, bigArrays());
            }

            if (executionMode.isHeuristicBased()) {
                // if we could have used segment ordinals, and it was our heuristic that made the choice not to, increment the counter
                ordinalsCollectorsOverheadTooHigh++;
            }
        }
        stringHashingCollectorsUsed++;
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        final BinaryDocValues singleton = FieldData.unwrapSingleton(values);
        if (singleton != null) {
            return new DirectSingleValuesCollector(counts, MurmurHash3SingleValues.hash(singleton));
        } else {
            return new DirectMultiValuesCollector(counts, MurmurHash3MultiValues.hash(values));
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        postCollectLastCollector();

        collector = pickCollector(aggCtx.getLeafReaderContext());
        return collector;
    }

    private void postCollectLastCollector() throws IOException {
        if (collector != null) {
            try {
                collector.postCollect();
            } finally {
                collector.close();
                collector = null;
            }
        }
    }

    @Override
    protected void doPostCollection() throws IOException {
        postCollectLastCollector();
    }

    @Override
    public double metric(long owningBucketOrd) {
        return counts.cardinality(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (owningBucketOrdinal >= counts.maxOrd() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        // We need to build a copy because the returned Aggregation needs remain usable after
        // this Aggregator (and its HLL++ counters) is released.
        AbstractHyperLogLogPlusPlus copy = counts.clone(owningBucketOrdinal, BigArrays.NON_RECYCLING_INSTANCE);
        return new InternalCardinality(name, copy, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return InternalCardinality.empty(name, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(counts, collector);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("empty_collectors_used", emptyCollectorsUsed);
        add.accept("numeric_collectors_used", numericCollectorsUsed);
        add.accept("ordinals_collectors_used", ordinalsCollectorsUsed);
        add.accept("ordinals_collectors_overhead_too_high", ordinalsCollectorsOverheadTooHigh);
        add.accept("string_hashing_collectors_used", stringHashingCollectorsUsed);
    }

    private abstract static class Collector extends LeafBucketCollector implements Releasable {

        public abstract void postCollect() throws IOException;

    }

    private static class EmptyCollector extends Collector {

        @Override
        public void collect(int doc, long bucketOrd) {
            // no-op
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private abstract static class DirectCollector extends Collector {
        protected final HyperLogLogPlusPlus counts;

        DirectCollector(HyperLogLogPlusPlus counts) {
            this.counts = counts;
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() {
            // no-op: the HyperLogLogPlusPlus object is closed as part of the aggregator itself.
        }
    }

    private static class DirectMultiValuesCollector extends DirectCollector {
        private final MurmurHash3MultiValues hashes;

        DirectMultiValuesCollector(HyperLogLogPlusPlus counts, MurmurHash3MultiValues values) {
            super(counts);
            this.hashes = values;
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            if (hashes.advanceExact(doc)) {
                final int valueCount = hashes.count();
                for (int i = 0; i < valueCount; ++i) {
                    counts.collect(bucketOrd, hashes.nextValue());
                }
            }
        }
    }

    private static class DirectSingleValuesCollector extends DirectCollector {
        private final MurmurHash3SingleValues hashes;

        DirectSingleValuesCollector(HyperLogLogPlusPlus counts, MurmurHash3SingleValues values) {
            super(counts);
            this.hashes = values;
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            if (hashes.advanceExact(doc)) {
                counts.collect(bucketOrd, hashes.longValue());
            }
        }
    }

    static class OrdinalsCollector extends Collector {

        private static final long SHALLOW_FIXEDBITSET_SIZE = RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class);

        /**
         * Return an approximate memory overhead per bucket for this collector.
         */
        public static long memoryOverhead(long maxOrd) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF + SHALLOW_FIXEDBITSET_SIZE + (maxOrd + 7) / 8; // 1 bit per ord
        }

        private final BigArrays bigArrays;
        private final SortedSetDocValues values;
        private final int maxOrd;
        private final HyperLogLogPlusPlus counts;
        private ObjectArray<BitArray> visitedOrds;

        OrdinalsCollector(HyperLogLogPlusPlus counts, SortedSetDocValues values, BigArrays bigArrays) {
            if (values.getValueCount() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException();
            }
            maxOrd = (int) values.getValueCount();
            this.bigArrays = bigArrays;
            this.counts = counts;
            this.values = values;
            visitedOrds = bigArrays.newObjectArray(1);
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            if (values.advanceExact(doc)) {
                visitedOrds = bigArrays.grow(visitedOrds, bucketOrd + 1);
                BitArray bits = visitedOrds.get(bucketOrd);
                if (bits == null) {
                    bits = new BitArray(maxOrd, bigArrays);
                    visitedOrds.set(bucketOrd, bits);
                }
                for (int i = 0; i < values.docValueCount(); i++) {
                    long ord = values.nextOrd();
                    bits.set((int) ord);
                }
            }
        }

        @Override
        public void postCollect() throws IOException {
            try (BitArray allVisitedOrds = new BitArray(maxOrd, bigArrays)) {
                for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                    final BitArray bits = visitedOrds.get(bucket);
                    if (bits != null) {
                        allVisitedOrds.or(bits);
                    }
                }

                try (LongArray hashes = bigArrays.newLongArray(maxOrd, false)) {
                    final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                    for (long ord = allVisitedOrds.nextSetBit(0); ord < Long.MAX_VALUE; ord = ord + 1 < maxOrd
                        ? allVisitedOrds.nextSetBit(ord + 1)
                        : Long.MAX_VALUE) {
                        final BytesRef value = values.lookupOrd(ord);
                        MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                        hashes.set(ord, hash.h1);
                    }

                    for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                        final BitArray bits = visitedOrds.get(bucket);
                        if (bits != null) {
                            for (long ord = bits.nextSetBit(0); ord < Long.MAX_VALUE; ord = ord + 1 < maxOrd
                                ? bits.nextSetBit(ord + 1)
                                : Long.MAX_VALUE) {
                                counts.collect(bucket, hashes.get(ord));
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void close() {
            for (int i = 0; i < visitedOrds.size(); i++) {
                Releasables.close(visitedOrds.get(i));
            }
            Releasables.close(visitedOrds);
        }
    }

    /**
     * Representation of a list of hash values. There might be dups and there is no guarantee on the order.
     */
    private abstract static class MurmurHash3MultiValues {

        public abstract boolean advanceExact(int docId) throws IOException;

        public abstract int count();

        public abstract long nextValue() throws IOException;

        /**
         * Return a {@link MurmurHash3MultiValues} instance that computes hashes on the fly for each double value.
         */
        public static MurmurHash3MultiValues hash(SortedNumericDoubleValues values) {
            return new Double(values);
        }

        /**
         * Return a {@link MurmurHash3MultiValues} instance that computes hashes on the fly for each long value.
         */
        public static MurmurHash3MultiValues hash(SortedNumericDocValues values) {
            return new Long(values);
        }

        /**
         * Return a {@link MurmurHash3MultiValues} instance that computes hashes on the fly for each binary value.
         */
        public static MurmurHash3MultiValues hash(SortedBinaryDocValues values) {
            return new Bytes(values);
        }

        private static class Long extends MurmurHash3MultiValues {

            private final SortedNumericDocValues values;

            Long(SortedNumericDocValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int count() {
                return values.docValueCount();
            }

            @Override
            public long nextValue() throws IOException {
                return BitMixer.mix64(values.nextValue());
            }
        }

        private static class Double extends MurmurHash3MultiValues {

            private final SortedNumericDoubleValues values;

            Double(SortedNumericDoubleValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int count() {
                return values.docValueCount();
            }

            @Override
            public long nextValue() throws IOException {
                return BitMixer.mix64(java.lang.Double.doubleToLongBits(values.nextValue()));
            }
        }

        private static class Bytes extends MurmurHash3MultiValues {

            private final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

            private final SortedBinaryDocValues values;

            Bytes(SortedBinaryDocValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int count() {
                return values.docValueCount();
            }

            @Override
            public long nextValue() throws IOException {
                final BytesRef bytes = values.nextValue();
                MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hash);
                return hash.h1;
            }
        }
    }

    /**
     * Representation of a list of hash values. There might be dups and there is no guarantee on the order.
     */
    private abstract static class MurmurHash3SingleValues {

        public abstract boolean advanceExact(int docId) throws IOException;

        public abstract long longValue() throws IOException;

        /**
         * Return a {@link MurmurHash3MultiValues} instance that computes hashes on the fly for each double value.
         */
        public static MurmurHash3SingleValues hash(NumericDoubleValues values) {
            return new Double(values);
        }

        /**
         * Return a {@link MurmurHash3MultiValues} instance that computes hashes on the fly for each long value.
         */
        public static MurmurHash3SingleValues hash(NumericDocValues values) {
            return new Long(values);
        }

        /**
         * Return a {@link MurmurHash3MultiValues} instance that computes hashes on the fly for each binary value.
         */
        public static MurmurHash3SingleValues hash(BinaryDocValues values) {
            return new Bytes(values);
        }

        private static class Long extends MurmurHash3SingleValues {

            private final NumericDocValues values;

            Long(NumericDocValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public long longValue() throws IOException {
                return BitMixer.mix64(values.longValue());
            }
        }

        private static class Double extends MurmurHash3SingleValues {

            private final NumericDoubleValues values;

            Double(NumericDoubleValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public long longValue() throws IOException {
                return BitMixer.mix64(java.lang.Double.doubleToLongBits(values.doubleValue()));
            }
        }

        private static class Bytes extends MurmurHash3SingleValues {

            private final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

            private final BinaryDocValues values;

            Bytes(BinaryDocValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public long longValue() throws IOException {
                final BytesRef bytes = values.binaryValue();
                MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hash);
                return hash.h1;
            }
        }
    }
}
