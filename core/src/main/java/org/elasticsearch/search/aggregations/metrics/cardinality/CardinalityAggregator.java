/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics.cardinality;

import com.carrotsearch.hppc.BitMixer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * An aggregator that computes approximate counts of unique values.
 */
public class CardinalityAggregator extends NumericMetricsAggregator.SingleValue {

    private final int precision;
    private final ValuesSource valuesSource;

    // Expensive to initialize, so we only initialize it when we have an actual value source
    @Nullable
    private HyperLogLogPlusPlus counts;

    private Collector collector;
    private ValueFormatter formatter;

    public CardinalityAggregator(String name, ValuesSource valuesSource, int precision, ValueFormatter formatter,
            AggregationContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.precision = precision;
        this.counts = valuesSource == null ? null : new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
        this.formatter = formatter;
    }

    @Override
    public boolean needsScores() {
        return valuesSource != null && valuesSource.needsScores();
    }

    private Collector pickCollector(LeafReaderContext ctx) throws IOException {
        if (valuesSource == null) {
            return new EmptyCollector();
        }

        if (valuesSource instanceof ValuesSource.Numeric) {
            ValuesSource.Numeric source = (ValuesSource.Numeric) valuesSource;
            MurmurHash3Values hashValues = source.isFloatingPoint() ? MurmurHash3Values.hash(source.doubleValues(ctx)) : MurmurHash3Values.hash(source.longValues(ctx));
            return new DirectCollector(counts, hashValues);
        }

        if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
            ValuesSource.Bytes.WithOrdinals source = (ValuesSource.Bytes.WithOrdinals) valuesSource;
            final RandomAccessOrds ordinalValues = source.ordinalsValues(ctx);
            final long maxOrd = ordinalValues.getValueCount();
            if (maxOrd == 0) {
                return new EmptyCollector();
            }

            final long ordinalsMemoryUsage = OrdinalsCollector.memoryOverhead(maxOrd);
            final long countsMemoryUsage = HyperLogLogPlusPlus.memoryUsage(precision);
            // only use ordinals if they don't increase memory usage by more than 25%
            if (ordinalsMemoryUsage < countsMemoryUsage / 4) {
                return new OrdinalsCollector(counts, ordinalValues, context.bigArrays());
            }
        }

        return new DirectCollector(counts, MurmurHash3Values.hash(valuesSource.bytesValues(ctx)));
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        postCollectLastCollector();

        collector = pickCollector(ctx);
        return collector;
    }

    private void postCollectLastCollector() {
        if (collector != null) {
            try {
                collector.postCollect();
                collector.close();
            } finally {
                collector = null;
            }
        }
    }

    @Override
    protected void doPostCollection() {
        postCollectLastCollector();
    }

    @Override
    public double metric(long owningBucketOrd) {
        return counts == null ? 0 : counts.cardinality(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (counts == null || owningBucketOrdinal >= counts.maxBucket() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        // We need to build a copy because the returned Aggregation needs remain usable after
        // this Aggregator (and its HLL++ counters) is released.
        HyperLogLogPlusPlus copy = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        copy.merge(0, counts, owningBucketOrdinal);
        return new InternalCardinality(name, copy, formatter, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalCardinality(name, null, formatter, pipelineAggregators(), metaData());
    }

    @Override
    protected void doClose() {
        Releasables.close(counts, collector);
    }

    private static abstract class Collector extends LeafBucketCollector implements Releasable {

        public abstract void postCollect();

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

    private static class DirectCollector extends Collector {

        private final MurmurHash3Values hashes;
        private final HyperLogLogPlusPlus counts;

        DirectCollector(HyperLogLogPlusPlus counts, MurmurHash3Values values) {
            this.counts = counts;
            this.hashes = values;
        }

        @Override
        public void collect(int doc, long bucketOrd) {
            hashes.setDocument(doc);
            final int valueCount = hashes.count();
            for (int i = 0; i < valueCount; ++i) {
                counts.collect(bucketOrd, hashes.valueAt(i));
            }
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

    private static class OrdinalsCollector extends Collector {

        private static final long SHALLOW_FIXEDBITSET_SIZE = RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class);

        /**
         * Return an approximate memory overhead per bucket for this collector.
         */
        public static long memoryOverhead(long maxOrd) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF + SHALLOW_FIXEDBITSET_SIZE + (maxOrd + 7) / 8; // 1 bit per ord
        }

        private final BigArrays bigArrays;
        private final RandomAccessOrds values;
        private final int maxOrd;
        private final HyperLogLogPlusPlus counts;
        private ObjectArray<FixedBitSet> visitedOrds;

        OrdinalsCollector(HyperLogLogPlusPlus counts, RandomAccessOrds values, BigArrays bigArrays) {
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
        public void collect(int doc, long bucketOrd) {
            visitedOrds = bigArrays.grow(visitedOrds, bucketOrd + 1);
            FixedBitSet bits = visitedOrds.get(bucketOrd);
            if (bits == null) {
                bits = new FixedBitSet(maxOrd);
                visitedOrds.set(bucketOrd, bits);
            }
            values.setDocument(doc);
            final int valueCount = values.cardinality();
            for (int i = 0; i < valueCount; ++i) {
                bits.set((int) values.ordAt(i));
            }
        }

        @Override
        public void postCollect() {
            final FixedBitSet allVisitedOrds = new FixedBitSet(maxOrd);
            for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                final FixedBitSet bits = visitedOrds.get(bucket);
                if (bits != null) {
                    allVisitedOrds.or(bits);
                }
            }

            final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();
            try (LongArray hashes = bigArrays.newLongArray(maxOrd, false)) {
                for (int ord = allVisitedOrds.nextSetBit(0); ord < DocIdSetIterator.NO_MORE_DOCS; ord = ord + 1 < maxOrd ? allVisitedOrds.nextSetBit(ord + 1) : DocIdSetIterator.NO_MORE_DOCS) {
                    final BytesRef value = values.lookupOrd(ord);
                    org.elasticsearch.common.hash.MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                    hashes.set(ord, hash.h1);
                }

                for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                    final FixedBitSet bits = visitedOrds.get(bucket);
                    if (bits != null) {
                        for (int ord = bits.nextSetBit(0); ord < DocIdSetIterator.NO_MORE_DOCS; ord = ord + 1 < maxOrd ? bits.nextSetBit(ord + 1) : DocIdSetIterator.NO_MORE_DOCS) {
                            counts.collect(bucket, hashes.get(ord));
                        }
                    }
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(visitedOrds);
        }

    }

    /**
     * Representation of a list of hash values. There might be dups and there is no guarantee on the order.
     */
    static abstract class MurmurHash3Values {

        public abstract void setDocument(int docId);

        public abstract int count();

        public abstract long valueAt(int index);

        /**
         * Return a {@link MurmurHash3Values} instance that returns each value as its hash.
         */
        public static MurmurHash3Values cast(final SortedNumericDocValues values) {
            return new MurmurHash3Values() {
                @Override
                public void setDocument(int docId) {
                    values.setDocument(docId);
                }
                @Override
                public int count() {
                    return values.count();
                }
                @Override
                public long valueAt(int index) {
                    return values.valueAt(index);
                }
            };
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each double value.
         */
        public static MurmurHash3Values hash(SortedNumericDoubleValues values) {
            return new Double(values);
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each long value.
         */
        public static MurmurHash3Values hash(SortedNumericDocValues values) {
            return new Long(values);
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each binary value.
         */
        public static MurmurHash3Values hash(SortedBinaryDocValues values) {
            return new Bytes(values);
        }

        private static class Long extends MurmurHash3Values {

            private final SortedNumericDocValues values;

            public Long(SortedNumericDocValues values) {
                this.values = values;
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.count();
            }

            @Override
            public long valueAt(int index) {
                return BitMixer.mix64(values.valueAt(index));
            }
        }

        private static class Double extends MurmurHash3Values {

            private final SortedNumericDoubleValues values;

            public Double(SortedNumericDoubleValues values) {
                this.values = values;
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.count();
            }

            @Override
            public long valueAt(int index) {
                return BitMixer.mix64(java.lang.Double.doubleToLongBits(values.valueAt(index)));
            }
        }

        private static class Bytes extends MurmurHash3Values {

            private final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();

            private final SortedBinaryDocValues values;

            public Bytes(SortedBinaryDocValues values) {
                this.values = values;
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
            }

            @Override
            public int count() {
                return values.count();
            }

            @Override
            public long valueAt(int index) {
                final BytesRef bytes = values.valueAt(index);
                org.elasticsearch.common.hash.MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hash);
                return hash.h1;
            }
        }
    }
}
