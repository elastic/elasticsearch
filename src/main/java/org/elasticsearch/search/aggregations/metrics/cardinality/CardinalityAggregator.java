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

import com.google.common.base.Preconditions;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.MurmurHash3Values;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;

/**
 * An aggregator that computes approximate counts of unique values.
 */
public class CardinalityAggregator extends MetricsAggregator.SingleValue {

    private final int precision;
    private final boolean rehash;
    private final ValuesSource valuesSource;

    // Expensive to initialize, so we only initialize it when we have an actual value source
    @Nullable
    private HyperLogLogPlusPlus counts;

    private Collector collector;

    public CardinalityAggregator(String name, long estimatedBucketsCount, ValuesSource valuesSource, boolean rehash,
                                 int precision, AggregationContext context, Aggregator parent) {
        super(name, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        this.rehash = rehash;
        this.precision = precision;
        this.counts = valuesSource == null ? null : new HyperLogLogPlusPlus(precision, bigArrays, estimatedBucketsCount);
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        postCollectLastCollector();
        collector = createCollector(reader);
    }

    private Collector createCollector(AtomicReaderContext reader) {

        // if rehash is false then the value source is either already hashed, or the user explicitly
        // requested not to hash the values (perhaps they already hashed the values themselves before indexing the doc)
        // so we can just work with the original value source as is
        if (!rehash) {
            LongValues hashValues = ((ValuesSource.Numeric) valuesSource).longValues();
            return new DirectCollector(counts, hashValues);
        }

        if (valuesSource instanceof ValuesSource.Numeric) {
            ValuesSource.Numeric source = (ValuesSource.Numeric) valuesSource;
            LongValues hashValues = source.isFloatingPoint() ? MurmurHash3Values.wrap(source.doubleValues()) : MurmurHash3Values.wrap(source.longValues());
            return new DirectCollector(counts, hashValues);
        }

        final BytesValues bytesValues = valuesSource.bytesValues();
        if (bytesValues instanceof BytesValues.WithOrdinals) {
            BytesValues.WithOrdinals values = (BytesValues.WithOrdinals) bytesValues;
            final long maxOrd = values.ordinals().getMaxOrd();
            if (maxOrd == 0) {
                return new EmptyCollector();
            }

            final long ordinalsMemoryUsage = OrdinalsCollector.memoryOverhead(maxOrd);
            final long countsMemoryUsage = HyperLogLogPlusPlus.memoryUsage(precision);
            // only use ordinals if they don't increase memory usage by more than 25%
            if (ordinalsMemoryUsage < countsMemoryUsage / 4) {
                return new OrdinalsCollector(counts, values, bigArrays);
            }
        }

        return new DirectCollector(counts, MurmurHash3Values.wrap(bytesValues));
    }


    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        collector.collect(doc, owningBucketOrdinal);
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
        return new InternalCardinality(name, copy);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalCardinality(name, null);
    }

    @Override
    protected void doClose() {
        Releasables.close(counts, collector);
    }

    private static interface Collector extends Releasable {

        void collect(int doc, long bucketOrd);

        void postCollect();

    }

    private static class EmptyCollector implements Collector {

        @Override
        public void collect(int doc, long bucketOrd) {
            // no-op
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() throws ElasticsearchException {
            // no-op
        }
    }

    private static class DirectCollector implements Collector {

        private final LongValues hashes;
        private final HyperLogLogPlusPlus counts;

        DirectCollector(HyperLogLogPlusPlus counts, LongValues values) {
            this.counts = counts;
            this.hashes = values;
        }

        @Override
        public void collect(int doc, long bucketOrd) {
            final int valueCount = hashes.setDocument(doc);
            for (int i = 0; i < valueCount; ++i) {
                counts.collect(bucketOrd, hashes.nextValue());
            }
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() throws ElasticsearchException {
            // no-op
        }

    }

    private static class OrdinalsCollector implements Collector {

        private static final long SHALLOW_FIXEDBITSET_SIZE = RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class);

        /**
         * Return an approximate memory overhead per bucket for this collector.
         */
        public static long memoryOverhead(long maxOrd) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF + SHALLOW_FIXEDBITSET_SIZE + (maxOrd + 7) / 8; // 1 bit per ord
        }

        private final BigArrays bigArrays;
        private final BytesValues.WithOrdinals values;
        private final Ordinals.Docs ordinals;
        private final int maxOrd;
        private final HyperLogLogPlusPlus counts;
        private ObjectArray<FixedBitSet> visitedOrds;

        OrdinalsCollector(HyperLogLogPlusPlus counts, BytesValues.WithOrdinals values, BigArrays bigArrays) {
            ordinals = values.ordinals();
            Preconditions.checkArgument(ordinals.getMaxOrd() <= Integer.MAX_VALUE);
            maxOrd = (int) ordinals.getMaxOrd();
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
            final int valueCount = ordinals.setDocument(doc);
            for (int i = 0; i < valueCount; ++i) {
                bits.set((int) ordinals.nextOrd());
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
                for (int ord = allVisitedOrds.nextSetBit(0); ord != -1; ord = ord + 1 < maxOrd ? allVisitedOrds.nextSetBit(ord + 1) : -1) {
                    final BytesRef value = values.getValueByOrd(ord);
                    org.elasticsearch.common.hash.MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                    hashes.set(ord, hash.h1);
                }

                for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                    final FixedBitSet bits = visitedOrds.get(bucket);
                    if (bits != null) {
                        for (int ord = bits.nextSetBit(0); ord != -1; ord = ord + 1 < maxOrd ? bits.nextSetBit(ord + 1) : -1) {
                            counts.collect(bucket, hashes.get(ord));
                        }
                    }
                }
            }
        }

        @Override
        public void close() throws ElasticsearchException {
            Releasables.close(visitedOrds);
        }

    }

}
