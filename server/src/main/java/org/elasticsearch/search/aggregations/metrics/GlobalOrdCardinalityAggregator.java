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

package org.elasticsearch.search.aggregations.metrics;


import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An aggregator that computes approximate counts of unique values.
 */
public class GlobalOrdCardinalityAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource.Bytes.WithOrdinals valuesSource;

    private HyperLogLogPlusPlusSparse counts;

    private final OrdinalsCollector collector;

    public GlobalOrdCardinalityAggregator(
            String name,
            ValuesSource.Bytes.WithOrdinals valuesSource,
            int precision,
            int maxOrd,
            SearchContext context,
            Aggregator parent,
            Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = valuesSource;
        this.counts = new HyperLogLogPlusPlusSparse(precision, context.bigArrays(), maxOrd, 1);
        this.collector = new OrdinalsCollector(counts, context.bigArrays(), maxOrd);
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        this.collector.set(valuesSource.globalOrdinalsValues(ctx));
        return collector;
    }

    @Override
    protected void doPostCollection() throws IOException {
        collector.postCollect();
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
        return new InternalCardinality(name, null, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(counts, collector);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        // maybe need to add something for global?
        super.collectDebugInfo(add);
        add.accept("empty_collectors_used", 0);
        add.accept("numeric_collectors_used", 0);
        add.accept("ordinals_collectors_used", 0);
        add.accept("ordinals_collectors_overhead_too_high", 0);
        add.accept("string_hashing_collectors_used", 0);
    }

    private static class OrdinalsCollector extends LeafBucketCollector implements Releasable {

        private final BigArrays bigArrays;
        private SortedSetDocValues values;
        private final int maxOrd;
        private final HyperLogLogPlusPlusSparse counts;
        private ObjectArray<BitArray> visitedOrds;

        OrdinalsCollector(HyperLogLogPlusPlusSparse counts, BigArrays bigArrays, int maxOrd) {
            this.maxOrd = maxOrd;
            this.bigArrays = bigArrays;
            this.counts = counts;
            visitedOrds = bigArrays.newObjectArray(1);
        }

        protected void set(SortedSetDocValues values) {
            this.values = values;
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            visitedOrds = bigArrays.grow(visitedOrds, bucketOrd + 1);
            BitArray bits = visitedOrds.get(bucketOrd);
            if (bits == null) {
                bits = new BitArray(maxOrd, bigArrays);
                visitedOrds.set(bucketOrd, bits);
            }
            if (values.advanceExact(doc)) {
                for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
                    bits.set((int) ord);
                }
            }
        }

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
                    for (long ord = allVisitedOrds.nextSetBit(0); ord < Long.MAX_VALUE;
                         ord = ord + 1 < maxOrd ? allVisitedOrds.nextSetBit(ord + 1) : Long.MAX_VALUE) {
                        final BytesRef value = values.lookupOrd(ord);
                        MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                        hashes.set(ord, hash.h1);
                    }

                    for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                        final BitArray bits = visitedOrds.get(bucket);
                        if (bits != null) {
                            for (long ord = bits.nextSetBit(0); ord < Long.MAX_VALUE;
                                 ord = ord + 1 < maxOrd ? bits.nextSetBit(ord + 1) : Long.MAX_VALUE) {
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
}
