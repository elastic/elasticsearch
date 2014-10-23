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

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.AbstractRandomAccessOrds;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalMapping;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.support.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Arrays;

/**
 * An aggregator of string values that relies on global ordinals in order to build buckets.
 */
public class GlobalOrdinalsStringTermsAggregator extends AbstractStringTermsAggregator {

    protected final ValuesSource.Bytes.WithOrdinals.FieldData valuesSource;
    protected final IncludeExclude includeExclude;

    protected RandomAccessOrds globalOrds;

    // TODO: cache the acceptedglobalValues per aggregation definition.
    // We can't cache this yet in ValuesSource, since ValuesSource is reused per field for aggs during the execution.
    // If aggs with same field, but different include/exclude are defined, then the last defined one will override the
    // first defined one.
    // So currently for each instance of this aggregator the acceptedglobalValues will be computed, this is unnecessary
    // especially if this agg is on a second layer or deeper.
    protected LongBitSet acceptedGlobalOrdinals;

    protected Collector collector;

    public GlobalOrdinalsStringTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Bytes.WithOrdinals.FieldData valuesSource, long estimatedBucketCount,
                                               long maxOrd, InternalOrder order, BucketCountThresholds bucketCountThresholds,
                                               IncludeExclude includeExclude, AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode collectionMode, boolean showTermDocCountError) {
        super(name, factories, maxOrd, aggregationContext, parent, order, bucketCountThresholds, collectionMode, showTermDocCountError);
        this.valuesSource = valuesSource;
        this.includeExclude = includeExclude;
    }

    protected long getBucketOrd(long termOrd) {
        return termOrd;
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    protected Collector newCollector(final RandomAccessOrds ords) {
        final SortedDocValues singleValues = DocValues.unwrapSingleton(ords);
        if (singleValues != null) {
            return new Collector() {
                @Override
                public void collect(int doc) throws IOException {
                    final int ord = singleValues.getOrd(doc);
                    if (ord >= 0) {
                        collectExistingBucket(doc, ord);
                    }
                }
            };
        } else {
            return new Collector() {
                public void collect(int doc) throws IOException {
                    ords.setDocument(doc);
                    final int numOrds = ords.cardinality();
                    for (int i = 0; i < numOrds; i++) {
                        final long globalOrd = ords.ordAt(i);
                        collectExistingBucket(doc, globalOrd);
                    }
                }
            };
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        globalOrds = valuesSource.globalOrdinalsValues();
        if (acceptedGlobalOrdinals != null) {
            globalOrds = new FilteredOrdinals(globalOrds, acceptedGlobalOrdinals);
        } else if (includeExclude != null) {
            acceptedGlobalOrdinals = includeExclude.acceptedGlobalOrdinals(globalOrds, valuesSource);
            globalOrds = new FilteredOrdinals(globalOrds, acceptedGlobalOrdinals);
        }
        collector = newCollector(globalOrds);
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        collector.collect(doc);
    }

    protected static void copy(BytesRef from, BytesRef to) {
        if (to.bytes.length < from.length) {
            to.bytes = new byte[ArrayUtil.oversize(from.length, RamUsageEstimator.NUM_BYTES_BYTE)];
        }
        to.offset = 0;
        to.length = from.length;
        System.arraycopy(from.bytes, from.offset, to.bytes, 0, from.length);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (globalOrds == null) { // no context in this reader
            return buildEmptyAggregation();
        }

        final int size;
        if (bucketCountThresholds.getMinDocCount() == 0) {
            // if minDocCount == 0 then we can end up with more buckets then maxBucketOrd() returns
            size = (int) Math.min(globalOrds.getValueCount(), bucketCountThresholds.getShardSize());
        } else {
            size = (int) Math.min(maxBucketOrd(), bucketCountThresholds.getShardSize());
        }
        long otherDocCount = 0;
        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator(this));
        OrdBucket spare = new OrdBucket(-1, 0, null, showTermDocCountError, 0);
        for (long globalTermOrd = 0; globalTermOrd < globalOrds.getValueCount(); ++globalTermOrd) {
            if (includeExclude != null && !acceptedGlobalOrdinals.get(globalTermOrd)) {
                continue;
            }
            final long bucketOrd = getBucketOrd(globalTermOrd);
            final int bucketDocCount = bucketOrd < 0 ? 0 : bucketDocCount(bucketOrd);
            if (bucketCountThresholds.getMinDocCount() > 0 && bucketDocCount == 0) {
                continue;
            }
            otherDocCount += bucketDocCount;
            spare.globalOrd = globalTermOrd;
            spare.bucketOrd = bucketOrd;
            spare.docCount = bucketDocCount;
            if (bucketCountThresholds.getShardMinDocCount() <= spare.docCount) {
                spare = (OrdBucket) ordered.insertWithOverflow(spare);
                if (spare == null) {
                    spare = new OrdBucket(-1, 0, null, showTermDocCountError, 0);
                }
            }
        }

        // Get the top buckets
        final InternalTerms.Bucket[] list = new InternalTerms.Bucket[ordered.size()];
        long survivingBucketOrds[] = new long[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final OrdBucket bucket = (OrdBucket) ordered.pop();
            survivingBucketOrds[i] = bucket.bucketOrd;
            BytesRef scratch = new BytesRef();
            copy(globalOrds.lookupOrd(bucket.globalOrd), scratch);
            list[i] = new StringTerms.Bucket(scratch, bucket.docCount, null, showTermDocCountError, 0);
            list[i].bucketOrd = bucket.bucketOrd;
            otherDocCount -= list[i].docCount;
        }
        //replay any deferred collections
        runDeferredCollections(survivingBucketOrds);
        
        //Now build the aggs
        for (int i = 0; i < list.length; i++) {
          Bucket bucket = list[i];
          bucket.aggregations = bucket.docCount == 0 ? bucketEmptyAggregations() : bucketAggregations(bucket.bucketOrd);
          bucket.docCountError = 0;
        }

        return new StringTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getShardSize(), bucketCountThresholds.getMinDocCount(), Arrays.asList(list), showTermDocCountError, 0, otherDocCount);
    }
    
    /** This is used internally only, just for compare using global ordinal instead of term bytes in the PQ */
    static class OrdBucket extends InternalTerms.Bucket {
        long globalOrd;

        OrdBucket(long globalOrd, long docCount, InternalAggregations aggregations, boolean showDocCountError, long docCountError) {
            super(docCount, aggregations, showDocCountError, docCountError);
            this.globalOrd = globalOrd;
        }

        @Override
        int compareTerm(Terms.Bucket other) {
            return Long.compare(globalOrd, ((OrdBucket)other).globalOrd);
        }

        @Override
        public String getKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Text getKeyAsText() {
            throw new UnsupportedOperationException();
        }

        @Override
        Object getKeyAsObject() {
            throw new UnsupportedOperationException();
        }

        @Override
        Bucket newBucket(long docCount, InternalAggregations aggs, long docCountError) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Number getKeyAsNumber() {
            throw new UnsupportedOperationException();
        }
    }

    private static interface Collector {
        void collect(int doc) throws IOException;
    }

    /**
     * Variant of {@link GlobalOrdinalsStringTermsAggregator} that rebases hashes in order to make them dense. Might be
     * useful in case few hashes are visited.
     */
    public static class WithHash extends GlobalOrdinalsStringTermsAggregator {

        private final LongHash bucketOrds;

        public WithHash(String name, AggregatorFactories factories, ValuesSource.Bytes.WithOrdinals.FieldData valuesSource, long estimatedBucketCount,
                        long maxOrd, InternalOrder order, BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude, AggregationContext aggregationContext,
                        Aggregator parent, SubAggCollectionMode collectionMode, boolean showTermDocCountError) {
            // Set maxOrd to estimatedBucketCount! To be conservative with memory.
            super(name, factories, valuesSource, estimatedBucketCount, estimatedBucketCount, order, bucketCountThresholds, includeExclude, aggregationContext, parent, collectionMode, showTermDocCountError);
            bucketOrds = new LongHash(estimatedBucketCount, aggregationContext.bigArrays());
        }

        protected Collector newCollector(final RandomAccessOrds ords) {
            final SortedDocValues singleValues = DocValues.unwrapSingleton(ords);
            if (singleValues != null) {
                return new Collector() {
                    @Override
                    public void collect(int doc) throws IOException {
                        final int globalOrd = singleValues.getOrd(doc);
                        if (globalOrd >= 0) {
                            long bucketOrd = bucketOrds.add(globalOrd);
                            if (bucketOrd < 0) {
                                bucketOrd = -1 - bucketOrd;
                                collectExistingBucket(doc, bucketOrd);
                            } else {
                                collectBucket(doc, bucketOrd);
                            }
                        }
                    }
                };
            } else {
                return new Collector() {
                    public void collect(int doc) throws IOException {
                        ords.setDocument(doc);
                        final int numOrds = ords.cardinality();
            for (int i = 0; i < numOrds; i++) {
                            final long globalOrd = ords.ordAt(i);
                long bucketOrd = bucketOrds.add(globalOrd);
                if (bucketOrd < 0) {
                    bucketOrd = -1 - bucketOrd;
                    collectExistingBucket(doc, bucketOrd);
                } else {
                    collectBucket(doc, bucketOrd);
                }
            }
        }
                };
            }
        }

        @Override
        protected long getBucketOrd(long termOrd) {
            return bucketOrds.find(termOrd);
        }

        @Override
        protected void doClose() {
            Releasables.close(bucketOrds);
        }

    }

    /**
     * Variant of {@link GlobalOrdinalsStringTermsAggregator} that resolves global ordinals post segment collection
     * instead of on the fly for each match.This is beneficial for low cardinality fields, because it can reduce
     * the amount of look-ups significantly.
     */
    public static class LowCardinality extends GlobalOrdinalsStringTermsAggregator {

        private final IntArray segmentDocCounts;

        private RandomAccessOrds segmentOrds;

        public LowCardinality(String name, AggregatorFactories factories, ValuesSource.Bytes.WithOrdinals.FieldData valuesSource, long estimatedBucketCount,
                              long maxOrd, InternalOrder order, BucketCountThresholds bucketCountThresholds, AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode collectionMode, boolean showTermDocCountError) {
            super(name, factories, valuesSource, estimatedBucketCount, maxOrd, order, bucketCountThresholds, null, aggregationContext, parent, collectionMode, showTermDocCountError);
            assert factories == null || factories.count() == 0;
            this.segmentDocCounts = bigArrays.newIntArray(maxOrd + 1, true);
        }

        // bucketOrd is ord + 1 to avoid a branch to deal with the missing ord
        protected Collector newCollector(final RandomAccessOrds ords) {
            final SortedDocValues singleValues = DocValues.unwrapSingleton(segmentOrds);
            if (singleValues != null) {
                return new Collector() {
        @Override
                    public void collect(int doc) throws IOException {
                        final int ord = singleValues.getOrd(doc);
                        segmentDocCounts.increment(ord + 1, 1);
                    }
                };
            } else {
                return new Collector() {
                    public void collect(int doc) throws IOException {
                        segmentOrds.setDocument(doc);
                        final int numOrds = segmentOrds.cardinality();
            for (int i = 0; i < numOrds; i++) {
                            final long segmentOrd = segmentOrds.ordAt(i);
                            segmentDocCounts.increment(segmentOrd + 1, 1);
                        }
                    }
                };
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            if (segmentOrds != null) {
                mapSegmentCountsToGlobalCounts();
            }

            globalOrds = valuesSource.globalOrdinalsValues();
            segmentOrds = valuesSource.ordinalsValues();
            collector = newCollector(segmentOrds);
            }

        @Override
        protected void doPostCollection() {
            if (segmentOrds != null) {
                mapSegmentCountsToGlobalCounts();
            }
        }

        @Override
        protected void doClose() {
            Releasables.close(segmentDocCounts);
        }

        private void mapSegmentCountsToGlobalCounts() {
            // There is no public method in Ordinals.Docs that allows for this mapping...
            // This is the cleanest way I can think of so far

            GlobalOrdinalMapping mapping;
            if (globalOrds instanceof GlobalOrdinalMapping) {
                mapping = (GlobalOrdinalMapping) globalOrds;
            } else {
                assert globalOrds.getValueCount() == segmentOrds.getValueCount();
                mapping = null;
            }
            for (long i = 1; i < segmentDocCounts.size(); i++) {
                final int inc = segmentDocCounts.set(i, 0);
                if (inc == 0) {
                    continue;
                }
                final long ord = i - 1; // remember we do +1 when counting
                final long globalOrd = mapping == null ? ord : mapping.getGlobalOrd(ord);
                try {
                    incrementBucketDocCount(globalOrd, inc);
                } catch (IOException e) {
                    throw ExceptionsHelper.convertToElastic(e);
                }
            }
        }
    }

    private static final class FilteredOrdinals extends AbstractRandomAccessOrds {

        private final RandomAccessOrds inner;
        private final LongBitSet accepted;

        private int cardinality;
        private long[] ords = new long[0];

        private FilteredOrdinals(RandomAccessOrds inner, LongBitSet accepted) {
            this.inner = inner;
            this.accepted = accepted;
        }

        @Override
        public long getValueCount() {
            return inner.getValueCount();
        }

        @Override
        public long ordAt(int index) {
            return ords[index];
        }

        @Override
        public void doSetDocument(int docId) {
            inner.setDocument(docId);
            final int innerCardinality = inner.cardinality();
            ords = ArrayUtil.grow(ords, innerCardinality);

            cardinality = 0;
            for (int slot = 0; slot < innerCardinality; slot++) {
                long ord = inner.ordAt(slot);
                if (accepted.get(ord)) {
                    ords[cardinality++] = ord;
                }
                }
            }

        @Override
        public int cardinality() {
            return cardinality;
        }

        @Override
        public BytesRef lookupOrd(long ord) {
            return inner.lookupOrd(ord);
        }
    }
}
