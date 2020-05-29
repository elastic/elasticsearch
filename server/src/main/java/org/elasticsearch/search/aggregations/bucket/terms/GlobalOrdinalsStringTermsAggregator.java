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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.LongPredicate;
import java.util.function.LongUnaryOperator;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

/**
 * An aggregator of string values that relies on global ordinals in order to build buckets.
 */
public class GlobalOrdinalsStringTermsAggregator extends AbstractStringTermsAggregator {

    protected final ValuesSource.Bytes.WithOrdinals valuesSource;

    // TODO: cache the acceptedglobalValues per aggregation definition.
    // We can't cache this yet in ValuesSource, since ValuesSource is reused per field for aggs during the execution.
    // If aggs with same field, but different include/exclude are defined, then the last defined one will override the
    // first defined one.
    // So currently for each instance of this aggregator the acceptedglobalValues will be computed, this is unnecessary
    // especially if this agg is on a second layer or deeper.
    protected final LongPredicate acceptedGlobalOrdinals;
    protected final long valueCount;
    protected final GlobalOrdLookupFunction lookupGlobalOrd;
    protected final CollectionStrategy collectionStrategy;

    public interface GlobalOrdLookupFunction {
        BytesRef apply(long ord) throws IOException;
    }

    public GlobalOrdinalsStringTermsAggregator(String name, AggregatorFactories factories,
                                               ValuesSource.Bytes.WithOrdinals valuesSource,
                                               BucketOrder order,
                                               DocValueFormat format,
                                               BucketCountThresholds bucketCountThresholds,
                                               IncludeExclude.OrdinalsFilter includeExclude,
                                               SearchContext context,
                                               Aggregator parent,
                                               boolean remapGlobalOrds,
                                               SubAggCollectionMode collectionMode,
                                               boolean showTermDocCountError,
                                               Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, order, format, bucketCountThresholds, collectionMode, showTermDocCountError, metadata);
        this.valuesSource = valuesSource;
        final IndexReader reader = context.searcher().getIndexReader();
        final SortedSetDocValues values = reader.leaves().size() > 0 ?
            valuesSource.globalOrdinalsValues(context.searcher().getIndexReader().leaves().get(0)) : DocValues.emptySortedSet();
        this.valueCount = values.getValueCount();
        this.lookupGlobalOrd = values::lookupOrd;
        this.acceptedGlobalOrdinals = includeExclude == null ? l -> true : includeExclude.acceptedGlobalOrdinals(values)::get;
        this.collectionStrategy = remapGlobalOrds ? new RemapGlobalOrds() : new DenseGlobalOrds();
    }

    String descriptCollectionStrategy() {
        return collectionStrategy.describe();
    }

    private SortedSetDocValues getGlobalOrds(LeafReaderContext ctx) throws IOException {
        return acceptedGlobalOrdinals == null ?
            valuesSource.globalOrdinalsValues(ctx) : new FilteredOrdinals(valuesSource.globalOrdinalsValues(ctx), acceptedGlobalOrdinals);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        SortedSetDocValues globalOrds = getGlobalOrds(ctx);
        collectionStrategy.globalOrdsReady(globalOrds);
        SortedDocValues singleValues = DocValues.unwrapSingleton(globalOrds);
        if (singleValues != null) {
            return new LeafBucketCollectorBase(sub, globalOrds) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    assert bucket == 0;
                    if (singleValues.advanceExact(doc)) {
                        int ord = singleValues.ordValue();
                        collectionStrategy.collectGlobalOrd(doc, ord, sub);
                    }
                }
            };
        }
        return new LeafBucketCollectorBase(sub, globalOrds) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (globalOrds.advanceExact(doc)) {
                    for (long globalOrd = globalOrds.nextOrd(); globalOrd != NO_MORE_ORDS; globalOrd = globalOrds.nextOrd()) {
                        collectionStrategy.collectGlobalOrd(doc, globalOrd, sub);
                    }
                }
            }
        };
    }

    protected static void copy(BytesRef from, BytesRef to) {
        if (to.bytes.length < from.length) {
            to.bytes = new byte[ArrayUtil.oversize(from.length, 1)];
        }
        to.offset = 0;
        to.length = from.length;
        System.arraycopy(from.bytes, from.offset, to.bytes, 0, from.length);
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        assert owningBucketOrds.length == 1 && owningBucketOrds[0] == 0;
        if (valueCount == 0) { // no context in this reader
            return new InternalAggregation[] {buildEmptyAggregation()};
        }

        final int size;
        if (bucketCountThresholds.getMinDocCount() == 0) {
            // if minDocCount == 0 then we can end up with more buckets then maxBucketOrd() returns
            size = (int) Math.min(valueCount, bucketCountThresholds.getShardSize());
        } else {
            size = (int) Math.min(maxBucketOrd(), bucketCountThresholds.getShardSize());
        }
        long[] otherDocCount = new long[1];
        BucketPriorityQueue<OrdBucket> ordered = new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
        collectionStrategy.forEach(new BucketInfoConsumer() {
            OrdBucket spare = null;

            @Override
            public void accept(long globalOrd, long bucketOrd, long docCount) {
                otherDocCount[0] += docCount;
                if (docCount >= bucketCountThresholds.getShardMinDocCount()) {
                    if (spare == null) {
                        spare = new OrdBucket(showTermDocCountError, format);
                    }
                    spare.globalOrd = globalOrd;
                    spare.bucketOrd = bucketOrd;
                    spare.docCount = docCount;
                    spare = ordered.insertWithOverflow(spare);
                    if (spare == null) {
                        consumeBucketsAndMaybeBreak(1);
                    }
                }
            }
        });

        // Get the top buckets
        final StringTerms.Bucket[] list = new StringTerms.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final OrdBucket bucket = ordered.pop();
            BytesRef scratch = new BytesRef();
            copy(lookupGlobalOrd.apply(bucket.globalOrd), scratch);
            list[i] = new StringTerms.Bucket(scratch, bucket.docCount, null, showTermDocCountError, 0, format);
            list[i].bucketOrd = bucket.bucketOrd;
            otherDocCount[0] -= list[i].docCount;
            list[i].docCountError = 0;
        }
        buildSubAggsForBuckets(list, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);

        return new InternalAggregation[] {
            new StringTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
                metadata(), format, bucketCountThresholds.getShardSize(), showTermDocCountError,
                otherDocCount[0], Arrays.asList(list), 0)
        };
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("collection_strategy", collectionStrategy.describe());
    }

    /**
     * This is used internally only, just for compare using global ordinal instead of term bytes in the PQ
     */
    static class OrdBucket extends InternalTerms.Bucket<OrdBucket> {
        long globalOrd;

        OrdBucket(boolean showDocCountError, DocValueFormat format) {
            super(0, null, showDocCountError, 0, format);
        }

        @Override
        public int compareKey(OrdBucket other) {
            return Long.compare(globalOrd, other.globalOrd);
        }

        @Override
        public String getKeyAsString() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Number getKeyAsNumber() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void writeTermTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected final XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    protected void doClose() {
        Releasables.close(collectionStrategy);
    }

    /**
     * Variant of {@link GlobalOrdinalsStringTermsAggregator} that resolves global ordinals post segment collection
     * instead of on the fly for each match.This is beneficial for low cardinality fields, because it can reduce
     * the amount of look-ups significantly.
     */
    static class LowCardinality extends GlobalOrdinalsStringTermsAggregator {

        private LongUnaryOperator mapping;
        private IntArray segmentDocCounts;

        LowCardinality(String name,
                       AggregatorFactories factories,
                       ValuesSource.Bytes.WithOrdinals valuesSource,
                       BucketOrder order,
                       DocValueFormat format,
                       BucketCountThresholds bucketCountThresholds,
                       SearchContext context,
                       Aggregator parent,
                       boolean forceDenseMode,
                       SubAggCollectionMode collectionMode,
                       boolean showTermDocCountError,
                       Map<String, Object> metadata) throws IOException {
            super(name, factories, valuesSource, order, format, bucketCountThresholds, null,
                context, parent, forceDenseMode, collectionMode, showTermDocCountError, metadata);
            assert factories == null || factories.countAggregators() == 0;
            this.segmentDocCounts = context.bigArrays().newIntArray(1, true);
        }

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                    final LeafBucketCollector sub) throws IOException {
            if (mapping != null) {
                mapSegmentCountsToGlobalCounts(mapping);
            }
            final SortedSetDocValues segmentOrds = valuesSource.ordinalsValues(ctx);
            segmentDocCounts = context.bigArrays().grow(segmentDocCounts, 1 + segmentOrds.getValueCount());
            assert sub == LeafBucketCollector.NO_OP_COLLECTOR;
            final SortedDocValues singleValues = DocValues.unwrapSingleton(segmentOrds);
            mapping = valuesSource.globalOrdinalsMapping(ctx);
            if (singleValues != null) {
                return new LeafBucketCollectorBase(sub, segmentOrds) {
                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        assert bucket == 0;
                        if (singleValues.advanceExact(doc)) {
                            final int ord = singleValues.ordValue();
                            segmentDocCounts.increment(ord + 1, 1);
                        }
                    }
                };
            } else {
                return new LeafBucketCollectorBase(sub, segmentOrds) {
                    @Override
                    public void collect(int doc, long bucket) throws IOException {
                        assert bucket == 0;
                        if (segmentOrds.advanceExact(doc)) {
                            for (long segmentOrd = segmentOrds.nextOrd(); segmentOrd != NO_MORE_ORDS; segmentOrd = segmentOrds.nextOrd()) {
                                segmentDocCounts.increment(segmentOrd + 1, 1);
                            }
                        }
                    }
                };
            }
        }

        @Override
        protected void doPostCollection() throws IOException {
            if (mapping != null) {
                mapSegmentCountsToGlobalCounts(mapping);
                mapping = null;
            }
        }

        @Override
        protected void doClose() {
            Releasables.close(segmentDocCounts);
        }

        private void mapSegmentCountsToGlobalCounts(LongUnaryOperator mapping) throws IOException {
            for (long i = 1; i < segmentDocCounts.size(); i++) {
                // We use set(...) here, because we need to reset the slow to 0.
                // segmentDocCounts get reused over the segments and otherwise counts would be too high.
                int inc = segmentDocCounts.set(i, 0);
                if (inc == 0) {
                    continue;
                }
                long ord = i - 1; // remember we do +1 when counting
                long globalOrd = mapping.applyAsLong(ord);
                incrementBucketDocCount(collectionStrategy.globalOrdToBucketOrd(globalOrd), inc);
            }
        }
    }

    private static final class FilteredOrdinals extends AbstractSortedSetDocValues {

        private final SortedSetDocValues inner;
        private final LongPredicate accepted;

        private FilteredOrdinals(SortedSetDocValues inner, LongPredicate accepted) {
            this.inner = inner;
            this.accepted = accepted;
        }

        @Override
        public long getValueCount() {
            return inner.getValueCount();
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            return inner.lookupOrd(ord);
        }

        @Override
        public long nextOrd() throws IOException {
            for (long ord = inner.nextOrd(); ord != NO_MORE_ORDS; ord = inner.nextOrd()) {
                if (accepted.test(ord)) {
                    return ord;
                }
            }
            return NO_MORE_ORDS;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (inner.advanceExact(target)) {
                for (long ord = inner.nextOrd(); ord != NO_MORE_ORDS; ord = inner.nextOrd()) {
                    if (accepted.test(ord)) {
                        // reset the iterator
                        boolean advanced = inner.advanceExact(target);
                        assert advanced;
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /**
     * Strategy for collecting global ordinals.
     * <p>
     * The {@link GlobalOrdinalsSignificantTermsAggregator} uses one of these
     * to collect the global ordinals by calling
     * {@link CollectionStrategy#collectGlobalOrd(int, long, LeafBucketCollector)}
     * for each global ordinal that it hits and then calling
     * {@link CollectionStrategy#forEach(BucketInfoConsumer)} once to iterate on
     * the results.
     */
    abstract class CollectionStrategy implements Releasable {
        /**
         * Short description of the collection mechanism added to the profile
         * output to help with debugging.
         */
        abstract String describe();
        /**
         * Called when the global ordinals are ready.
         */
        abstract void globalOrdsReady(SortedSetDocValues globalOrds);
        /**
         * Called once per unique document, global ordinal combination to
         * collect the bucket.
         *
         * @param doc the doc id in to collect
         * @param globalOrd the global ordinal to collect
         * @param sub the sub-aggregators that that will collect the bucket data
         */
        abstract void collectGlobalOrd(int doc, long globalOrd, LeafBucketCollector sub) throws IOException;
        /**
         * Convert a global ordinal into a bucket ordinal.
         */
        abstract long globalOrdToBucketOrd(long globalOrd);
        /**
         * Iterate all of the buckets. Implementations take into account
         * the {@link BucketCountThresholds}. In particular,
         * if the {@link BucketCountThresholds#getMinDocCount()} is 0 then
         * they'll make sure to iterate a bucket even if it was never 
         * {{@link #collectGlobalOrd(int, long, LeafBucketCollector) collected}.
         * If {@link BucketCountThresholds#getMinDocCount()} is not 0 then
         * they'll skip all global ords that weren't collected.
         */
        abstract void forEach(BucketInfoConsumer consumer) throws IOException;
    }
    interface BucketInfoConsumer {
        void accept(long globalOrd, long bucketOrd, long docCount) throws IOException;
    }


    /**
     * {@linkplain CollectionStrategy} that just uses the global ordinal as the
     * bucket ordinal.
     */
    class DenseGlobalOrds extends CollectionStrategy {
        @Override
        String describe() {
            return "dense";
        }

        @Override
        void globalOrdsReady(SortedSetDocValues globalOrds) {
            grow(globalOrds.getValueCount());
        }

        @Override
        void collectGlobalOrd(int doc, long globalOrd, LeafBucketCollector sub) throws IOException {
            collectExistingBucket(sub, doc, globalOrd);
        }

        @Override
        long globalOrdToBucketOrd(long globalOrd) {
            return globalOrd;
        }

        @Override
        void forEach(BucketInfoConsumer consumer) throws IOException {
            for (long globalOrd = 0; globalOrd < valueCount; globalOrd++) {
                if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                    continue;
                }
                long docCount = bucketDocCount(globalOrd);
                if (bucketCountThresholds.getMinDocCount() == 0 || docCount > 0) {
                    consumer.accept(globalOrd, globalOrd, docCount);
                }
            }
        }

        @Override
        public void close() {}
    }

    /**
     * {@linkplain CollectionStrategy} that uses a {@link LongHash} to map the
     * global ordinal into bucket ordinals. This uses more memory than
     * {@link DenseGlobalOrds} when collecting every ordinal, but significantly
     * less when collecting only a few.
     */
    class RemapGlobalOrds extends CollectionStrategy {
        private final LongHash bucketOrds = new LongHash(1, context.bigArrays());

        @Override
        String describe() {
            return "remap";
        }

        @Override
        void globalOrdsReady(SortedSetDocValues globalOrds) {}

        @Override
        void collectGlobalOrd(int doc, long globalOrd, LeafBucketCollector sub) throws IOException {
            long bucketOrd = bucketOrds.add(globalOrd);
            if (bucketOrd < 0) {
                bucketOrd = -1 - bucketOrd;
                collectExistingBucket(sub, doc, bucketOrd);
            } else {
                collectBucket(sub, doc, bucketOrd);
            }
        }

        @Override
        long globalOrdToBucketOrd(long globalOrd) {
            return bucketOrds.find(globalOrd);
        }

        @Override
        void forEach(BucketInfoConsumer consumer) throws IOException {
            if (bucketCountThresholds.getMinDocCount() == 0) {
                for (long globalOrd = 0; globalOrd < valueCount; globalOrd++) {
                    if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                        continue;
                    }
                    long bucketOrd = bucketOrds.find(globalOrd);
                    long docCount = bucketOrd < 0 ? 0 : bucketDocCount(bucketOrd);
                    consumer.accept(globalOrd, bucketOrd, docCount);
                }
            } else {
                for (long bucketOrd = 0; bucketOrd < bucketOrds.size(); bucketOrd++) {
                    long globalOrd = bucketOrds.get(bucketOrd);
                    if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                        continue;
                    }
                    consumer.accept(globalOrd, bucketOrd, bucketDocCount(bucketOrd));
                }
            }
        }
            

        @Override
        public void close() {
            bucketOrds.close();
        }
    }
}
