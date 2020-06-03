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
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.function.LongUnaryOperator;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

/**
 * An aggregator of string values that relies on global ordinals in order to build buckets.
 */
public class GlobalOrdinalsStringTermsAggregator extends AbstractStringTermsAggregator {
    protected final ResultStrategy<?, ?, ?> resultStrategy;
    protected final ValuesSource.Bytes.WithOrdinals valuesSource;

    // TODO: cache the acceptedglobalValues per aggregation definition.
    // We can't cache this yet in ValuesSource, since ValuesSource is reused per field for aggs during the execution.
    // If aggs with same field, but different include/exclude are defined, then the last defined one will override the
    // first defined one.
    // So currently for each instance of this aggregator the acceptedglobalValues will be computed, this is unnecessary
    // especially if this agg is on a second layer or deeper.
    private final LongPredicate acceptedGlobalOrdinals;
    private final long valueCount;
    private final GlobalOrdLookupFunction lookupGlobalOrd;
    protected final CollectionStrategy collectionStrategy;
    protected int segmentsWithSingleValuedOrds = 0;
    protected int segmentsWithMultiValuedOrds = 0;

    public interface GlobalOrdLookupFunction {
        BytesRef apply(long ord) throws IOException;
    }

    public GlobalOrdinalsStringTermsAggregator(
        String name,
        AggregatorFactories factories,
        Function<GlobalOrdinalsStringTermsAggregator, ResultStrategy<?, ?, ?>> resultStrategy,
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
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, order, format, bucketCountThresholds, collectionMode, showTermDocCountError, metadata);
        this.resultStrategy = resultStrategy.apply(this); // ResultStrategy needs a reference to the Aggregator to do its job.
        this.valuesSource = valuesSource;
        final IndexReader reader = context.searcher().getIndexReader();
        final SortedSetDocValues values = reader.leaves().size() > 0 ?
            valuesSource.globalOrdinalsValues(context.searcher().getIndexReader().leaves().get(0)) : DocValues.emptySortedSet();
        this.valueCount = values.getValueCount();
        this.lookupGlobalOrd = values::lookupOrd;
        this.acceptedGlobalOrdinals = includeExclude == null ? ALWAYS_TRUE : includeExclude.acceptedGlobalOrdinals(values)::get;
        this.collectionStrategy = remapGlobalOrds ? new RemapGlobalOrds() : new DenseGlobalOrds();
    }

    String descriptCollectionStrategy() {
        return collectionStrategy.describe();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        SortedSetDocValues globalOrds = valuesSource.globalOrdinalsValues(ctx);
        collectionStrategy.globalOrdsReady(globalOrds);
        SortedDocValues singleValues = DocValues.unwrapSingleton(globalOrds);
        if (singleValues != null) {
            segmentsWithSingleValuedOrds++;
            if (acceptedGlobalOrdinals == ALWAYS_TRUE) {
                /*
                 * Optimize when there isn't a filter because that is very
                 * common and marginally faster.
                 */
                return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, globalOrds) {
                    @Override
                    public void collect(int doc, long owningBucketOrd) throws IOException {
                        assert owningBucketOrd == 0;
                        if (false == singleValues.advanceExact(doc)) {
                            return;
                        }
                        int globalOrd = singleValues.ordValue();
                        collectionStrategy.collectGlobalOrd(doc, globalOrd, sub);
                    }
                });
            }
            return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, globalOrds) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    assert owningBucketOrd == 0;
                    if (false == singleValues.advanceExact(doc)) {
                        return;
                    }
                    int globalOrd = singleValues.ordValue();
                    if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                        return;
                    }
                    collectionStrategy.collectGlobalOrd(doc, globalOrd, sub);
                }
            });
        }
        segmentsWithMultiValuedOrds++;
        if (acceptedGlobalOrdinals == ALWAYS_TRUE) {
            /*
             * Optimize when there isn't a filter because that is very
             * common and marginally faster.
             */
            return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, globalOrds) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    assert owningBucketOrd == 0;
                    if (false == globalOrds.advanceExact(doc)) {
                        return;
                    }
                    for (long globalOrd = globalOrds.nextOrd(); globalOrd != NO_MORE_ORDS; globalOrd = globalOrds.nextOrd()) {
                        collectionStrategy.collectGlobalOrd(doc, globalOrd, sub);
                    }
                }
            });
        }
        return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, globalOrds) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                assert owningBucketOrd == 0;
                if (false == globalOrds.advanceExact(doc)) {
                    return;
                }
                for (long globalOrd = globalOrds.nextOrd(); globalOrd != NO_MORE_ORDS; globalOrd = globalOrds.nextOrd()) {
                    if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                        continue;
                    }
                    collectionStrategy.collectGlobalOrd(doc, globalOrd, sub);
                }
            }
        });
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return resultStrategy.buildAggregations(owningBucketOrds);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return resultStrategy.buildEmptyResult();
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("collection_strategy", collectionStrategy.describe());
        add.accept("result_strategy", resultStrategy.describe());
        add.accept("segments_with_single_valued_ords", segmentsWithSingleValuedOrds);
        add.accept("segments_with_multi_valued_ords", segmentsWithMultiValuedOrds);
        add.accept("has_filter", acceptedGlobalOrdinals != ALWAYS_TRUE);
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
        Releasables.close(resultStrategy, collectionStrategy);
    }

    /**
     * Variant of {@link GlobalOrdinalsStringTermsAggregator} that
     * resolves global ordinals post segment collection instead of on the fly
     * for each match.This is beneficial for low cardinality fields, because
     * it can reduce the amount of look-ups significantly.
     * <p>
     * This is only supported for the standard {@code terms} aggregation and
     * doesn't support {@code significant_terms} so this forces
     * {@link StandardTermsResults}.
     */
    static class LowCardinality extends GlobalOrdinalsStringTermsAggregator {

        private LongUnaryOperator mapping;
        private IntArray segmentDocCounts;

        LowCardinality(
            String name,
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
            Map<String, Object> metadata
        ) throws IOException {
            super(name, factories, a -> a.new StandardTermsResults(), valuesSource, order, format, bucketCountThresholds, null,
                context, parent, forceDenseMode, collectionMode, showTermDocCountError, metadata);
            assert factories == null || factories.countAggregators() == 0;
            this.segmentDocCounts = context.bigArrays().newIntArray(1, true);
        }

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
            if (mapping != null) {
                mapSegmentCountsToGlobalCounts(mapping);
            }
            final SortedSetDocValues segmentOrds = valuesSource.ordinalsValues(ctx);
            segmentDocCounts = context.bigArrays().grow(segmentDocCounts, 1 + segmentOrds.getValueCount());
            assert sub == LeafBucketCollector.NO_OP_COLLECTOR;
            final SortedDocValues singleValues = DocValues.unwrapSingleton(segmentOrds);
            mapping = valuesSource.globalOrdinalsMapping(ctx);
            // Dense mode doesn't support include/exclude so we don't have to check it here.
            if (singleValues != null) {
                segmentsWithSingleValuedOrds++;
                return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, segmentOrds) {
                    @Override
                    public void collect(int doc, long owningBucketOrd) throws IOException {
                        assert owningBucketOrd == 0;
                        if (false == singleValues.advanceExact(doc)) {
                            return;
                        }
                        int ord = singleValues.ordValue();
                        segmentDocCounts.increment(ord + 1, 1);
                    }
                });
            }
            segmentsWithMultiValuedOrds++;
            return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, segmentOrds) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    assert owningBucketOrd == 0;
                    if (false == segmentOrds.advanceExact(doc)) {
                        return;
                    }
                    for (long segmentOrd = segmentOrds.nextOrd(); segmentOrd != NO_MORE_ORDS; segmentOrd = segmentOrds.nextOrd()) {
                        segmentDocCounts.increment(segmentOrd + 1, 1);
                    }
                }
            });
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
            Releasables.close(resultStrategy, segmentDocCounts, collectionStrategy);
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

    /**
     * Strategy for collecting global ordinals.
     * <p>
     * The {@link GlobalOrdinalsStringTermsAggregator} uses one of these
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

    /**
     * Strategy for building results.
     */
    abstract class ResultStrategy<
        R extends InternalAggregation,
        B extends InternalMultiBucketAggregation.InternalBucket,
        TB extends InternalMultiBucketAggregation.InternalBucket> implements Releasable {

        private InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
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
            PriorityQueue<TB> ordered = buildPriorityQueue(size);
            collectionStrategy.forEach(new BucketInfoConsumer() {
                TB spare = null;

                @Override
                public void accept(long globalOrd, long bucketOrd, long docCount) throws IOException {
                    otherDocCount[0] += docCount;
                    if (docCount >= bucketCountThresholds.getShardMinDocCount()) {
                        if (spare == null) {
                            spare = buildEmptyTemporaryBucket();
                        }
                        updateBucket(spare, globalOrd, bucketOrd, docCount);
                        spare = ordered.insertWithOverflow(spare);
                    }
                }
            });

            // Get the top buckets
            B[] topBuckets = buildBuckets(ordered.size());
            for (int i = ordered.size() - 1; i >= 0; --i) {
                topBuckets[i] = convertTempBucketToRealBucket(ordered.pop());
            }
            buildSubAggs(topBuckets);

            return new InternalAggregation[] {
                buildResult(topBuckets)
            };
        }

        /**
         * Short description of the collection mechanism added to the profile
         * output to help with debugging.
         */
        abstract String describe();

        /**
         * Wrap the "standard" numeric terms collector to collect any more
         * information that this result type may need.
         */
        abstract LeafBucketCollector wrapCollector(LeafBucketCollector primary);

        /**
         * Build an empty temporary bucket.
         */
        abstract TB buildEmptyTemporaryBucket();

        /**
         * Update fields in {@code spare} to reflect information collected for
         * this bucket ordinal.
         */
        abstract void updateBucket(TB spare, long globalOrd, long bucketOrd, long docCount) throws IOException;

        /**
         * Build a {@link PriorityQueue} to sort the buckets. After we've
         * collected all of the buckets we'll collect all entries in the queue.
         */
        abstract PriorityQueue<TB> buildPriorityQueue(int size);

        /**
         * Build an array of buckets for a particular ordinal to collect the
         * results. The populated list is passed to {@link #buildResult}.
         */
        abstract B[] buildBuckets(int size);

        /**
         * Convert a temporary bucket into a real bucket.
         */
        abstract B convertTempBucketToRealBucket(TB temp) throws IOException;

        /**
         * Build the sub-aggregations into the buckets. This will usually
         * delegate to {@link #buildSubAggsForAllBuckets}.
         */
        abstract void buildSubAggs(B[] topBuckets) throws IOException;

        /**
         * Turn the buckets into an aggregation result.
         */
        abstract R buildResult(B[] topBuckets);

        /**
         * Build an "empty" result. Only called if there isn't any data on this
         * shard.
         */
        abstract R buildEmptyResult();
    }

    /**
     * Builds results for the standard {@code terms} aggregation.
     */
    class StandardTermsResults extends ResultStrategy<StringTerms, StringTerms.Bucket, OrdBucket> {
        private long otherDocCount;

        @Override
        String describe() {
            return "terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return primary;
        }

        @Override
        StringTerms.Bucket[] buildBuckets(int size) {
            return new StringTerms.Bucket[size];
        }

        @Override
        OrdBucket buildEmptyTemporaryBucket() {
            return new OrdBucket(showTermDocCountError, format);
        }

        @Override
        void updateBucket(OrdBucket spare, long globalOrd, long bucketOrd, long docCount) throws IOException {
            spare.globalOrd = globalOrd;
            spare.bucketOrd = bucketOrd;
            spare.docCount = docCount;
            otherDocCount += docCount;
        }

        @Override
        PriorityQueue<OrdBucket> buildPriorityQueue(int size) {
            return new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
        }

        StringTerms.Bucket convertTempBucketToRealBucket(OrdBucket temp) throws IOException {
            BytesRef term = BytesRef.deepCopyOf(lookupGlobalOrd.apply(temp.globalOrd));
            StringTerms.Bucket result = new StringTerms.Bucket(term, temp.docCount, null, showTermDocCountError, 0, format);
            result.bucketOrd = temp.bucketOrd;
            otherDocCount -= temp.docCount;
            result.docCountError = 0;
            return result;
        }

        @Override
        void buildSubAggs(StringTerms.Bucket[] topBuckets) throws IOException {
            buildSubAggsForBuckets(topBuckets, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        StringTerms buildResult(StringTerms.Bucket[] topBuckets) {
            return new StringTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
                metadata(), format, bucketCountThresholds.getShardSize(), showTermDocCountError,
                otherDocCount, Arrays.asList(topBuckets), 0);
        }

        @Override
        StringTerms buildEmptyResult() {
            return buildEmptyTermsAggregation();
        }

        @Override
        public void close() {}
    }

    /**
     * Builds results for the {@code significant_terms} aggregation.
     */
    class SignificantTermsResults extends ResultStrategy<
        SignificantStringTerms,
        SignificantStringTerms.Bucket,
        SignificantStringTerms.Bucket> {

        // TODO a reference to the factory is weird - probably should be reference to what we need from it.
        private final SignificantTermsAggregatorFactory termsAggFactory;
        private final SignificanceHeuristic significanceHeuristic;

        private long subsetSize = 0;

        SignificantTermsResults(SignificantTermsAggregatorFactory termsAggFactory, SignificanceHeuristic significanceHeuristic) {
            this.termsAggFactory = termsAggFactory;
            this.significanceHeuristic = significanceHeuristic;
        }

        @Override
        String describe() {
            return "terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return new LeafBucketCollectorBase(primary, null) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    super.collect(doc, owningBucketOrd);
                    subsetSize++;
                }
            };
        }

        @Override
        SignificantStringTerms.Bucket[] buildBuckets(int size) {
            return new SignificantStringTerms.Bucket[size];
        }

        @Override
        SignificantStringTerms.Bucket buildEmptyTemporaryBucket() {
            return new SignificantStringTerms.Bucket(new BytesRef(), 0, 0, 0, 0, null, format, 0);
        }

        @Override
        void updateBucket(SignificantStringTerms.Bucket spare, long globalOrd, long bucketOrd, long docCount) throws IOException {
            spare.bucketOrd = bucketOrd;
            oversizedCopy(lookupGlobalOrd.apply(globalOrd), spare.termBytes);
            spare.subsetDf = docCount;
            spare.subsetSize = subsetSize;
            spare.supersetDf = termsAggFactory.getBackgroundFrequency(spare.termBytes);
            spare.supersetSize = termsAggFactory.getSupersetNumDocs();
            /*
             * During shard-local down-selection we use subset/superset stats
             * that are for this shard only. Back at the central reducer these
             * properties will be updated with global stats.
             */
            spare.updateScore(significanceHeuristic);
        }

        @Override
        PriorityQueue<SignificantStringTerms.Bucket> buildPriorityQueue(int size) {
            return new BucketSignificancePriorityQueue<>(size);
        }

        @Override
        SignificantStringTerms.Bucket convertTempBucketToRealBucket(SignificantStringTerms.Bucket temp) throws IOException {
            return temp;
        }

        @Override
        void buildSubAggs(SignificantStringTerms.Bucket[] topBuckets) throws IOException {
            buildSubAggsForBuckets(topBuckets, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        SignificantStringTerms buildResult(SignificantStringTerms.Bucket[] topBuckets) {
            return new SignificantStringTerms(name, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
                metadata(), format, subsetSize, termsAggFactory.getSupersetNumDocs(), significanceHeuristic, Arrays.asList(topBuckets));
        }

        @Override
        SignificantStringTerms buildEmptyResult() {
            return buildEmptySignificantTermsAggregation(significanceHeuristic);
        }

        @Override
        public void close() {
            termsAggFactory.close();
        }

        /**
         * Copies the bytes from {@code from} into {@code to}, oversizing
         * the destination array if the bytes won't fit into the array.
         * <p>
         * This is fairly similar in spirit to
         * {@link BytesRef#deepCopyOf(BytesRef)} in that it is a way to read
         * bytes from a mutable {@link BytesRef} into
         * <strong>something</strong> that won't mutate out from under you.
         * Unlike {@linkplain BytesRef#deepCopyOf(BytesRef)} its designed to
         * be run over and over again into the same destination. In particular,
         * oversizing the destination bytes helps to keep from allocating
         * a bunch of little arrays over and over and over again.
         */
        private void oversizedCopy(BytesRef from, BytesRef to) {
            if (to.bytes.length < from.length) {
                to.bytes = new byte[ArrayUtil.oversize(from.length, 1)];
            }
            to.offset = 0;
            to.length = from.length;
            System.arraycopy(from.bytes, from.offset, to.bytes, 0, from.length);
        }
    }

    /**
     * Predicate used for {@link #acceptedGlobalOrdinals} if there is no filter.
     */
    private static final LongPredicate ALWAYS_TRUE = l -> true;
}
