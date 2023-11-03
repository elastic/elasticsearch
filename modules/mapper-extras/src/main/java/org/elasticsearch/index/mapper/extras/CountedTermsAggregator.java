/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.SuppressForbidden;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.BucketPriorityQueue;
import org.elasticsearch.search.aggregations.bucket.terms.BytesKeyedBucketOrds;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;
import static org.elasticsearch.search.aggregations.InternalOrder.isKeyOrder;

public class CountedTermsAggregator extends TermsAggregator {
    protected final CountedTermsAggregator.ResultStrategy<?, ?, ?> resultStrategy;
    protected final ValuesSource.Bytes.WithOrdinals valuesSource;

    private final CheckedSupplier<SortedSetDocValues, IOException> valuesSupplier;
    private final long valueCount;
    private final CountedGlobalOrds collectionStrategy;
    protected int segmentsWithSingleValuedOrds = 0;
    protected int segmentsWithMultiValuedOrds = 0;
    private final boolean showTermDocCountError;

    public interface GlobalOrdLookupFunction {
        BytesRef apply(long ord) throws IOException;
    }

    @SuppressWarnings("this-escape")
    public CountedTermsAggregator(
        String name,
        AggregatorFactories factories,
        Function<CountedTermsAggregator, ResultStrategy<?, ?, ?>> resultStrategy,
        ValuesSource.Bytes.WithOrdinals valuesSource,
        CheckedSupplier<SortedSetDocValues, IOException> valuesSupplier,
        BucketOrder order,
        DocValueFormat format,
        BucketCountThresholds bucketCountThresholds,
        AggregationContext context,
        Aggregator parent,
        SubAggCollectionMode collectionMode,
        boolean showTermDocCountError,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, bucketCountThresholds, order, format, collectionMode, metadata);
        this.showTermDocCountError = showTermDocCountError;
        this.resultStrategy = resultStrategy.apply(this); // ResultStrategy needs a reference to the Aggregator to do its job.
        this.valuesSource = valuesSource;
        this.valuesSupplier = valuesSupplier;
        this.valueCount = valuesSupplier.get().getValueCount();
        this.collectionStrategy = new CountedGlobalOrds(cardinality);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        SortedSetDocValues globalOrds = valuesSource.globalOrdinalsValues(aggCtx.getLeafReaderContext());
        collectionStrategy.globalOrdsReady(globalOrds);
        SortedDocValues singleValues = DocValues.unwrapSingleton(globalOrds);
        if (singleValues != null) {
            segmentsWithSingleValuedOrds++;
            return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, globalOrds) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (false == singleValues.advanceExact(doc)) {
                        return;
                    }
                    int globalOrd = singleValues.ordValue();
                    collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, globalOrd, sub);
                }
            });
        }
        segmentsWithMultiValuedOrds++;
        return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, globalOrds) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (false == globalOrds.advanceExact(doc)) {
                    return;
                }
                for (long globalOrd = globalOrds.nextOrd(); globalOrd != NO_MORE_ORDS; globalOrd = globalOrds.nextOrd()) {
                    collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, globalOrd, sub);
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
        // TODO: There is no need, it's always the same strategy
        // add.accept("collection_strategy", collectionStrategy.describe());
        add.accept("total_buckets", collectionStrategy.totalBuckets());
        add.accept("result_strategy", resultStrategy.describe());
        add.accept("segments_with_single_valued_ords", segmentsWithSingleValuedOrds);
        add.accept("segments_with_multi_valued_ords", segmentsWithMultiValuedOrds);
    }

    /**
     * This is used internally only, just for compare using global ordinal instead of term bytes in the PQ
     */
    static class OrdBucket extends InternalTerms.Bucket<CountedTermsAggregator.OrdBucket> {
        long globalOrd;

        OrdBucket(boolean showDocCountError, DocValueFormat format) {
            super(0, null, showDocCountError, 0, format);
        }

        @Override
        public int compareKey(CountedTermsAggregator.OrdBucket other) {
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

    @SuppressForbidden(
        reason = "enabled temporarily to avoid new module dependencies - will be removed once this moves to a dedicated module"
    )
    @Override
    protected void doClose() {
        // TODO: Implement using releasables
        try {
            IOUtils.close(resultStrategy, collectionStrategy);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    interface BucketInfoConsumer {
        void accept(long globalOrd, long bucketOrd, long docCount) throws IOException;
    }

    private class CountedGlobalOrds implements Closeable {
        private final BytesKeyedBucketOrds bucketOrds;

        private SortedSetDocValues segmentOrds;

        private CountedGlobalOrds(CardinalityUpperBound cardinality) {
            // TODO: Hardcoded to many. It failed with an assertion error when adding segment ords because it only expected cardinality
            // single?
            bucketOrds = BytesKeyedBucketOrds.build(bigArrays(), CardinalityUpperBound.MANY);
        }

        /**
         * The total number of buckets collected by this strategy.
         */
        long totalBuckets() {
            return bucketOrds.size();
        }

        /**
         * Called when the global ordinals are ready.
         */
        void globalOrdsReady(SortedSetDocValues globalOrds) {
            // TODO: Not sure this is correct...
            segmentOrds = globalOrds;
        }

        /**
         * Called once per unique document, global ordinal combination to
         * collect the bucket.
         *
         * @param owningBucketOrd the ordinal of the bucket that owns this collection
         * @param doc the doc id in to collect
         * @param globalOrd the global ordinal to collect
         * @param sub the sub-aggregators that that will collect the bucket data
         */
        void collectGlobalOrd(long owningBucketOrd, int doc, long globalOrd, LeafBucketCollector sub) throws IOException {
            // TODO: where do I get the segmentOrds? Is globalOrdsReady() the correct place?
            if (false == segmentOrds.advanceExact(doc)) {
                return;
            }
            for (long segmentOrd = segmentOrds.nextOrd(); segmentOrd != NO_MORE_ORDS; segmentOrd = segmentOrds.nextOrd()) {
                // TODO: Is the key (segmentOrd) correct?
                long bucketOrdinal = bucketOrds.add(segmentOrd, segmentOrds.lookupOrd(segmentOrd));
                if (bucketOrdinal < 0) { // already seen
                    bucketOrdinal = -1 - bucketOrdinal;
                    collectExistingBucket(sub, doc, bucketOrdinal);
                } else {
                    collectBucket(sub, doc, bucketOrdinal);
                }
            }
        }

        /**
         * Iterate all of the buckets. Implementations take into account
         * the {@link BucketCountThresholds}. In particular,
         * if the {@link BucketCountThresholds#getMinDocCount()} is 0 then
         * they'll make sure to iterate a bucket even if it was never
         * {{@link #collectGlobalOrd collected}.
         * If {@link BucketCountThresholds#getMinDocCount()} is not 0 then
         * they'll skip all global ords that weren't collected.
         */
        void forEach(long owningBucketOrd, BucketInfoConsumer consumer) throws IOException {
            // TODO: Is this correct?
            assert owningBucketOrd == 0;
            for (long globalOrd = 0; globalOrd < valueCount; globalOrd++) {
                long docCount = bucketDocCount(globalOrd);
                if (bucketCountThresholds.getMinDocCount() == 0 || docCount > 0) {
                    consumer.accept(globalOrd, globalOrd, docCount);
                }
            }
        }

        @Override
        public void close() {
            bucketOrds.close();
        }
    }

    // TODO: Merge this with the (only) subclass
    /**
     * Strategy for building results.
     */
    abstract class ResultStrategy<
        R extends InternalAggregation,
        B extends InternalMultiBucketAggregation.InternalBucket,
        TB extends InternalMultiBucketAggregation.InternalBucket> implements Closeable {

        private InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            if (valueCount == 0) { // no context in this reader
                InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
                for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                    results[ordIdx] = buildNoValuesResult(owningBucketOrds[ordIdx]);
                }
                return results;
            }

            B[][] topBucketsPreOrd = buildTopBucketsPerOrd(owningBucketOrds.length);
            long[] otherDocCount = new long[owningBucketOrds.length];
            CountedTermsAggregator.GlobalOrdLookupFunction lookupGlobalOrd = valuesSupplier.get()::lookupOrd;
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                final int size;
                if (bucketCountThresholds.getMinDocCount() == 0) {
                    // if minDocCount == 0 then we can end up with more buckets then maxBucketOrd() returns
                    size = (int) Math.min(valueCount, bucketCountThresholds.getShardSize());
                } else {
                    size = (int) Math.min(maxBucketOrd(), bucketCountThresholds.getShardSize());
                }
                PriorityQueue<TB> ordered = buildPriorityQueue(size);
                final int finalOrdIdx = ordIdx;
                CountedTermsAggregator.BucketUpdater<TB> updater = bucketUpdater(owningBucketOrds[ordIdx], lookupGlobalOrd);
                collectionStrategy.forEach(owningBucketOrds[ordIdx], new CountedTermsAggregator.BucketInfoConsumer() {
                    TB spare = null;

                    @Override
                    public void accept(long globalOrd, long bucketOrd, long docCount) throws IOException {
                        otherDocCount[finalOrdIdx] += docCount;
                        if (docCount >= bucketCountThresholds.getShardMinDocCount()) {
                            if (spare == null) {
                                spare = buildEmptyTemporaryBucket();
                            }
                            updater.updateBucket(spare, globalOrd, bucketOrd, docCount);
                            spare = ordered.insertWithOverflow(spare);
                        }
                    }
                });

                // Get the top buckets
                topBucketsPreOrd[ordIdx] = buildBuckets(ordered.size());
                for (int i = ordered.size() - 1; i >= 0; --i) {
                    topBucketsPreOrd[ordIdx][i] = convertTempBucketToRealBucket(ordered.pop(), lookupGlobalOrd);
                    otherDocCount[ordIdx] -= topBucketsPreOrd[ordIdx][i].getDocCount();
                }
            }

            buildSubAggs(topBucketsPreOrd);

            InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                results[ordIdx] = buildResult(owningBucketOrds[ordIdx], otherDocCount[ordIdx], topBucketsPreOrd[ordIdx]);
            }
            return results;
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
        abstract CountedTermsAggregator.BucketUpdater<TB> bucketUpdater(
            long owningBucketOrd,
            CountedTermsAggregator.GlobalOrdLookupFunction lookupGlobalOrd
        ) throws IOException;

        /**
         * Build a {@link PriorityQueue} to sort the buckets. After we've
         * collected all of the buckets we'll collect all entries in the queue.
         */
        abstract PriorityQueue<TB> buildPriorityQueue(int size);

        /**
         * Build an array to hold the "top" buckets for each ordinal.
         */
        abstract B[][] buildTopBucketsPerOrd(int size);

        /**
         * Build an array of buckets for a particular ordinal to collect the
         * results. The populated list is passed to {@link #buildResult}.
         */
        abstract B[] buildBuckets(int size);

        /**
         * Convert a temporary bucket into a real bucket.
         */
        abstract B convertTempBucketToRealBucket(TB temp, CountedTermsAggregator.GlobalOrdLookupFunction lookupGlobalOrd)
            throws IOException;

        /**
         * Build the sub-aggregations into the buckets. This will usually
         * delegate to {@link #buildSubAggsForAllBuckets}.
         */
        abstract void buildSubAggs(B[][] topBucketsPreOrd) throws IOException;

        /**
         * Turn the buckets into an aggregation result.
         */
        abstract R buildResult(long owningBucketOrd, long otherDocCount, B[] topBuckets);

        /**
         * Build an "empty" result. Only called if there isn't any data on this
         * shard.
         */
        abstract R buildEmptyResult();

        /**
         * Build an "empty" result for a particular bucket ordinal. Called when
         * there aren't any values for the field on this shard.
         */
        abstract R buildNoValuesResult(long owningBucketOrdinal);
    }

    interface BucketUpdater<TB extends InternalMultiBucketAggregation.InternalBucket> {
        void updateBucket(TB spare, long globalOrd, long bucketOrd, long docCount) throws IOException;
    }

    /**
     * Builds results for the standard {@code terms} aggregation.
     */
    class StandardTermsResults extends CountedTermsAggregator.ResultStrategy<
        StringTerms,
        StringTerms.Bucket,
        CountedTermsAggregator.OrdBucket> {
        @Override
        String describe() {
            return "terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return primary;
        }

        @Override
        StringTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new StringTerms.Bucket[size][];
        }

        @Override
        StringTerms.Bucket[] buildBuckets(int size) {
            return new StringTerms.Bucket[size];
        }

        @Override
        CountedTermsAggregator.OrdBucket buildEmptyTemporaryBucket() {
            return new CountedTermsAggregator.OrdBucket(showTermDocCountError, format);
        }

        @Override
        CountedTermsAggregator.BucketUpdater<CountedTermsAggregator.OrdBucket> bucketUpdater(
            long owningBucketOrd,
            CountedTermsAggregator.GlobalOrdLookupFunction lookupGlobalOrd
        ) throws IOException {
            return (spare, globalOrd, bucketOrd, docCount) -> {
                spare.globalOrd = globalOrd;
                spare.setBucketOrd(bucketOrd);
                spare.setDocCount(docCount);
            };
        }

        @Override
        PriorityQueue<CountedTermsAggregator.OrdBucket> buildPriorityQueue(int size) {
            return new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
        }

        @Override
        StringTerms.Bucket convertTempBucketToRealBucket(
            CountedTermsAggregator.OrdBucket temp,
            CountedTermsAggregator.GlobalOrdLookupFunction lookupGlobalOrd
        ) throws IOException {
            BytesRef term = BytesRef.deepCopyOf(lookupGlobalOrd.apply(temp.globalOrd));
            StringTerms.Bucket result = new StringTerms.Bucket(term, temp.getDocCount(), null, showTermDocCountError, 0, format);
            result.setBucketOrd(temp.getBucketOrd());
            result.setDocCountError(0);
            return result;
        }

        @Override
        void buildSubAggs(StringTerms.Bucket[][] topBucketsPreOrd) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPreOrd, b -> b.getBucketOrd(), (b, aggs) -> b.setAggregations(aggs));
        }

        @Override
        StringTerms buildResult(long owningBucketOrd, long otherDocCount, StringTerms.Bucket[] topBuckets) {
            final BucketOrder reduceOrder;
            if (isKeyOrder(order) == false) {
                reduceOrder = InternalOrder.key(true);
                Arrays.sort(topBuckets, reduceOrder.comparator());
            } else {
                reduceOrder = order;
            }
            return new StringTerms(
                name,
                reduceOrder,
                order,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                otherDocCount,
                Arrays.asList(topBuckets),
                null
            );
        }

        protected StringTerms buildEmptyTermsAggregation() {
            return new StringTerms(
                name,
                order,
                order,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                0,
                emptyList(),
                0L
            );
        }

        @Override
        StringTerms buildEmptyResult() {
            return buildEmptyTermsAggregation();
        }

        @Override
        StringTerms buildNoValuesResult(long owningBucketOrdinal) {
            return buildEmptyResult();
        }

        @Override
        public void close() {}
    }
}
