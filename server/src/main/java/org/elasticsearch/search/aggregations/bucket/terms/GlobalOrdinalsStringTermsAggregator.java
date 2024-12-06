/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.util.ObjectArrayPriorityQueue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.SignificanceLookup.BackgroundFrequencyForBytes;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.function.LongUnaryOperator;

import static org.elasticsearch.search.aggregations.InternalOrder.isKeyOrder;

/**
 * An aggregator of string values that relies on global ordinals in order to build buckets.
 */
public class GlobalOrdinalsStringTermsAggregator extends AbstractStringTermsAggregator {
    protected final ResultStrategy<?, ?, ?> resultStrategy;
    protected final ValuesSource.Bytes.WithOrdinals valuesSource;

    private final LongPredicate acceptedGlobalOrdinals;

    private final CheckedSupplier<SortedSetDocValues, IOException> valuesSupplier;
    private final long valueCount;
    protected final CollectionStrategy collectionStrategy;
    protected int segmentsWithSingleValuedOrds = 0;
    protected int segmentsWithMultiValuedOrds = 0;

    public interface GlobalOrdLookupFunction {
        BytesRef apply(long ord) throws IOException;
    }

    @SuppressWarnings("this-escape")
    public GlobalOrdinalsStringTermsAggregator(
        String name,
        AggregatorFactories factories,
        Function<GlobalOrdinalsStringTermsAggregator, ResultStrategy<?, ?, ?>> resultStrategy,
        ValuesSource.Bytes.WithOrdinals valuesSource,
        CheckedSupplier<SortedSetDocValues, IOException> valuesSupplier,
        BucketOrder order,
        DocValueFormat format,
        BucketCountThresholds bucketCountThresholds,
        LongPredicate acceptedOrds,
        AggregationContext context,
        Aggregator parent,
        boolean remapGlobalOrds,
        SubAggCollectionMode collectionMode,
        boolean showTermDocCountError,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata,
        boolean excludeDeletedDocs
    ) throws IOException {
        super(name, factories, context, parent, order, format, bucketCountThresholds, collectionMode, showTermDocCountError, metadata);
        this.resultStrategy = resultStrategy.apply(this); // ResultStrategy needs a reference to the Aggregator to do its job.
        this.valuesSource = valuesSource;
        this.valuesSupplier = valuesSupplier;
        this.valueCount = valuesSupplier.get().getValueCount();
        this.acceptedGlobalOrdinals = acceptedOrds;
        if (remapGlobalOrds) {
            this.collectionStrategy = new RemapGlobalOrds<>(this.resultStrategy, cardinality, excludeDeletedDocs);
        } else {
            this.collectionStrategy = cardinality.map(estimate -> {
                if (estimate > 1) {
                    // This is a 500 class error, because we should never be able to reach it.
                    throw new AggregationExecutionException("Dense ords don't know how to collect from many buckets");
                }
                return new DenseGlobalOrds<>(this.resultStrategy, excludeDeletedDocs);
            });
        }
    }

    String descriptCollectionStrategy() {
        return collectionStrategy.describe();
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        SortedSetDocValues globalOrds = valuesSource.globalOrdinalsValues(aggCtx.getLeafReaderContext());
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
                        if (false == singleValues.advanceExact(doc)) {
                            return;
                        }
                        int globalOrd = singleValues.ordValue();
                        collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, globalOrd, sub);
                    }
                });
            }
            return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, globalOrds) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (false == singleValues.advanceExact(doc)) {
                        return;
                    }
                    int globalOrd = singleValues.ordValue();
                    if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                        return;
                    }
                    collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, globalOrd, sub);
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
                    if (false == globalOrds.advanceExact(doc)) {
                        return;
                    }
                    for (int i = 0; i < globalOrds.docValueCount(); i++) {
                        long globalOrd = globalOrds.nextOrd();
                        collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, globalOrd, sub);
                    }
                }
            });
        }
        return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, globalOrds) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (false == globalOrds.advanceExact(doc)) {
                    return;
                }
                for (int i = 0; i < globalOrds.docValueCount(); i++) {
                    long globalOrd = globalOrds.nextOrd();
                    if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                        continue;
                    }
                    collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, globalOrd, sub);
                }
            }
        });
    }

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        if (valueCount == 0) { // no context in this reader
            return GlobalOrdinalsStringTermsAggregator.this.buildAggregations(
                Math.toIntExact(owningBucketOrds.size()),
                ordIdx -> resultStrategy.buildNoValuesResult(owningBucketOrds.get(ordIdx))
            );
        }
        return collectionStrategy.buildAggregations(owningBucketOrds);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return resultStrategy.buildEmptyResult();
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("collection_strategy", collectionStrategy.describe());
        add.accept("total_buckets", collectionStrategy.totalBuckets());
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
        private LongArray segmentDocCounts;
        protected int segmentsWithoutValues = 0;

        LowCardinality(
            String name,
            AggregatorFactories factories,
            Function<GlobalOrdinalsStringTermsAggregator, ResultStrategy<?, ?, ?>> resultStrategy,
            ValuesSource.Bytes.WithOrdinals valuesSource,
            CheckedSupplier<SortedSetDocValues, IOException> valuesSupplier,
            BucketOrder order,
            DocValueFormat format,
            BucketCountThresholds bucketCountThresholds,
            AggregationContext context,
            Aggregator parent,
            boolean remapGlobalOrds,
            SubAggCollectionMode collectionMode,
            boolean showTermDocCountError,
            Map<String, Object> metadata,
            boolean excludeDeletedDocs
        ) throws IOException {
            super(
                name,
                factories,
                resultStrategy,
                valuesSource,
                valuesSupplier,
                order,
                format,
                bucketCountThresholds,
                ALWAYS_TRUE,
                context,
                parent,
                remapGlobalOrds,
                collectionMode,
                showTermDocCountError,
                CardinalityUpperBound.ONE,
                metadata,
                excludeDeletedDocs
            );
            assert factories == null || factories.countAggregators() == 0;
            this.segmentDocCounts = context.bigArrays().newLongArray(1, true);
        }

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
            if (mapping != null) {
                mapSegmentCountsToGlobalCounts(mapping);
            }
            final SortedSetDocValues segmentOrds = valuesSource.ordinalsValues(aggCtx.getLeafReaderContext());
            mapping = valuesSource.globalOrdinalsMapping(aggCtx.getLeafReaderContext());
            if (segmentOrds.getValueCount() == 0) {
                segmentsWithoutValues++;
                return LeafBucketCollector.NO_OP_COLLECTOR;
            }
            segmentDocCounts = bigArrays().grow(segmentDocCounts, 1 + segmentOrds.getValueCount());
            assert sub.isNoop();
            final SortedDocValues singleValues = DocValues.unwrapSingleton(segmentOrds);
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
                        int docCount = docCountProvider.getDocCount(doc);
                        segmentDocCounts.increment(ord + 1, docCount);
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
                    for (int i = 0; i < segmentOrds.docValueCount(); i++) {
                        long segmentOrd = segmentOrds.nextOrd();
                        int docCount = docCountProvider.getDocCount(doc);
                        segmentDocCounts.increment(segmentOrd + 1, docCount);
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
        public void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("segments_without_values", segmentsWithoutValues);
        }

        @Override
        protected void doClose() {
            Releasables.close(resultStrategy, segmentDocCounts, collectionStrategy);
        }

        private void mapSegmentCountsToGlobalCounts(LongUnaryOperator mapping) throws IOException {
            for (long i = 1; i < segmentDocCounts.size(); i++) {
                // We use set(...) here, because we need to reset the slow to 0.
                // segmentDocCounts get reused over the segments and otherwise counts would be too high.
                long inc = segmentDocCounts.getAndSet(i, 0);
                if (inc == 0) {
                    continue;
                }
                long ord = i - 1; // remember we do +1 when counting
                long globalOrd = mapping.applyAsLong(ord);
                incrementBucketDocCount(collectionStrategy.globalOrdToBucketOrd(0, globalOrd), inc);
            }
        }
    }

    /**
     * Strategy for collecting global ordinals.
     * <p>
     * The {@link GlobalOrdinalsStringTermsAggregator} uses one of these
     * to collect the global ordinals by calling
     * {@link CollectionStrategy#collectGlobalOrd} for each global ordinal
     * that it hits and then calling {@link CollectionStrategy#buildAggregations}
     * to generate the results.
     */
    abstract static class CollectionStrategy implements Releasable {
        /**
         * Short description of the collection mechanism added to the profile
         * output to help with debugging.
         */
        abstract String describe();

        /**
         * The total number of buckets collected by this strategy.
         */
        abstract long totalBuckets();

        /**
         * Called when the global ordinals are ready.
         */
        abstract void globalOrdsReady(SortedSetDocValues globalOrds);

        /**
         * Called once per unique document, global ordinal combination to
         * collect the bucket.
         *
         * @param owningBucketOrd the ordinal of the bucket that owns this collection
         * @param doc the doc id in to collect
         * @param globalOrd the global ordinal to collect
         * @param sub the sub-aggregators that that will collect the bucket data
         */
        abstract void collectGlobalOrd(long owningBucketOrd, int doc, long globalOrd, LeafBucketCollector sub) throws IOException;

        /**
         * Convert a global ordinal into a bucket ordinal.
         */
        abstract long globalOrdToBucketOrd(long owningBucketOrd, long globalOrd);

        /**
         * Create the aggregation result
         */
        abstract InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException;
    }

    interface BucketInfoConsumer {
        void accept(long globalOrd, long bucketOrd, long docCount) throws IOException;
    }

    /**
     * {@linkplain CollectionStrategy} that just uses the global ordinal as the
     * bucket ordinal.
     */
    class DenseGlobalOrds<
        R extends InternalAggregation,
        B extends InternalMultiBucketAggregation.InternalBucket,
        TB extends InternalMultiBucketAggregation.InternalBucket> extends CollectionStrategy {

        private final boolean excludeDeletedDocs;
        private final ResultStrategy<R, B, TB> collectionStrategy;

        DenseGlobalOrds(ResultStrategy<R, B, TB> collectionStrategy, boolean excludeDeletedDocs) {
            this.excludeDeletedDocs = excludeDeletedDocs;
            this.collectionStrategy = collectionStrategy;
        }

        @Override
        String describe() {
            return "dense";
        }

        @Override
        long totalBuckets() {
            return valueCount;
        }

        @Override
        void globalOrdsReady(SortedSetDocValues globalOrds) {
            grow(globalOrds.getValueCount());
        }

        @Override
        void collectGlobalOrd(long owningBucketOrd, int doc, long globalOrd, LeafBucketCollector sub) throws IOException {
            assert owningBucketOrd == 0;
            collectExistingBucket(sub, doc, globalOrd);
        }

        @Override
        long globalOrdToBucketOrd(long owningBucketOrd, long globalOrd) {
            assert owningBucketOrd == 0;
            return globalOrd;
        }

        private void collect(BucketInfoConsumer consumer) throws IOException {
            if (excludeDeletedDocs) {
                forEachExcludeDeletedDocs(consumer);
            } else {
                forEachAllowDeletedDocs(consumer);
            }
        }

        private void forEachAllowDeletedDocs(BucketInfoConsumer consumer) throws IOException {
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

        /**
         * Excludes deleted docs in the results by cross-checking with liveDocs.
         */
        private void forEachExcludeDeletedDocs(BucketInfoConsumer consumer) throws IOException {
            try (LongHash accepted = new LongHash(20, bigArrays())) {
                for (LeafReaderContext ctx : searcher().getTopReaderContext().leaves()) {
                    LeafReader reader = ctx.reader();
                    Bits liveDocs = reader.getLiveDocs();
                    SortedSetDocValues globalOrds = null;
                    for (int docId = 0; docId < reader.maxDoc(); ++docId) {
                        if (liveDocs == null || liveDocs.get(docId)) {  // document is not deleted
                            globalOrds = globalOrds == null ? valuesSource.globalOrdinalsValues(ctx) : globalOrds;
                            if (globalOrds.advanceExact(docId)) {
                                for (int i = 0; i < globalOrds.docValueCount(); i++) {
                                    long globalOrd = globalOrds.nextOrd();
                                    if (accepted.find(globalOrd) >= 0) {
                                        continue;
                                    }
                                    if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                                        continue;
                                    }
                                    long docCount = bucketDocCount(globalOrd);
                                    if (bucketCountThresholds.getMinDocCount() == 0 || docCount > 0) {
                                        consumer.accept(globalOrd, globalOrd, docCount);
                                        accepted.add(globalOrd);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void close() {}

        @Override
        InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
            assert owningBucketOrds.size() == 1 && owningBucketOrds.get(0) == 0;
            try (
                LongArray otherDocCount = bigArrays().newLongArray(1, true);
                ObjectArray<B[]> topBucketsPreOrd = collectionStrategy.buildTopBucketsPerOrd(1)
            ) {
                GlobalOrdLookupFunction lookupGlobalOrd = valuesSupplier.get()::lookupOrd;
                final int size = (int) Math.min(valueCount, bucketCountThresholds.getShardSize());
                try (ObjectArrayPriorityQueue<BucketAndOrd<TB>> ordered = collectionStrategy.buildPriorityQueue(size)) {
                    BucketUpdater<TB> updater = collectionStrategy.bucketUpdater(0, lookupGlobalOrd);
                    collect(new BucketInfoConsumer() {
                        BucketAndOrd<TB> spare = null;

                        @Override
                        public void accept(long globalOrd, long bucketOrd, long docCount) throws IOException {
                            otherDocCount.increment(0, docCount);
                            if (docCount >= bucketCountThresholds.getShardMinDocCount()) {
                                if (spare == null) {
                                    checkRealMemoryCBForInternalBucket();
                                    spare = new BucketAndOrd<>(collectionStrategy.buildEmptyTemporaryBucket());
                                }
                                spare.ord = bucketOrd;
                                updater.updateBucket(spare.bucket, globalOrd, docCount);
                                spare = ordered.insertWithOverflow(spare);
                            }
                        }
                    });

                    // Get the top buckets
                    int orderedSize = (int) ordered.size();
                    try (LongArray ordsArray = bigArrays().newLongArray(orderedSize)) {
                        B[] buckets = collectionStrategy.buildBuckets(orderedSize);
                        for (int i = orderedSize - 1; i >= 0; --i) {
                            checkRealMemoryCBForInternalBucket();
                            BucketAndOrd<TB> bucketAndOrd = ordered.pop();
                            B bucket = collectionStrategy.convertTempBucketToRealBucket(bucketAndOrd.bucket, lookupGlobalOrd);
                            ordsArray.set(i, bucketAndOrd.ord);
                            buckets[i] = bucket;
                            otherDocCount.increment(0, -bucket.getDocCount());
                        }
                        topBucketsPreOrd.set(0, buckets);
                        collectionStrategy.buildSubAggs(topBucketsPreOrd, ordsArray);
                    }
                }
                return GlobalOrdinalsStringTermsAggregator.this.buildAggregations(
                    Math.toIntExact(owningBucketOrds.size()),
                    ordIdx -> collectionStrategy.buildResult(
                        owningBucketOrds.get(ordIdx),
                        otherDocCount.get(ordIdx),
                        topBucketsPreOrd.get(ordIdx)
                    )
                );
            }
        }
    }

    /**
     * {@linkplain CollectionStrategy} that uses a {@link LongHash} to map the
     * global ordinal into bucket ordinals. This uses more memory than
     * {@link DenseGlobalOrds} when collecting every ordinal, but significantly
     * less when collecting only a few.
     */
    private class RemapGlobalOrds<
        R extends InternalAggregation,
        B extends InternalMultiBucketAggregation.InternalBucket,
        TB extends InternalMultiBucketAggregation.InternalBucket> extends CollectionStrategy {
        private final LongKeyedBucketOrds bucketOrds;
        private final boolean excludeDeletedDocs;
        private final ResultStrategy<R, B, TB> collectionStrategy;

        private RemapGlobalOrds(
            ResultStrategy<R, B, TB> collectionStrategy,
            CardinalityUpperBound cardinality,
            boolean excludeDeletedDocs
        ) {
            bucketOrds = LongKeyedBucketOrds.buildForValueRange(bigArrays(), cardinality, 0, valueCount - 1);
            this.excludeDeletedDocs = excludeDeletedDocs;
            this.collectionStrategy = collectionStrategy;
        }

        @Override
        String describe() {
            return "remap using " + bucketOrds.decribe();
        }

        @Override
        long totalBuckets() {
            return bucketOrds.size();
        }

        @Override
        void globalOrdsReady(SortedSetDocValues globalOrds) {}

        @Override
        void collectGlobalOrd(long owningBucketOrd, int doc, long globalOrd, LeafBucketCollector sub) throws IOException {
            long bucketOrd = bucketOrds.add(owningBucketOrd, globalOrd);
            if (bucketOrd < 0) {
                bucketOrd = -1 - bucketOrd;
                collectExistingBucket(sub, doc, bucketOrd);
            } else {
                collectBucket(sub, doc, bucketOrd);
            }
        }

        @Override
        long globalOrdToBucketOrd(long owningBucketOrd, long globalOrd) {
            return bucketOrds.find(owningBucketOrd, globalOrd);
        }

        private void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException {
            if (excludeDeletedDocs) {
                forEachExcludeDeletedDocs(owningBucketOrd);
            } else if (bucketCountThresholds.getMinDocCount() == 0) {
                for (long globalOrd = 0; globalOrd < valueCount; globalOrd++) {
                    if (acceptedGlobalOrdinals.test(globalOrd)) {
                        bucketOrds.add(owningBucketOrd, globalOrd);
                    }
                }
            }
        }

        /**
         * Excludes deleted docs in the results by cross-checking with liveDocs.
         */
        private void forEachExcludeDeletedDocs(long owningBucketOrd) throws IOException {
            assert bucketCountThresholds.getMinDocCount() == 0;
            try (LongHash accepted = new LongHash(20, bigArrays())) {
                for (LeafReaderContext ctx : searcher().getTopReaderContext().leaves()) {
                    LeafReader reader = ctx.reader();
                    Bits liveDocs = reader.getLiveDocs();
                    SortedSetDocValues globalOrds = null;
                    for (int docId = 0; docId < reader.maxDoc(); ++docId) {
                        if (liveDocs == null || liveDocs.get(docId)) {  // document is not deleted
                            globalOrds = globalOrds == null ? valuesSource.globalOrdinalsValues(ctx) : globalOrds;
                            if (globalOrds.advanceExact(docId)) {
                                for (int i = 0; i < globalOrds.docValueCount(); i++) {
                                    long globalOrd = globalOrds.nextOrd();
                                    if (accepted.find(globalOrd) >= 0) {
                                        continue;
                                    }
                                    if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                                        continue;
                                    }
                                    bucketOrds.add(owningBucketOrd, globalOrd);
                                    accepted.add(globalOrd);
                                }
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void close() {
            bucketOrds.close();
        }

        @Override
        InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
            try (
                LongArray otherDocCount = bigArrays().newLongArray(owningBucketOrds.size(), true);
                ObjectArray<B[]> topBucketsPreOrd = collectionStrategy.buildTopBucketsPerOrd(owningBucketOrds.size())
            ) {
                try (IntArray bucketsToCollect = bigArrays().newIntArray(owningBucketOrds.size())) {
                    long ordsToCollect = 0;
                    for (long ordIdx = 0; ordIdx < owningBucketOrds.size(); ordIdx++) {
                        final long owningBucketOrd = owningBucketOrds.get(ordIdx);
                        collectZeroDocEntriesIfNeeded(owningBucketOrd);
                        final int size = (int) Math.min(bucketOrds.bucketsInOrd(owningBucketOrd), bucketCountThresholds.getShardSize());
                        ordsToCollect += size;
                        bucketsToCollect.set(ordIdx, size);
                    }
                    try (LongArray ordsArray = bigArrays().newLongArray(ordsToCollect)) {
                        long ordsCollected = 0;
                        GlobalOrdLookupFunction lookupGlobalOrd = valuesSupplier.get()::lookupOrd;
                        for (long ordIdx = 0; ordIdx < topBucketsPreOrd.size(); ordIdx++) {
                            long owningBucketOrd = owningBucketOrds.get(ordIdx);
                            try (
                                ObjectArrayPriorityQueue<BucketAndOrd<TB>> ordered = collectionStrategy.buildPriorityQueue(
                                    bucketsToCollect.get(ordIdx)
                                )
                            ) {
                                BucketUpdater<TB> updater = collectionStrategy.bucketUpdater(owningBucketOrd, lookupGlobalOrd);
                                LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrd);
                                BucketAndOrd<TB> spare = null;
                                while (ordsEnum.next()) {
                                    long docCount = bucketDocCount(ordsEnum.ord());
                                    otherDocCount.increment(ordIdx, docCount);
                                    if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                                        continue;
                                    }
                                    if (spare == null) {
                                        checkRealMemoryCBForInternalBucket();
                                        spare = new BucketAndOrd<>(collectionStrategy.buildEmptyTemporaryBucket());
                                    }
                                    updater.updateBucket(spare.bucket, ordsEnum.value(), docCount);
                                    spare.ord = ordsEnum.ord();
                                    spare = ordered.insertWithOverflow(spare);
                                }
                                // Get the top buckets
                                int orderedSize = (int) ordered.size();
                                B[] buckets = collectionStrategy.buildBuckets(orderedSize);
                                for (int i = orderedSize - 1; i >= 0; --i) {
                                    checkRealMemoryCBForInternalBucket();
                                    BucketAndOrd<TB> bucketAndOrd = ordered.pop();
                                    B bucket = collectionStrategy.convertTempBucketToRealBucket(bucketAndOrd.bucket, lookupGlobalOrd);
                                    ordsArray.set(ordsCollected + i, bucketAndOrd.ord);
                                    buckets[i] = bucket;
                                    otherDocCount.increment(ordIdx, -bucket.getDocCount());
                                }
                                topBucketsPreOrd.set(ordIdx, buckets);
                                ordsCollected += orderedSize;
                            }
                        }
                        assert ordsCollected == ordsArray.size();
                        collectionStrategy.buildSubAggs(topBucketsPreOrd, ordsArray);
                    }
                }
                return GlobalOrdinalsStringTermsAggregator.this.buildAggregations(
                    Math.toIntExact(owningBucketOrds.size()),
                    ordIdx -> collectionStrategy.buildResult(
                        owningBucketOrds.get(ordIdx),
                        otherDocCount.get(ordIdx),
                        topBucketsPreOrd.get(ordIdx)
                    )
                );
            }
        }

    }

    /**
     * Strategy for building results.
     */
    abstract class ResultStrategy<
        R extends InternalAggregation,
        B extends InternalMultiBucketAggregation.InternalBucket,
        TB extends InternalMultiBucketAggregation.InternalBucket> implements Releasable {

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
        abstract BucketUpdater<TB> bucketUpdater(long owningBucketOrd, GlobalOrdLookupFunction lookupGlobalOrd);

        /**
         * Build a {@link PriorityQueue} to sort the buckets. After we've
         * collected all of the buckets we'll collect all entries in the queue.
         */
        abstract ObjectArrayPriorityQueue<BucketAndOrd<TB>> buildPriorityQueue(int size);

        /**
         * Build an array to hold the "top" buckets for each ordinal.
         */
        abstract ObjectArray<B[]> buildTopBucketsPerOrd(long size);

        /**
         * Build an array of buckets for a particular ordinal to collect the
         * results. The populated list is passed to {@link #buildResult}.
         */
        abstract B[] buildBuckets(int size);

        /**
         * Convert a temporary bucket into a real bucket.
         */
        abstract B convertTempBucketToRealBucket(TB temp, GlobalOrdLookupFunction lookupGlobalOrd) throws IOException;

        /**
         * Build the sub-aggregations into the buckets. This will usually
         * delegate to {@link #buildSubAggsForAllBuckets}.
         */
        abstract void buildSubAggs(ObjectArray<B[]> topBucketsPreOrd, LongArray ordsArray) throws IOException;

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
        void updateBucket(TB spare, long globalOrd, long docCount) throws IOException;
    }

    /**
     * Builds results for the standard {@code terms} aggregation.
     */
    class StandardTermsResults extends ResultStrategy<StringTerms, StringTerms.Bucket, OrdBucket> {
        @Override
        String describe() {
            return "terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return primary;
        }

        @Override
        ObjectArray<StringTerms.Bucket[]> buildTopBucketsPerOrd(long size) {
            return bigArrays().newObjectArray(size);
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
        BucketUpdater<OrdBucket> bucketUpdater(long owningBucketOrd, GlobalOrdLookupFunction lookupGlobalOrd) {
            return (spare, globalOrd, docCount) -> {
                spare.globalOrd = globalOrd;
                spare.docCount = docCount;
            };
        }

        @Override
        ObjectArrayPriorityQueue<BucketAndOrd<OrdBucket>> buildPriorityQueue(int size) {
            return new BucketPriorityQueue<>(
                size,
                bigArrays(),
                order.partiallyBuiltBucketComparator(GlobalOrdinalsStringTermsAggregator.this)
            );
        }

        @Override
        StringTerms.Bucket convertTempBucketToRealBucket(OrdBucket temp, GlobalOrdLookupFunction lookupGlobalOrd) throws IOException {
            BytesRef term = BytesRef.deepCopyOf(lookupGlobalOrd.apply(temp.globalOrd));
            return new StringTerms.Bucket(term, temp.docCount, null, showTermDocCountError, 0, format);
        }

        @Override
        void buildSubAggs(ObjectArray<StringTerms.Bucket[]> topBucketsPreOrd, LongArray ordsArray) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPreOrd, ordsArray, (b, aggs) -> b.aggregations = aggs);
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

    /**
     * Builds results for the {@code significant_terms} aggregation.
     */
    class SignificantTermsResults extends ResultStrategy<
        SignificantStringTerms,
        SignificantStringTerms.Bucket,
        SignificantStringTerms.Bucket> {

        private final BackgroundFrequencyForBytes backgroundFrequencies;
        private final long supersetSize;
        private final SignificanceHeuristic significanceHeuristic;

        private LongArray subsetSizes;

        SignificantTermsResults(
            SignificanceLookup significanceLookup,
            SignificanceHeuristic significanceHeuristic,
            CardinalityUpperBound cardinality
        ) {
            backgroundFrequencies = significanceLookup.bytesLookup(bigArrays(), cardinality);
            supersetSize = significanceLookup.supersetSize();
            this.significanceHeuristic = significanceHeuristic;
            boolean success = false;
            try {
                subsetSizes = bigArrays().newLongArray(1, true);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        @Override
        String describe() {
            return "significant_terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return new LeafBucketCollectorBase(primary, null) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    super.collect(doc, owningBucketOrd);
                    subsetSizes = bigArrays().grow(subsetSizes, owningBucketOrd + 1);
                    subsetSizes.increment(owningBucketOrd, 1);
                }
            };
        }

        @Override
        ObjectArray<SignificantStringTerms.Bucket[]> buildTopBucketsPerOrd(long size) {
            return bigArrays().newObjectArray(size);
        }

        @Override
        SignificantStringTerms.Bucket[] buildBuckets(int size) {
            return new SignificantStringTerms.Bucket[size];
        }

        @Override
        SignificantStringTerms.Bucket buildEmptyTemporaryBucket() {
            return new SignificantStringTerms.Bucket(new BytesRef(), 0, 0, null, format, 0);
        }

        private long subsetSize(long owningBucketOrd) {
            // if the owningBucketOrd is not in the array that means the bucket is empty so the size has to be 0
            return owningBucketOrd < subsetSizes.size() ? subsetSizes.get(owningBucketOrd) : 0;
        }

        @Override
        BucketUpdater<SignificantStringTerms.Bucket> bucketUpdater(long owningBucketOrd, GlobalOrdLookupFunction lookupGlobalOrd) {
            long subsetSize = subsetSize(owningBucketOrd);
            return (spare, globalOrd, docCount) -> {
                oversizedCopy(lookupGlobalOrd.apply(globalOrd), spare.termBytes);
                spare.subsetDf = docCount;
                spare.supersetDf = backgroundFrequencies.freq(spare.termBytes);
                /*
                 * During shard-local down-selection we use subset/superset stats
                 * that are for this shard only. Back at the central reducer these
                 * properties will be updated with global stats.
                 */
                spare.updateScore(significanceHeuristic, subsetSize, supersetSize);
            };
        }

        @Override
        ObjectArrayPriorityQueue<BucketAndOrd<SignificantStringTerms.Bucket>> buildPriorityQueue(int size) {
            return new BucketSignificancePriorityQueue<>(size, bigArrays());
        }

        @Override
        SignificantStringTerms.Bucket convertTempBucketToRealBucket(
            SignificantStringTerms.Bucket temp,
            GlobalOrdLookupFunction lookupGlobalOrd
        ) throws IOException {
            return temp;
        }

        @Override
        void buildSubAggs(ObjectArray<SignificantStringTerms.Bucket[]> topBucketsPreOrd, LongArray ordsArray) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPreOrd, ordsArray, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        SignificantStringTerms buildResult(long owningBucketOrd, long otherDocCount, SignificantStringTerms.Bucket[] topBuckets) {
            return new SignificantStringTerms(
                name,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                subsetSize(owningBucketOrd),
                supersetSize,
                significanceHeuristic,
                Arrays.asList(topBuckets)
            );
        }

        @Override
        SignificantStringTerms buildEmptyResult() {
            return buildEmptySignificantTermsAggregation(0, supersetSize, significanceHeuristic);
        }

        @Override
        SignificantStringTerms buildNoValuesResult(long owningBucketOrdinal) {
            return buildEmptySignificantTermsAggregation(subsetSizes.get(owningBucketOrdinal), supersetSize, significanceHeuristic);
        }

        @Override
        public void close() {
            Releasables.close(backgroundFrequencies, subsetSizes);
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
        private static void oversizedCopy(BytesRef from, BytesRef to) {
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
    static final LongPredicate ALWAYS_TRUE = l -> true;
}
