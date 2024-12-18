/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.util.ObjectArrayPriorityQueue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
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
import org.elasticsearch.search.aggregations.bucket.terms.SignificanceLookup.BackgroundFrequencyForBytes;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongConsumer;

import static org.elasticsearch.search.aggregations.InternalOrder.isKeyOrder;

/**
 * An aggregator of string values that hashes the strings on the fly rather
 * than up front like the {@link GlobalOrdinalsStringTermsAggregator}.
 */
public final class MapStringTermsAggregator extends AbstractStringTermsAggregator {
    private final CollectorSource collectorSource;
    private final ResultStrategy<?, ?> resultStrategy;
    private final BytesKeyedBucketOrds bucketOrds;
    private final IncludeExclude.StringFilter includeExclude;
    private final boolean excludeDeletedDocs;

    public MapStringTermsAggregator(
        String name,
        AggregatorFactories factories,
        CollectorSource collectorSource,
        Function<MapStringTermsAggregator, ResultStrategy<?, ?>> resultStrategy,
        BucketOrder order,
        DocValueFormat format,
        BucketCountThresholds bucketCountThresholds,
        IncludeExclude.StringFilter includeExclude,
        AggregationContext context,
        Aggregator parent,
        SubAggCollectionMode collectionMode,
        boolean showTermDocCountError,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata,
        boolean excludeDeletedDocs
    ) throws IOException {
        super(name, factories, context, parent, order, format, bucketCountThresholds, collectionMode, showTermDocCountError, metadata);
        this.resultStrategy = resultStrategy.apply(this); // ResultStrategy needs a reference to the Aggregator to do its job.
        this.includeExclude = includeExclude;
        bucketOrds = BytesKeyedBucketOrds.build(context.bigArrays(), cardinality);
        // set last because if there is an error during construction the collector gets release outside the constructor.
        this.collectorSource = collectorSource;
        this.excludeDeletedDocs = excludeDeletedDocs;
    }

    @Override
    public ScoreMode scoreMode() {
        if (collectorSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        return resultStrategy.wrapCollector(
            collectorSource.getLeafCollector(
                includeExclude,
                aggCtx.getLeafReaderContext(),
                sub,
                this::addRequestCircuitBreakerBytes,
                (s, doc, owningBucketOrd, bytes) -> {
                    long bucketOrdinal = bucketOrds.add(owningBucketOrd, bytes);
                    if (bucketOrdinal < 0) { // already seen
                        bucketOrdinal = -1 - bucketOrdinal;
                        collectExistingBucket(s, doc, bucketOrdinal);
                    } else {
                        collectBucket(s, doc, bucketOrdinal);
                    }
                }
            )
        );
    }

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        return resultStrategy.buildAggregations(owningBucketOrds);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return resultStrategy.buildEmptyResult();
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("total_buckets", bucketOrds.size());
        add.accept("collection_strategy", collectorSource.describe());
        collectorSource.collectDebugInfo(add);
        add.accept("result_strategy", resultStrategy.describe());
    }

    @Override
    public void doClose() {
        Releasables.close(collectorSource, resultStrategy, bucketOrds);
    }

    /**
     * Abstraction on top of building collectors to fetch values so {@code terms},
     * {@code significant_terms}, and {@code significant_text} can share a bunch of
     * aggregation code.
     */
    public interface CollectorSource extends Releasable {
        /**
         * A description of the strategy to include in profile results.
         */
        String describe();

        /**
         * Collect debug information to add to the profiling results. This will
         * only be called if the aggregation is being profiled.
         */
        void collectDebugInfo(BiConsumer<String, Object> add);

        /**
         * Does this {@link CollectorSource} need queries to calculate the score?
         */
        boolean needsScores();

        /**
         * Build the collector.
         */
        LeafBucketCollector getLeafCollector(
            IncludeExclude.StringFilter includeExclude,
            LeafReaderContext ctx,
            LeafBucketCollector sub,
            LongConsumer addRequestCircuitBreakerBytes,
            CollectConsumer consumer
        ) throws IOException;
    }

    @FunctionalInterface
    public interface CollectConsumer {
        void accept(LeafBucketCollector sub, int doc, long owningBucketOrd, BytesRef bytes) throws IOException;
    }

    /**
     * Fetch values from a {@link ValuesSource}.
     */
    public static class ValuesSourceCollectorSource implements CollectorSource {
        private final ValuesSourceConfig valuesSourceConfig;

        public ValuesSourceCollectorSource(ValuesSourceConfig valuesSourceConfig) {
            this.valuesSourceConfig = valuesSourceConfig;
        }

        @Override
        public String describe() {
            return "from " + valuesSourceConfig.getDescription();
        }

        @Override
        public void collectDebugInfo(BiConsumer<String, Object> add) {}

        @Override
        public boolean needsScores() {
            return valuesSourceConfig.getValuesSource().needsScores();
        }

        @Override
        public LeafBucketCollector getLeafCollector(
            IncludeExclude.StringFilter includeExclude,
            LeafReaderContext ctx,
            LeafBucketCollector sub,
            LongConsumer addRequestCircuitBreakerBytes,
            CollectConsumer consumer
        ) throws IOException {
            final SortedBinaryDocValues values = valuesSourceConfig.getValuesSource().bytesValues(ctx);
            final BinaryDocValues singleton = FieldData.unwrapSingleton(values);
            return singleton != null
                ? getLeafCollector(includeExclude, singleton, sub, consumer)
                : getLeafCollector(includeExclude, values, sub, consumer);
        }

        private LeafBucketCollector getLeafCollector(
            IncludeExclude.StringFilter includeExclude,
            SortedBinaryDocValues values,
            LeafBucketCollector sub,
            CollectConsumer consumer
        ) {
            return new LeafBucketCollectorBase(sub, values) {
                final BytesRefBuilder previous = new BytesRefBuilder();

                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (false == values.advanceExact(doc)) {
                        return;
                    }
                    int valuesCount = values.docValueCount();

                    // SortedBinaryDocValues don't guarantee uniqueness so we
                    // need to take care of dups
                    previous.clear();
                    for (int i = 0; i < valuesCount; ++i) {
                        BytesRef bytes = values.nextValue();
                        if (includeExclude != null && false == includeExclude.accept(bytes)) {
                            continue;
                        }
                        if (i > 0 && previous.get().equals(bytes)) {
                            continue;
                        }
                        previous.copyBytes(bytes);
                        consumer.accept(sub, doc, owningBucketOrd, bytes);
                    }
                }
            };
        }

        private LeafBucketCollector getLeafCollector(
            IncludeExclude.StringFilter includeExclude,
            BinaryDocValues values,
            LeafBucketCollector sub,
            CollectConsumer consumer
        ) {
            return new LeafBucketCollectorBase(sub, values) {

                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (values.advanceExact(doc)) {
                        BytesRef bytes = values.binaryValue();
                        if (includeExclude == null || includeExclude.accept(bytes)) {
                            consumer.accept(sub, doc, owningBucketOrd, bytes);
                        }
                    }
                }
            };
        }

        @Override
        public void close() {}
    }

    /**
     * Strategy for building results.
     */
    abstract class ResultStrategy<R extends InternalAggregation, B extends InternalMultiBucketAggregation.InternalBucket>
        implements
            Releasable {

        private InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
            try (
                LongArray otherDocCounts = bigArrays().newLongArray(owningBucketOrds.size(), true);
                ObjectArray<B[]> topBucketsPerOrd = buildTopBucketsPerOrd(Math.toIntExact(owningBucketOrds.size()))
            ) {
                try (IntArray bucketsToCollect = bigArrays().newIntArray(owningBucketOrds.size())) {
                    long ordsToCollect = 0;
                    for (long ordIdx = 0; ordIdx < owningBucketOrds.size(); ordIdx++) {
                        final long owningBucketOrd = owningBucketOrds.get(ordIdx);
                        collectZeroDocEntriesIfNeeded(owningBucketOrd, excludeDeletedDocs);
                        final int size = (int) Math.min(bucketOrds.bucketsInOrd(owningBucketOrd), bucketCountThresholds.getShardSize());
                        ordsToCollect += size;
                        bucketsToCollect.set(ordIdx, size);
                    }
                    try (LongArray ordsArray = bigArrays().newLongArray(ordsToCollect)) {
                        long ordsCollected = 0;
                        for (long ordIdx = 0; ordIdx < topBucketsPerOrd.size(); ordIdx++) {
                            long owningOrd = owningBucketOrds.get(ordIdx);
                            try (ObjectArrayPriorityQueue<BucketAndOrd<B>> ordered = buildPriorityQueue(bucketsToCollect.get(ordIdx))) {
                                BucketAndOrd<B> spare = null;
                                BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningOrd);
                                BucketUpdater<B> bucketUpdater = bucketUpdater(owningOrd);
                                while (ordsEnum.next()) {
                                    long docCount = bucketDocCount(ordsEnum.ord());
                                    otherDocCounts.increment(ordIdx, docCount);
                                    if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                                        continue;
                                    }
                                    if (spare == null) {
                                        checkRealMemoryCBForInternalBucket();
                                        spare = new BucketAndOrd<>(buildEmptyBucket());
                                    }
                                    bucketUpdater.updateBucket(spare.bucket, ordsEnum, docCount);
                                    spare.ord = ordsEnum.ord();
                                    spare = ordered.insertWithOverflow(spare);
                                }

                                final int orderedSize = (int) ordered.size();
                                final B[] buckets = buildBuckets(orderedSize);
                                for (int i = orderedSize - 1; i >= 0; --i) {
                                    BucketAndOrd<B> bucketAndOrd = ordered.pop();
                                    finalizeBucket(bucketAndOrd.bucket);
                                    buckets[i] = bucketAndOrd.bucket;
                                    ordsArray.set(ordsCollected + i, bucketAndOrd.ord);
                                    otherDocCounts.increment(ordIdx, -bucketAndOrd.bucket.getDocCount());
                                }
                                topBucketsPerOrd.set(ordIdx, buckets);
                                ordsCollected += orderedSize;
                            }
                        }
                        assert ordsCollected == ordsArray.size();
                        buildSubAggs(topBucketsPerOrd, ordsArray);
                    }
                }
                return MapStringTermsAggregator.this.buildAggregations(
                    Math.toIntExact(owningBucketOrds.size()),
                    ordIdx -> buildResult(owningBucketOrds.get(ordIdx), otherDocCounts.get(ordIdx), topBucketsPerOrd.get(ordIdx))
                );
            }
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
         * Collect extra entries for "zero" hit documents if they were requested
         * and required.
         */
        abstract void collectZeroDocEntriesIfNeeded(long owningBucketOrd, boolean excludeDeletedDocs) throws IOException;

        /**
         * Build an empty bucket.
         */
        abstract B buildEmptyBucket();

        /**
         * Build a {@link PriorityQueue} to sort the buckets. After we've
         * collected all of the buckets we'll collect all entries in the queue.
         */
        abstract ObjectArrayPriorityQueue<BucketAndOrd<B>> buildPriorityQueue(int size);

        /**
         * Update fields in {@code spare} to reflect information collected for
         * this bucket ordinal.
         */
        abstract BucketUpdater<B> bucketUpdater(long owningBucketOrd);

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
         * Finalize building a bucket. Called once we know that the bucket will
         * be included in the results.
         */
        abstract void finalizeBucket(B bucket);

        /**
         * Build the sub-aggregations into the buckets. This will usually
         * delegate to {@link #buildSubAggsForAllBuckets(ObjectArray, LongArray, BiConsumer)}.
         */
        abstract void buildSubAggs(ObjectArray<B[]> topBucketsPerOrd, LongArray ordsArray) throws IOException;

        /**
         * Turn the buckets into an aggregation result.
         */
        abstract R buildResult(long owningBucketOrd, long otherDocCount, B[] topBuckets);

        /**
         * Build an "empty" result. Only called if there isn't any data on this
         * shard.
         */
        abstract R buildEmptyResult();
    }

    interface BucketUpdater<B extends InternalMultiBucketAggregation.InternalBucket> {
        void updateBucket(B spare, BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum, long docCount) throws IOException;
    }

    /**
     * Builds results for the standard {@code terms} aggregation.
     */
    class StandardTermsResults extends ResultStrategy<StringTerms, StringTerms.Bucket> {
        private final ValuesSource valuesSource;
        private final Comparator<BucketAndOrd<StringTerms.Bucket>> comparator;

        StandardTermsResults(ValuesSource valuesSource, Aggregator aggregator) {
            this.valuesSource = valuesSource;
            this.comparator = order.partiallyBuiltBucketComparator(aggregator);
        }

        @Override
        String describe() {
            return "terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return primary;
        }

        @Override
        void collectZeroDocEntriesIfNeeded(long owningBucketOrd, boolean excludeDeletedDocs) throws IOException {
            if (bucketCountThresholds.getMinDocCount() != 0) {
                return;
            }
            if (InternalOrder.isCountDesc(order) && bucketOrds.bucketsInOrd(owningBucketOrd) >= bucketCountThresholds.getRequiredSize()) {
                return;
            }
            // we need to fill-in the blanks
            for (LeafReaderContext ctx : searcher().getTopReaderContext().leaves()) {
                final Bits liveDocs = excludeDeletedDocs ? ctx.reader().getLiveDocs() : null;
                if (liveDocs == null && valuesSource.hasOrdinals()) {
                    final SortedSetDocValues values = ((ValuesSource.Bytes.WithOrdinals) valuesSource).ordinalsValues(ctx);
                    collectZeroDocEntries(values, owningBucketOrd);
                } else {
                    final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
                    final BinaryDocValues singleton = FieldData.unwrapSingleton(values);
                    if (singleton != null) {
                        collectZeroDocEntries(singleton, liveDocs, ctx.reader().maxDoc(), owningBucketOrd);
                    } else {
                        collectZeroDocEntries(values, liveDocs, ctx.reader().maxDoc(), owningBucketOrd);
                    }
                }
            }
        }

        private void collectZeroDocEntries(SortedSetDocValues values, long owningBucketOrd) throws IOException {
            final TermsEnum termsEnum = values.termsEnum();
            BytesRef term;
            while ((term = termsEnum.next()) != null) {
                if (includeExclude == null || includeExclude.accept(term)) {
                    bucketOrds.add(owningBucketOrd, term);
                }
            }
        }

        private void collectZeroDocEntries(SortedBinaryDocValues values, Bits liveDocs, int maxDoc, long owningBucketOrd)
            throws IOException {
            // brute force
            for (int docId = 0; docId < maxDoc; ++docId) {
                if (liveDocs != null && liveDocs.get(docId) == false) {
                    continue;
                }
                if (values.advanceExact(docId)) {
                    final int valueCount = values.docValueCount();
                    for (int i = 0; i < valueCount; ++i) {
                        final BytesRef term = values.nextValue();
                        if (includeExclude == null || includeExclude.accept(term)) {
                            bucketOrds.add(owningBucketOrd, term);
                        }
                    }
                }
            }
        }

        private void collectZeroDocEntries(BinaryDocValues values, Bits liveDocs, int maxDoc, long owningBucketOrd) throws IOException {
            // brute force
            for (int docId = 0; docId < maxDoc; ++docId) {
                if (liveDocs != null && liveDocs.get(docId) == false) {
                    continue;
                }
                if (values.advanceExact(docId)) {
                    final BytesRef term = values.binaryValue();
                    if (includeExclude == null || includeExclude.accept(term)) {
                        bucketOrds.add(owningBucketOrd, term);
                    }
                }
            }
        }

        @Override
        StringTerms.Bucket buildEmptyBucket() {
            return new StringTerms.Bucket(new BytesRef(), 0, null, showTermDocCountError, 0, format);
        }

        @Override
        ObjectArrayPriorityQueue<BucketAndOrd<StringTerms.Bucket>> buildPriorityQueue(int size) {
            return new BucketPriorityQueue<>(size, bigArrays(), comparator);
        }

        @Override
        BucketUpdater<StringTerms.Bucket> bucketUpdater(long owningBucketOrd) {
            return (spare, ordsEnum, docCount) -> {
                ordsEnum.readValue(spare.termBytes);
                spare.docCount = docCount;
            };
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
        void finalizeBucket(StringTerms.Bucket bucket) {
            /*
             * termBytes contains a reference to the bytes held by the
             * bucketOrds which will be invalid once the aggregation is
             * closed so we have to copy it.
             */
            bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
        }

        @Override
        void buildSubAggs(ObjectArray<StringTerms.Bucket[]> topBucketsPerOrd, LongArray ordArray) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, ordArray, (b, a) -> b.aggregations = a);
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
        public void close() {}
    }

    /**
     * Builds results for the {@code significant_terms} aggregation.
     */
    class SignificantTermsResults extends ResultStrategy<SignificantStringTerms, SignificantStringTerms.Bucket> {
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
        void collectZeroDocEntriesIfNeeded(long owningBucketOrd, boolean excludeDeletedDocs) throws IOException {}

        @Override
        SignificantStringTerms.Bucket buildEmptyBucket() {
            return new SignificantStringTerms.Bucket(new BytesRef(), 0, 0, null, format, 0);
        }

        @Override
        ObjectArrayPriorityQueue<BucketAndOrd<SignificantStringTerms.Bucket>> buildPriorityQueue(int size) {
            return new BucketSignificancePriorityQueue<>(size, bigArrays());
        }

        @Override
        BucketUpdater<SignificantStringTerms.Bucket> bucketUpdater(long owningBucketOrd) {
            long subsetSize = subsetSizes.get(owningBucketOrd);
            return (spare, ordsEnum, docCount) -> {
                ordsEnum.readValue(spare.termBytes);
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
        ObjectArray<SignificantStringTerms.Bucket[]> buildTopBucketsPerOrd(long size) {
            return bigArrays().newObjectArray(size);
        }

        @Override
        SignificantStringTerms.Bucket[] buildBuckets(int size) {
            return new SignificantStringTerms.Bucket[size];
        }

        @Override
        void finalizeBucket(SignificantStringTerms.Bucket bucket) {
            /*
             * termBytes contains a reference to the bytes held by the
             * bucketOrds which will be invalid once the aggregation is
             * closed so we have to copy it.
             */
            bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
        }

        @Override
        void buildSubAggs(ObjectArray<SignificantStringTerms.Bucket[]> topBucketsPerOrd, LongArray ordsArray) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, ordsArray, (b, a) -> b.aggregations = a);
        }

        @Override
        SignificantStringTerms buildResult(long owningBucketOrd, long otherDocCount, SignificantStringTerms.Bucket[] topBuckets) {
            return new SignificantStringTerms(
                name,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(),
                format,
                subsetSizes.get(owningBucketOrd),
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
        public void close() {
            Releasables.close(backgroundFrequencies, subsetSizes);
        }
    }
}
