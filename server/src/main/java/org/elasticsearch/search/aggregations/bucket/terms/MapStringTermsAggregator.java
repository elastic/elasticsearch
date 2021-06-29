/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
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
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.search.aggregations.InternalOrder.isKeyOrder;

/**
 * An aggregator of string values that hashes the strings on the fly rather
 * than up front like the {@link GlobalOrdinalsStringTermsAggregator}.
 */
public class MapStringTermsAggregator extends AbstractStringTermsAggregator {
    private final CollectorSource collectorSource;
    private final ResultStrategy<?, ?> resultStrategy;
    private final BytesKeyedBucketOrds bucketOrds;
    private final IncludeExclude.StringFilter includeExclude;

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
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, order, format, bucketCountThresholds, collectionMode, showTermDocCountError, metadata);
        this.collectorSource = collectorSource;
        this.resultStrategy = resultStrategy.apply(this); // ResultStrategy needs a reference to the Aggregator to do its job.
        this.includeExclude = includeExclude;
        bucketOrds = BytesKeyedBucketOrds.build(context.bigArrays(), cardinality);
    }

    @Override
    public ScoreMode scoreMode() {
        if (collectorSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        return resultStrategy.wrapCollector(
            collectorSource.getLeafCollector(
                includeExclude,
                ctx,
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
            SortedBinaryDocValues values = valuesSourceConfig.getValuesSource().bytesValues(ctx);
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

        @Override
        public void close() {}
    }

    /**
     * Strategy for building results.
     */
    abstract class ResultStrategy<R extends InternalAggregation, B extends InternalMultiBucketAggregation.InternalBucket>
        implements
            Releasable {

        private InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            B[][] topBucketsPerOrd = buildTopBucketsPerOrd(owningBucketOrds.length);
            long[] otherDocCounts = new long[owningBucketOrds.length];
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                collectZeroDocEntriesIfNeeded(owningBucketOrds[ordIdx]);
                int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

                PriorityQueue<B> ordered = buildPriorityQueue(size);
                B spare = null;
                BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
                Supplier<B> emptyBucketBuilder = emptyBucketBuilder(owningBucketOrds[ordIdx]);
                while (ordsEnum.next()) {
                    long docCount = bucketDocCount(ordsEnum.ord());
                    otherDocCounts[ordIdx] += docCount;
                    if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                        continue;
                    }
                    if (spare == null) {
                        spare = emptyBucketBuilder.get();
                    }
                    updateBucket(spare, ordsEnum, docCount);
                    spare = ordered.insertWithOverflow(spare);
                }

                topBucketsPerOrd[ordIdx] = buildBuckets(ordered.size());
                for (int i = ordered.size() - 1; i >= 0; --i) {
                    topBucketsPerOrd[ordIdx][i] = ordered.pop();
                    otherDocCounts[ordIdx] -= topBucketsPerOrd[ordIdx][i].getDocCount();
                    finalizeBucket(topBucketsPerOrd[ordIdx][i]);
                }
            }

            buildSubAggs(topBucketsPerOrd);
            InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                result[ordIdx] = buildResult(owningBucketOrds[ordIdx], otherDocCounts[ordIdx], topBucketsPerOrd[ordIdx]);
            }
            return result;
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
        abstract void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException;

        /**
         * Build an empty temporary bucket.
         */
        abstract Supplier<B> emptyBucketBuilder(long owningBucketOrd);

        /**
         * Build a {@link PriorityQueue} to sort the buckets. After we've
         * collected all of the buckets we'll collect all entries in the queue.
         */
        abstract PriorityQueue<B> buildPriorityQueue(int size);

        /**
         * Update fields in {@code spare} to reflect information collected for
         * this bucket ordinal.
         */
        abstract void updateBucket(B spare, BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum, long docCount) throws IOException;

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
         * Finalize building a bucket. Called once we know that the bucket will
         * be included in the results.
         */
        abstract void finalizeBucket(B bucket);

        /**
         * Build the sub-aggregations into the buckets. This will usually
         * delegate to {@link #buildSubAggsForAllBuckets}.
         */
        abstract void buildSubAggs(B[][] topBucketsPerOrd) throws IOException;

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

    /**
     * Builds results for the standard {@code terms} aggregation.
     */
    class StandardTermsResults extends ResultStrategy<StringTerms, StringTerms.Bucket> {
        private final ValuesSource valuesSource;

        StandardTermsResults(ValuesSource valuesSource) {
            this.valuesSource = valuesSource;
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
        void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException {
            if (bucketCountThresholds.getMinDocCount() != 0) {
                return;
            }
            if (InternalOrder.isCountDesc(order) && bucketOrds.bucketsInOrd(owningBucketOrd) >= bucketCountThresholds.getRequiredSize()) {
                return;
            }
            // we need to fill-in the blanks
            for (LeafReaderContext ctx : searcher().getTopReaderContext().leaves()) {
                SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
                // brute force
                for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                    if (values.advanceExact(docId)) {
                        int valueCount = values.docValueCount();
                        for (int i = 0; i < valueCount; ++i) {
                            BytesRef term = values.nextValue();
                            if (includeExclude == null || includeExclude.accept(term)) {
                                bucketOrds.add(owningBucketOrd, term);
                            }
                        }
                    }
                }
            }
        }

        @Override
        Supplier<StringTerms.Bucket> emptyBucketBuilder(long owningBucketOrd) {
            return () -> new StringTerms.Bucket(new BytesRef(), 0, null, showTermDocCountError, 0, format);
        }

        @Override
        PriorityQueue<StringTerms.Bucket> buildPriorityQueue(int size) {
            return new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
        }

        @Override
        void updateBucket(StringTerms.Bucket spare, BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum, long docCount) throws IOException {
            ordsEnum.readValue(spare.termBytes);
            spare.docCount = docCount;
            spare.bucketOrd = ordsEnum.ord();
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
        void finalizeBucket(StringTerms.Bucket bucket) {
            /*
             * termBytes contains a reference to the bytes held by the
             * bucketOrds which will be invalid once the aggregation is
             * closed so we have to copy it.
             */
            bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
        }

        @Override
        void buildSubAggs(StringTerms.Bucket[][] topBucketsPerOrd) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, a) -> b.aggregations = a);
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
            return new StringTerms(name, reduceOrder, order, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(), metadata(), format, bucketCountThresholds.getShardSize(), showTermDocCountError,
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
    class SignificantTermsResults extends ResultStrategy<SignificantStringTerms, SignificantStringTerms.Bucket> {
        private final BackgroundFrequencyForBytes backgroundFrequencies;
        private final long supersetSize;
        private final SignificanceHeuristic significanceHeuristic;

        private LongArray subsetSizes = bigArrays().newLongArray(1, true);

        SignificantTermsResults(
            SignificanceLookup significanceLookup,
            SignificanceHeuristic significanceHeuristic,
            CardinalityUpperBound cardinality
        ) {
            backgroundFrequencies = significanceLookup.bytesLookup(bigArrays(), cardinality);
            supersetSize = significanceLookup.supersetSize();
            this.significanceHeuristic = significanceHeuristic;
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
        void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException {}

        @Override
        Supplier<SignificantStringTerms.Bucket> emptyBucketBuilder(long owningBucketOrd) {
            long subsetSize = subsetSizes.get(owningBucketOrd);
            return () -> new SignificantStringTerms.Bucket(new BytesRef(), 0, subsetSize, 0, 0, null, format, 0);
        }

        @Override
        PriorityQueue<SignificantStringTerms.Bucket> buildPriorityQueue(int size) {
            return new BucketSignificancePriorityQueue<>(size);
        }

        @Override
        void updateBucket(SignificantStringTerms.Bucket spare, BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum, long docCount)
            throws IOException {

            ordsEnum.readValue(spare.termBytes);
            spare.bucketOrd = ordsEnum.ord();
            spare.subsetDf = docCount;
            spare.supersetDf = backgroundFrequencies.freq(spare.termBytes);
            spare.supersetSize = supersetSize;
            /*
             * During shard-local down-selection we use subset/superset stats
             * that are for this shard only. Back at the central reducer these
             * properties will be updated with global stats.
             */
            spare.updateScore(significanceHeuristic);
        }

        @Override
        SignificantStringTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new SignificantStringTerms.Bucket[size][];
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
        void buildSubAggs(SignificantStringTerms.Bucket[][] topBucketsPerOrd) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, a) -> b.aggregations = a);
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
            return buildEmptySignificantTermsAggregation(0, significanceHeuristic);
        }

        @Override
        public void close() {
            Releasables.close(backgroundFrequencies, subsetSizes);
        }
    }
}

