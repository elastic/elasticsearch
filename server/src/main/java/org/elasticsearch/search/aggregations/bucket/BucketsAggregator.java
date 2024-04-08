/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.aggregations.AggregationErrors;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorBase;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ToLongFunction;

public abstract class BucketsAggregator extends AggregatorBase {
    private final CircuitBreaker breaker;
    private LongArray docCounts;
    protected final DocCountProvider docCountProvider;
    private int callCount;

    @SuppressWarnings("this-escape")
    public BucketsAggregator(
        String name,
        AggregatorFactories factories,
        AggregationContext aggCtx,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, aggCtx, parent, bucketCardinality, metadata);
        breaker = aggCtx.breaker();
        docCounts = bigArrays().newLongArray(1, true);
        docCountProvider = new DocCountProvider();
    }

    /**
     * Return an upper bound of the maximum bucket ordinal seen so far.
     */
    public final long maxBucketOrd() {
        return docCounts.size();
    }

    /**
     * Ensure there are at least <code>maxBucketOrd</code> buckets available.
     */
    public final void grow(long maxBucketOrd) {
        docCounts = bigArrays().grow(docCounts, maxBucketOrd);
    }

    /**
     * Utility method to collect the given doc in the given bucket (identified by the bucket ordinal)
     */
    public final void collectBucket(LeafBucketCollector subCollector, int doc, long bucketOrd) throws IOException {
        grow(bucketOrd + 1);
        int docCount = docCountProvider.getDocCount(doc);
        if (docCounts.increment(bucketOrd, docCount) == docCount) {
            // We call the circuit breaker the time to time in order to give it a chance to check available
            // memory in the parent breaker and break the execution if we are running out. To achieve that we
            // are passing 0 as the estimated bytes every 1024 calls
            if ((++callCount & 0x3FF) == 0) {
                breaker.addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
            }
        }
        subCollector.collect(doc, bucketOrd);
    }

    /**
     * Same as {@link #collectBucket(LeafBucketCollector, int, long)}, but doesn't check if the docCounts needs to be re-sized.
     */
    public final void collectExistingBucket(LeafBucketCollector subCollector, int doc, long bucketOrd) throws IOException {
        docCounts.increment(bucketOrd, docCountProvider.getDocCount(doc));
        subCollector.collect(doc, bucketOrd);
    }

    /**
     * Merge doc counts. If the {@linkplain Aggregator} is delayed then you must also call
     * {@link BestBucketsDeferringCollector#rewriteBuckets(LongUnaryOperator)} to merge the delayed buckets.
     * @param mergeMap a unary operator which maps a bucket's ordinal to the ordinal it should be merged with.
     *  If a bucket's ordinal is mapped to -1 then the bucket is removed entirely.
     */
    public final void rewriteBuckets(long newNumBuckets, LongUnaryOperator mergeMap) {
        LongArray oldDocCounts = docCounts;
        boolean success = false;
        try {
            docCounts = bigArrays().newLongArray(newNumBuckets, true);
            success = true;
            docCounts.fill(0, newNumBuckets, 0);
            for (long i = 0; i < oldDocCounts.size(); i++) {
                long docCount = oldDocCounts.get(i);

                if (docCount == 0) continue;

                // Skip any in the map which have been "removed", signified with -1
                long destinationOrdinal = mergeMap.applyAsLong(i);
                if (destinationOrdinal != -1) {
                    docCounts.increment(destinationOrdinal, docCount);
                }
            }
        } finally {
            if (success) {
                oldDocCounts.close();
            }
        }
    }

    public LongArray getDocCounts() {
        return docCounts;
    }

    /**
     * Utility method to increment the doc counts of the given bucket (identified by the bucket ordinal)
     */
    public final void incrementBucketDocCount(long bucketOrd, long inc) {
        docCounts = bigArrays().grow(docCounts, bucketOrd + 1);
        docCounts.increment(bucketOrd, inc);
    }

    /**
     * Utility method to return the number of documents that fell in the given bucket (identified by the bucket ordinal)
     */
    public final long bucketDocCount(long bucketOrd) {
        if (bucketOrd >= docCounts.size()) {
            // This may happen eg. if no document in the highest buckets is accepted by a sub aggregator.
            // For example, if there is a long terms agg on 3 terms 1,2,3 with a sub filter aggregator and if no document with 3 as a value
            // matches the filter, then the filter will never collect bucket ord 3. However, the long terms agg will call
            // bucketAggregations(3) on the filter aggregator anyway to build sub-aggregations.
            return 0;
        } else {
            return docCounts.get(bucketOrd);
        }
    }

    /**
     * Hook to allow taking an action before building the sub agg results.
     */
    protected void prepareSubAggs(long[] ordsToCollect) throws IOException {}

    /**
     * Build the results of the sub-aggregations of the buckets at each of
     * the provided ordinals.
     * <p>
     * Most aggregations should probably use something like
     * {@link #buildSubAggsForAllBuckets(Object[][], ToLongFunction, BiConsumer)}
     * or {@link #buildAggregationsForVariableBuckets(long[], LongKeyedBucketOrds, BucketBuilderForVariable, ResultBuilderForVariable)}
     * or {@link #buildAggregationsForFixedBucketCount(long[], int, BucketBuilderForFixedCount, Function)}
     * or {@link #buildAggregationsForSingleBucket(long[], SingleBucketResultBuilder)}
     * instead of calling this directly.
     * @return the sub-aggregation results in the same order as the provided
     *         array of ordinals
     */
    protected final IntFunction<InternalAggregations> buildSubAggsForBuckets(long[] bucketOrdsToCollect) throws IOException {
        prepareSubAggs(bucketOrdsToCollect);
        InternalAggregation[][] aggregations = new InternalAggregation[subAggregators.length][];
        for (int i = 0; i < subAggregators.length; i++) {
            aggregations[i] = subAggregators[i].buildAggregations(bucketOrdsToCollect);
        }
        return subAggsForBucketFunction(aggregations);
    }

    private static IntFunction<InternalAggregations> subAggsForBucketFunction(InternalAggregation[][] aggregations) {
        return ord -> InternalAggregations.from(new AbstractList<>() {
            @Override
            public InternalAggregation get(int index) {
                return aggregations[index][ord];
            }

            @Override
            public int size() {
                return aggregations.length;
            }
        });
    }

    /**
     * Build the sub aggregation results for a list of buckets and set them on
     * the buckets. This is usually used by aggregations that are selective
     * in which bucket they build. They use some mechanism of selecting a list
     * of buckets to build use this method to "finish" building the results.
     * @param buckets the buckets to finish building
     * @param bucketToOrd how to convert a bucket into an ordinal
     * @param setAggs how to set the sub-aggregation results on a bucket
     */
    protected final <B> void buildSubAggsForAllBuckets(
        B[][] buckets,
        ToLongFunction<B> bucketToOrd,
        BiConsumer<B, InternalAggregations> setAggs
    ) throws IOException {
        int totalBucketOrdsToCollect = 0;
        for (B[] bucketsForOneResult : buckets) {
            totalBucketOrdsToCollect += bucketsForOneResult.length;
        }
        long[] bucketOrdsToCollect = new long[totalBucketOrdsToCollect];
        int s = 0;
        for (B[] bucketsForOneResult : buckets) {
            for (B bucket : bucketsForOneResult) {
                bucketOrdsToCollect[s++] = bucketToOrd.applyAsLong(bucket);
            }
        }
        var results = buildSubAggsForBuckets(bucketOrdsToCollect);
        s = 0;
        for (B[] bucket : buckets) {
            for (int b = 0; b < bucket.length; b++) {
                setAggs.accept(bucket[b], results.apply(s++));
            }
        }
    }

    /**
     * Build aggregation results for an aggregator that has a fixed number of buckets per owning ordinal.
     * @param <B> the type of the bucket
     * @param owningBucketOrds owning bucket ordinals for which to build the results
     * @param bucketsPerOwningBucketOrd how many buckets there are per ord
     * @param bucketBuilder how to build a bucket
     * @param resultBuilder how to build a result from buckets
     */
    protected final <B> InternalAggregation[] buildAggregationsForFixedBucketCount(
        long[] owningBucketOrds,
        int bucketsPerOwningBucketOrd,
        BucketBuilderForFixedCount<B> bucketBuilder,
        Function<List<B>, InternalAggregation> resultBuilder
    ) throws IOException {
        int totalBuckets = owningBucketOrds.length * bucketsPerOwningBucketOrd;
        long[] bucketOrdsToCollect = new long[totalBuckets];
        int bucketOrdIdx = 0;
        for (long owningBucketOrd : owningBucketOrds) {
            long ord = owningBucketOrd * bucketsPerOwningBucketOrd;
            for (int offsetInOwningOrd = 0; offsetInOwningOrd < bucketsPerOwningBucketOrd; offsetInOwningOrd++) {
                bucketOrdsToCollect[bucketOrdIdx++] = ord++;
            }
        }
        bucketOrdIdx = 0;
        var subAggregationResults = buildSubAggsForBuckets(bucketOrdsToCollect);
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int owningOrdIdx = 0; owningOrdIdx < owningBucketOrds.length; owningOrdIdx++) {
            List<B> buckets = new ArrayList<>(bucketsPerOwningBucketOrd);
            for (int offsetInOwningOrd = 0; offsetInOwningOrd < bucketsPerOwningBucketOrd; offsetInOwningOrd++) {
                buckets.add(
                    bucketBuilder.build(
                        offsetInOwningOrd,
                        bucketDocCount(bucketOrdsToCollect[bucketOrdIdx]),
                        subAggregationResults.apply(bucketOrdIdx++)
                    )
                );
            }
            results[owningOrdIdx] = resultBuilder.apply(buckets);
        }
        return results;
    }

    @FunctionalInterface
    protected interface BucketBuilderForFixedCount<B> {
        B build(int offsetInOwningOrd, long docCount, InternalAggregations subAggregationResults);
    }

    /**
     * Build aggregation results for an aggregator that always contain a single bucket.
     * @param owningBucketOrds owning bucket ordinals for which to build the results
     * @param resultBuilder how to build a result from the sub aggregation results
     */
    protected final InternalAggregation[] buildAggregationsForSingleBucket(long[] owningBucketOrds, SingleBucketResultBuilder resultBuilder)
        throws IOException {
        /*
         * It'd be entirely reasonable to call
         * `consumeBucketsAndMaybeBreak(owningBucketOrds.length)`
         * here but we don't because single bucket aggs never have.
         */
        var subAggregationResults = buildSubAggsForBuckets(owningBucketOrds);
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            results[ordIdx] = resultBuilder.build(owningBucketOrds[ordIdx], subAggregationResults.apply(ordIdx));
        }
        return results;
    }

    @FunctionalInterface
    protected interface SingleBucketResultBuilder {
        InternalAggregation build(long owningBucketOrd, InternalAggregations subAggregationResults);
    }

    /**
     * Build aggregation results for an aggregator with a varying number of
     * {@code long} keyed buckets.
     * @param owningBucketOrds owning bucket ordinals for which to build the results
     * @param bucketOrds hash of values to the bucket ordinal
     */
    protected final <B> InternalAggregation[] buildAggregationsForVariableBuckets(
        long[] owningBucketOrds,
        LongKeyedBucketOrds bucketOrds,
        BucketBuilderForVariable<B> bucketBuilder,
        ResultBuilderForVariable<B> resultBuilder
    ) throws IOException {
        long totalOrdsToCollect = 0;
        final int[] bucketsInOrd = new int[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            final long bucketCount = bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);
            bucketsInOrd[ordIdx] = (int) bucketCount;
            totalOrdsToCollect += bucketCount;
        }
        if (totalOrdsToCollect > Integer.MAX_VALUE) {
            // TODO: We should instrument this error. While it is correct for it to be a 400 class IllegalArgumentException, there is not
            // much the user can do about that. If this occurs with any frequency, we should do something about it.
            throw new IllegalArgumentException(
                "Can't collect more than [" + Integer.MAX_VALUE + "] buckets but attempted [" + totalOrdsToCollect + "]"
            );
        }
        long[] bucketOrdsToCollect = new long[(int) totalOrdsToCollect];
        int b = 0;
        for (long owningBucketOrd : owningBucketOrds) {
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrd);
            while (ordsEnum.next()) {
                bucketOrdsToCollect[b++] = ordsEnum.ord();
            }
        }
        var subAggregationResults = buildSubAggsForBuckets(bucketOrdsToCollect);

        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        b = 0;
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            List<B> buckets = new ArrayList<>(bucketsInOrd[ordIdx]);
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            while (ordsEnum.next()) {
                if (bucketOrdsToCollect[b] != ordsEnum.ord()) {
                    // If we hit this, something has gone horribly wrong and we need to investigate
                    throw AggregationErrors.iterationOrderChangedWithoutMutating(
                        bucketOrds.toString(),
                        ordsEnum.ord(),
                        bucketOrdsToCollect[b]
                    );
                }
                buckets.add(bucketBuilder.build(ordsEnum.value(), bucketDocCount(ordsEnum.ord()), subAggregationResults.apply(b++)));
            }
            results[ordIdx] = resultBuilder.build(owningBucketOrds[ordIdx], buckets);
        }
        return results;
    }

    @FunctionalInterface
    protected interface BucketBuilderForVariable<B> {
        B build(long bucketValue, long docCount, InternalAggregations subAggregationResults);
    }

    @FunctionalInterface
    protected interface ResultBuilderForVariable<B> {
        InternalAggregation build(long owninigBucketOrd, List<B> buckets);
    }

    @Override
    public final void close() {
        try (Releasable releasable = docCounts) {
            super.close();
        }
    }

    @Override
    public Aggregator resolveSortPath(AggregationPath.PathElement next, Iterator<AggregationPath.PathElement> path) {
        if (this instanceof SingleBucketAggregator) {
            return resolveSortPathOnValidAgg(next, path);
        }
        return super.resolveSortPath(next, path);
    }

    @Override
    public BucketComparator bucketComparator(String key, SortOrder order) {
        if (false == this instanceof SingleBucketAggregator) {
            return super.bucketComparator(key, order);
        }
        if (key == null || "doc_count".equals(key)) {
            return (lhs, rhs) -> order.reverseMul() * Long.compare(bucketDocCount(lhs), bucketDocCount(rhs));
        }
        throw new IllegalArgumentException(String.format(Locale.ROOT, """
            Ordering on a single-bucket aggregation can only be done on its doc_count. \
            Either drop the key (a la "%s") or change it to "doc_count" (a la "%s.doc_count") or "key".""", name(), name()));
    }

    public static boolean descendsFromGlobalAggregator(Aggregator parent) {
        while (parent != null) {
            if (parent.getClass() == GlobalAggregator.class) {
                return true;
            }
            parent = parent.parent();
        }
        return false;
    }

    @Override
    protected void preGetSubLeafCollectors(LeafReaderContext ctx) throws IOException {
        super.preGetSubLeafCollectors(ctx);
        // Set LeafReaderContext to the doc_count provider
        docCountProvider.setLeafReaderContext(ctx);
    }
}
