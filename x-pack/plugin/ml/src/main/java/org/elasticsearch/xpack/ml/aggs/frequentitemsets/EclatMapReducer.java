/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.FrequentItemSetCollector.FrequentItemSet;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.TransactionStore.TopTransactionIds;
import org.elasticsearch.xpack.ml.aggs.mapreduce.AbstractMapReducer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Map Reducer that implements the eclat algorithm, see
 *
 * Zaki et.al. 1997 New Algorithms for Fast Discovery of Association Rules KDD'97, 283-296
 * Christian Borgelt 2003 Efficient Implementations of Apriori and Eclat FIMI 2003
 *
 * Eclat is a successor of the well known apriori algorithm, in contrast to apriori eclat
 * mines for item sets depth first instead of breath first. The main advantage is the lower
 * memory requirements. Depth-first only requires state with a small footprint in contrast to
 * a depth first approach which has to remember tons of sets in parallel.
 *
 * The implementation contains several adjustments and enhancements:
 *
 *  - top-N retrieval via the `size` parameter, this has a significant influence on runtime
 *  - multi field support, the classic apriori has only a bag of items of the same kind
 *  - mapping and merging results per shard
 *
 * Algorithmic differences:
 *
 *  - pruning/rewriting of items/transaction prior mining
 *  - a fixed size bitvector to speed up traversal for known branches (C. Borgelt uses a similar technique)
 *  - adaptive pruning based on a changing minimum support (for top-N retrieval)
 */
public class EclatMapReducer extends AbstractMapReducer<EclatMapReducer, TransactionStore, EclatMapReducer.EclatResult> {

    static class EclatResult implements ToXContent, Writeable {

        private final FrequentItemSet[] frequentItemSets;
        private final Map<String, Long> meta;

        EclatResult(FrequentItemSet[] frequentItemSets, Map<String, Long> meta) {
            this.frequentItemSets = frequentItemSets;
            this.meta = meta;
        }

        FrequentItemSet[] getFrequentItemSets() {
            // we allow mutating the item sets for sampling
            return frequentItemSets;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeArray(frequentItemSets);
            out.writeMap(meta, StreamOutput::writeString, StreamOutput::writeLong);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(CommonFields.BUCKETS.getPreferredName(), frequentItemSets);

            if (params != null && params.paramAsBoolean(SearchProfileResults.PROFILE_FIELD, false)) {
                builder.startObject("profile");
                builder.mapContents(meta);
                builder.endObject();
            }
            return builder;
        }
    }

    private static final Logger logger = LogManager.getLogger(EclatMapReducer.class);
    private static final int VERSION = 1;

    public static final String NAME = "frequent_items-eclat-" + VERSION;

    // cache for marking transactions visited, memory usage: ((BITSET_CACHE_TRAVERSAL_DEPTH -2) * BITSET_CACHE_NUMBER_OF_TRANSACTIONS) / 8
    private static final int MAX_BITSET_CACHE_NUMBER_OF_TRANSACTIONS = 65536;
    private static final int BITSET_CACHE_TRAVERSAL_DEPTH = 20;

    private final double minimumSupport;
    private final int minimumSetSize;
    private final int size;

    public EclatMapReducer(String aggregationName, double minimumSupport, int minimumSetSize, int size) {
        super(aggregationName);
        this.minimumSupport = minimumSupport;
        this.minimumSetSize = minimumSetSize;
        this.size = size;
    }

    public EclatMapReducer(StreamInput in) throws IOException {
        super(in);

        // parameters
        this.minimumSupport = in.readDouble();
        this.minimumSetSize = in.readInt();
        this.size = in.readInt();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public double getMinimumSupport() {
        return minimumSupport;
    }

    public int getMinimumSetSize() {
        return minimumSetSize;
    }

    public int getSize() {
        return size;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        // parameters
        out.writeDouble(minimumSupport);
        out.writeInt(minimumSetSize);
        out.writeInt(size);
    }

    @Override
    public TransactionStore doMapInit(BigArrays bigArrays) {
        return new TransactionStore(bigArrays);
    }

    @Override
    public TransactionStore doMap(Stream<Tuple<String, List<Object>>> keyValues, TransactionStore transactionStore) {
        transactionStore.add(keyValues);
        return transactionStore;
    }

    @Override
    public TransactionStore doReadMapContext(StreamInput in, BigArrays bigArrays) throws IOException {
        return new TransactionStore(in, bigArrays);
    }

    @Override
    public TransactionStore doReduceInit(BigArrays bigArrays) {
        return new TransactionStore(bigArrays);
    }

    @Override
    public TransactionStore doReduce(Stream<TransactionStore> partitions, TransactionStore transactionStore) {
        // we must iterate one at a time, because the transaction store isn't thread-safe
        partitions.forEachOrdered(p -> { transactionStore.mergeAndClose(p); });
        return transactionStore;
    }

    @Override
    public EclatResult doReduceFinalize(TransactionStore transactionStore) throws IOException {
        transactionStore.prune(minimumSupport);
        EclatResult frequentItemSets = eclat(transactionStore, minimumSupport, minimumSetSize, size);
        transactionStore.close();
        return frequentItemSets;
    }

    @Override
    public EclatResult doFinalizeSampling(SamplingContext samplingContext, EclatResult eclatResult) {
        for (FrequentItemSet itemSet : eclatResult.getFrequentItemSets()) {
            itemSet.setDocCount(samplingContext.scaleUp(itemSet.getDocCount()));
        }
        return eclatResult;
    }

    private static EclatResult eclat(TransactionStore transactionStore, double minimumSupport, int minimumSetSize, int size)
        throws IOException {
        final long relativeStartNanos = System.nanoTime();
        final long totalTransactionCount = transactionStore.getTotalTransactionCount();

        long minCount = (long) (totalTransactionCount * minimumSupport);
        FrequentItemSetCollector collector = new FrequentItemSetCollector(transactionStore, size, minCount);

        // implementation of a cache that remembers which transactions are still of interest up to depth and number of top transactions
        long[] transactionSkipCounts = new long[BITSET_CACHE_TRAVERSAL_DEPTH - 1];

        int cacheNumberOfTransactions = (int) Math.min(MAX_BITSET_CACHE_NUMBER_OF_TRANSACTIONS, totalTransactionCount);
        BitSet transactionSkipList = new FixedBitSet((BITSET_CACHE_TRAVERSAL_DEPTH - 1) * cacheNumberOfTransactions);

        try (
            TopTransactionIds topTransactionIds = transactionStore.getTopTransactionIds();
            ItemSetTraverser topItemSetTraverser = transactionStore.getTopItemIdTraverser();
        ) {
            long previousOccurences = 0;
            long occurences = 0;
            int previousDepth = 0;

            logger.trace(
                "total transaction count {}, min count: {}, total items: {}",
                totalTransactionCount,
                minCount,
                transactionStore.getTotalItemCount()
            );

            long previousMinCount = 0;
            while (topItemSetTraverser.next()) {
                // remember count only if we move down the tree, not up
                previousOccurences = topItemSetTraverser.getDepth() > previousDepth ? occurences : 0;

                // advance the iterator the next set of interest
                occurences = advanceIteratorToNextSet(
                    transactionStore,
                    totalTransactionCount,
                    minCount,
                    transactionSkipCounts,
                    cacheNumberOfTransactions,
                    transactionSkipList,
                    topTransactionIds,
                    topItemSetTraverser
                );

                // stop exploring the current branch if we fall below minCount
                if (occurences < minCount) {

                    /**
                     * This prunes the search tree, e.g.
                     *
                     * a - b - c - d
                     * |   |    \- h
                     * |   |\- e - f
                     * |    \- h - j
                     *  \- x - y
                     *
                     * if the current item set is [a, b, c] and now prune, we go back to [a, b] and iterate to [a, b, e] next.
                     */
                    topItemSetTraverser.prune();

                    /**
                     * Closed item sets: if we did not go up the tree before, collect the previous set (e.g. [a, b])
                     */
                    if (previousOccurences > minCount && topItemSetTraverser.getItemSet().size() >= minimumSetSize) {
                        minCount = collector.add(topItemSetTraverser.getItemSet(), previousOccurences);
                    }
                    continue;
                }

                /**
                 * Closed item sets: if the previous set has a higher count, add it to closed sets, e.g.
                 *
                 * [a, b]    -> 444
                 * [a, b, c] -> 345
                 *
                 * iff the count of the subset is higher, collect
                 */
                if (topItemSetTraverser.getItemSet().size() > minimumSetSize && occurences < previousOccurences) {
                    // add the set without the last item
                    minCount = collector.add(topItemSetTraverser.getItemSet().subList(0, previousDepth - 1), previousOccurences);
                }

                /**
                 * Optimization for top-N: the collector returns the count for the last(n-th) item set, the next best
                 * item set must have a higher count. During mining we can set the bar higher and prune earlier
                 */
                if (previousMinCount != minCount) {
                    previousMinCount = minCount;
                    logger.trace("adjusting min count to {}", minCount);
                }

                previousDepth = topItemSetTraverser.getDepth();
            }
            /**
             * report the very last set if necessary
             */
            if (topItemSetTraverser.getItemSet().size() >= minimumSetSize) {
                minCount = collector.add(topItemSetTraverser.getItemSet(), occurences);
            }
        }

        FrequentItemSet[] topFrequentItems = collector.finalizeAndGetResults();

        final long eclatRuntimeNanos = System.nanoTime() - relativeStartNanos;

        return new EclatResult(
            topFrequentItems,
            Map.of(
                "total_items",
                transactionStore.getTotalItemCount(),
                "total_transactions",
                transactionStore.getTotalTransactionCount(),
                "unique_items",
                transactionStore.getUniqueItemsCount(),
                "start_min_count",
                (long) (totalTransactionCount * minimumSupport),
                "end_min_count",
                minCount,
                "eclat_runtime_ms",
                TimeUnit.NANOSECONDS.toMillis(eclatRuntimeNanos)
            )
        );
    }

    /**
     * Find the next interesting item set.
     *
     * TODO: this could potentially be wrapped in an Iterator
     *
     * The traverser returns items that we haven't visited in this combination yet.
     *
     *  basic algorithm
     *
     *  - expand the set with the item reported by the traverser
     *  - re-calculate the count of transactions that contain the given item set
     *    - optimization: if we go down the tree, a bitset is used to skip transactions,
     *      that do not pass a previous step:
     *          if [a, b] is not in T, [a, b, c] can not be in T either
     *  - check which item sets should be reported, this is done lazily
     */
    private static long advanceIteratorToNextSet(
        TransactionStore transactionStore,
        final long totalTransactionCount,
        long minCount,
        long[] transactionSkipCounts,
        int cacheNumberOfTransactions,
        BitSet transactionSkipList,
        TopTransactionIds topTransactionIds,
        ItemSetTraverser topItemSetTraverser
    ) {
        int depth = topItemSetTraverser.getDepth();
        if (depth == 1) {
            // at the 1st level, we can take the count directly from the transaction store
            return transactionStore.getItemCount(topItemSetTraverser.getItemId());

            // till a certain depth store results in a cache matrix
        } else if (depth < BITSET_CACHE_TRAVERSAL_DEPTH) {
            // get the cached skip count
            long skipCount = transactionSkipCounts[depth - 2];

            // use the countdown from a previous iteration
            long maxReachableTransactionCount = totalTransactionCount - skipCount;

            // we recalculate the row for this depth, so we have to clear the bits first
            transactionSkipList.clear((depth - 1) * cacheNumberOfTransactions, ((depth) * cacheNumberOfTransactions));

            int transactionNumber = 0;
            long occurences = 0;

            for (Long transactionId : topTransactionIds) {
                // caching: if the transaction is already marked for skipping, quickly continue
                if (transactionNumber < cacheNumberOfTransactions
                    && transactionSkipList.get(cacheNumberOfTransactions * (depth - 2) + transactionNumber)) {
                    // set the bit for the next iteration
                    transactionSkipList.set(cacheNumberOfTransactions * (depth - 1) + transactionNumber);
                    transactionNumber++;
                    continue;
                }

                long transactionCount = transactionStore.getTransactionCount(transactionId);

                if (transactionStore.transactionContainAllIds(topItemSetTraverser.getItemSet(), transactionId)) {
                    occurences += transactionCount;
                } else if (transactionNumber < cacheNumberOfTransactions) {
                    // put this transaction to the skip list
                    skipCount += transactionCount;
                    transactionSkipList.set(cacheNumberOfTransactions * (depth - 1) + transactionNumber);
                }

                maxReachableTransactionCount -= transactionCount;
                // exit early if min support can't be reached
                if (maxReachableTransactionCount + occurences < minCount) {
                    break;
                }

                transactionNumber++;
            }

            transactionSkipCounts[depth - 1] = skipCount;

            return occurences;
            // deep traversal: use the last cached values, but don't store any
            // this is exactly the same (unrolled) code as before, but without writing to the cache
        }

        // get the last cached skip count
        long skipCount = transactionSkipCounts[BITSET_CACHE_TRAVERSAL_DEPTH - 2];

        // use the countdown from a previous iteration
        long maxReachableTransactionCount = totalTransactionCount - skipCount;

        int transactionNumber = 0;
        long occurences = 0;
        for (Long transactionId : topTransactionIds) {
            // caching: if the transaction is already marked for skipping, quickly continue
            if (transactionNumber < cacheNumberOfTransactions
                && transactionSkipList.get(cacheNumberOfTransactions * (BITSET_CACHE_TRAVERSAL_DEPTH - 2) + transactionNumber)) {
                transactionNumber++;
                continue;
            }

            long transactionCount = transactionStore.getTransactionCount(transactionId);

            if (transactionStore.transactionContainAllIds(topItemSetTraverser.getItemSet(), transactionId)) {
                occurences += transactionCount;
            }

            maxReachableTransactionCount -= transactionCount;

            // exit early if min support can't be reached
            if (maxReachableTransactionCount + occurences < minCount) {
                break;
            }

            transactionNumber++;
        }

        return occurences;
    }

}
