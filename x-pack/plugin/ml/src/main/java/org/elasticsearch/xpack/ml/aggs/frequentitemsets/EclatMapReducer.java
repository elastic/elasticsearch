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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.TransactionStore.TopItemIds;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.TransactionStore.TopTransactionIds;
import org.elasticsearch.xpack.ml.aggs.mapreduce.MapReducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
public class EclatMapReducer implements MapReducer {
    private static final Logger logger = LogManager.getLogger(EclatMapReducer.class);

    private static final int VERSION = 1;
    public static final String NAME = "frequent_items-eclat-" + VERSION;

    // cache for marking transactions visited, memory usage: ((BITSET_CACHE_TRAVERSAL_DEPTH -2) * BITSET_CACHE_NUMBER_OF_TRANSACTIONS) / 8
    private static final int MAX_BITSET_CACHE_NUMBER_OF_TRANSACTIONS = 65536;
    private static final int BITSET_CACHE_TRAVERSAL_DEPTH = 20;

    private final String aggregationWritableName;

    private final double minimumSupport;
    private final int minimumSetSize;
    private final int size;

    private Iterable<FrequentItemSetCollector.FrequentItemSet> frequentSets = null;
    private TransactionStore transactionStore;
    private long eclatRuntimeNanos = 0;

    public EclatMapReducer(String aggregationWritableName, double minimumSupport, int minimumSetSize, int size) {
        this.aggregationWritableName = aggregationWritableName;
        this.minimumSupport = minimumSupport;
        this.minimumSetSize = minimumSetSize;
        this.size = size;

        // TODO: big arrays should be taken from AggregationContext
        this.transactionStore = new TransactionStore(BigArrays.NON_RECYCLING_INSTANCE);
    }

    public EclatMapReducer(StreamInput in) throws IOException {
        this.aggregationWritableName = in.readString();

        // parameters
        this.minimumSupport = in.readDouble();
        this.minimumSetSize = in.readInt();
        this.size = in.readInt();

        // data
        this.transactionStore = new TransactionStore(in, BigArrays.NON_RECYCLING_INSTANCE);

        // not send over the wire
        this.frequentSets = null;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getAggregationWritableName() {
        return aggregationWritableName;
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
        out.writeString(aggregationWritableName);

        // parameters
        out.writeDouble(minimumSupport);
        out.writeInt(minimumSetSize);
        out.writeInt(size);

        // data
        transactionStore.writeTo(out);
    }

    @Override
    public void map(Stream<Tuple<String, List<Object>>> keyValues) {
        transactionStore.add(keyValues);
    }

    @Override
    public void reduce(Stream<MapReducer> partitions) {
        TransactionStore transactionsReduced = new TransactionStore(BigArrays.NON_RECYCLING_INSTANCE);

        // we must iterate one at a time, because the transaction store isn't thread-safe
        partitions.forEachOrdered(p -> {
            EclatMapReducer aprioriPartition = (EclatMapReducer) p;
            transactionsReduced.mergeAndClose(aprioriPartition.transactionStore);
        });

        transactionStore.close();
        transactionStore = transactionsReduced;
    }

    @Override
    public void reduceFinalize() throws IOException {
        transactionStore.prune(minimumSupport);
        eclat();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (frequentSets != null) {
            builder.field(CommonFields.BUCKETS.getPreferredName(), frequentSets);
        }

        if (params != null && params.paramAsBoolean(SearchProfileResults.PROFILE_FIELD, false)) {
            builder.startObject("profile");

            // some useful data
            builder.field("total_items", transactionStore.getTotalItemCount());
            builder.field("total_transactions", transactionStore.getTotalTransactionCount());
            builder.field("unique_items", transactionStore.getUniqueItemsCount());
            builder.field("eclat_runtime_ms", TimeUnit.NANOSECONDS.toMillis(this.eclatRuntimeNanos));

            // items
            builder.startArray("items");

            TopItemIds topIds = null;

            try {
                topIds = transactionStore.getTopItemIds();
                for (Long id : topIds) {
                    Tuple<String, String> item = transactionStore.getItem(id);
                    builder.startObject();
                    builder.field("id", id.toString());
                    builder.field(item.v1(), item.v2());
                    builder.field(CommonFields.DOC_COUNT.getPreferredName(), transactionStore.getItemCount(id));
                    builder.endObject();
                }
            } finally {
                Releasables.close(topIds);
            }

            builder.endArray();
            builder.endObject();
        }
        return builder;
    }

    private void eclat() throws IOException {
        final long relativeStartNanos = System.nanoTime();
        final long totalTransactionCount = transactionStore.getTotalTransactionCount();

        ItemSetTraverser topItemSetTraverser = null;
        TopTransactionIds topTransactionIds = null;
        long minCount = (long) (totalTransactionCount * minimumSupport);
        FrequentItemSetCollector collector = new FrequentItemSetCollector(transactionStore, size, minCount);

        // implementation of a cache that remembers which transactions are still of interest up to depth and number of top transactions
        long[] transactionSkipCounts = new long[BITSET_CACHE_TRAVERSAL_DEPTH - 1];

        int cacheNumberOfTransactions = (int) Math.min(MAX_BITSET_CACHE_NUMBER_OF_TRANSACTIONS, totalTransactionCount);
        BitSet transactionSkipList = new FixedBitSet((BITSET_CACHE_TRAVERSAL_DEPTH - 1) * cacheNumberOfTransactions);

        try {
            // get the releasables
            topTransactionIds = transactionStore.getTopTransactionIds();
            topItemSetTraverser = transactionStore.getTopItemIdTraverser();

            long previousOccurences = 0;
            long occurences = 0;
            int depth = 0;
            int transactionNumber = 0;

            logger.trace(
                "total transaction count {}, min count: {}, total items: {}",
                totalTransactionCount,
                minCount,
                transactionStore.getTotalItemCount()
            );

            long previousMinCount = 0;
            while (topItemSetTraverser.next()) {
                // remember count only if we move down the tree, not up
                previousOccurences = topItemSetTraverser.getDepth() > depth ? occurences : 0;
                depth = topItemSetTraverser.getDepth();

                // at the 1st level, we can take the count directly from the transaction store
                if (depth == 1) {
                    occurences = transactionStore.getItemCount(topItemSetTraverser.getItemId());

                    // till a certain depth store results in a cache matrix
                } else if (depth < BITSET_CACHE_TRAVERSAL_DEPTH) {
                    // get the cached skip count
                    long skipCount = transactionSkipCounts[depth - 2];

                    // use the countdown from a previous iteration
                    long maxReachableTransactionCount = totalTransactionCount - skipCount;

                    // we recalculate the row for this depth, so we have to clear the bits first
                    transactionSkipList.clear((depth - 1) * cacheNumberOfTransactions, ((depth) * cacheNumberOfTransactions));

                    transactionNumber = 0;
                    occurences = 0;

                    for (Long transactionId : topTransactionIds) {
                        // caching: if the transaction is already marked for skipping, quickly continue
                        if (transactionNumber < cacheNumberOfTransactions) {
                            if (transactionSkipList.get(cacheNumberOfTransactions * (depth - 2) + transactionNumber)) {
                                // set the bit for the next iteration
                                transactionSkipList.set(cacheNumberOfTransactions * (depth - 1) + transactionNumber);
                                transactionNumber++;
                                continue;
                            }
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

                    // deep traversal: use the last cached values, but don't store any
                } else {
                    // get the last cached skip count
                    long skipCount = transactionSkipCounts[BITSET_CACHE_TRAVERSAL_DEPTH - 2];

                    // use the countdown from a previous iteration
                    long maxReachableTransactionCount = totalTransactionCount - skipCount;

                    occurences = 0;
                    for (Long transactionId : topTransactionIds) {
                        // caching: if the transaction is already marked for skipping, quickly continue
                        if (transactionNumber < cacheNumberOfTransactions) {
                            if (transactionSkipList.get(
                                cacheNumberOfTransactions * (BITSET_CACHE_TRAVERSAL_DEPTH - 2) + transactionNumber
                            )) {
                                transactionNumber++;
                                continue;
                            }
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
                }

                // stop exploring the current branch if we fall below minCount
                if (occurences < minCount) {
                    topItemSetTraverser.prune();
                    if (previousOccurences > minCount && topItemSetTraverser.getItemSet().size() >= minimumSetSize) {
                        minCount = collector.add(topItemSetTraverser.getItemSet(), previousOccurences);
                    }
                    continue;
                }

                // if the previous set has a higher count, add it to closed sets
                if (topItemSetTraverser.getItemSet().size() > minimumSetSize && occurences < previousOccurences) {
                    // add the set without the last item
                    minCount = collector.add(topItemSetTraverser.getItemSet().subList(0, depth - 1), previousOccurences);
                }

                // if we reached a leaf, we can not extend the itemset further and return the result
                if (topItemSetTraverser.hasNext() == false && topItemSetTraverser.getItemSet().size() >= minimumSetSize) {
                    minCount = collector.add(topItemSetTraverser.getItemSet(), occurences);
                }

                if (previousMinCount != minCount) {
                    previousMinCount = minCount;
                    logger.trace("adjusting min count to {}", minCount);
                }
            }

        } finally {
            Releasables.close(topItemSetTraverser, topTransactionIds);
        }

        // the collector queue is implemented as heap and its iterator returns an unordered set,
        // to get a sorted set pop the heap in reverse order
        FrequentItemSetCollector.FrequentItemSet[] topFrequentItems = new FrequentItemSetCollector.FrequentItemSet[collector.size()];
        for (int i = topFrequentItems.length - 1; i >= 0; i--) {
            topFrequentItems[i] = collector.pop();
        }
        this.frequentSets = Arrays.asList(topFrequentItems);

        // TODO: we could get rid of the transactions at this point, but we still need the items
        final long relativeEndNanos = System.nanoTime();
        eclatRuntimeNanos = relativeEndNanos - relativeStartNanos;
    }

}
