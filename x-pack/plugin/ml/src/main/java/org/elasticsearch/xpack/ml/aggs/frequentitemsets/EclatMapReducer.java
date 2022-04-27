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

public class EclatMapReducer implements MapReducer {
    private static final Logger logger = LogManager.getLogger(EclatMapReducer.class);

    private static final int VERSION = 1;
    public static final String NAME = "frequent_items-eclat-" + VERSION;

    // cache for marking transactions visited, memory usage: ((BITSET_CACHE_TRAVERSAL_DEPTH -2) * BITSET_CACHE_NUMBER_OF_TRANSACTIONS) / 8
    private static final int MAX_BITSET_CACHE_NUMBER_OF_TRANSACTIONS = 65536; // 256;
    private static final int BITSET_CACHE_TRAVERSAL_DEPTH = 20; // 12;

    private final String aggregationWritableName;

    private final double minimumSupport;
    private final int minimumSetSize;
    private final int size;

    private Iterable<FrequentItemSetCollector.FrequentItemSet> frequentSets = null;

    private TransactionStore transactionStore;

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

        TopItemIds topIds = null;
        TopTransactionIds topTransactionIds = null;
        long minCount = (long) (totalTransactionCount * minimumSupport);
        FrequentItemSetCollector collector = new FrequentItemSetCollector(transactionStore, size, minCount);

        // implementation of a cache that remembers which transactions are still of interest up to depth and number of top transactions
        long[] transactionSkipCounts = new long[BITSET_CACHE_TRAVERSAL_DEPTH - 1];

        int cacheNumberOfTransactions = (int) Math.min(MAX_BITSET_CACHE_NUMBER_OF_TRANSACTIONS, totalTransactionCount);
        BitSet transactionSkipList = new FixedBitSet((BITSET_CACHE_TRAVERSAL_DEPTH - 1) * cacheNumberOfTransactions);

        try {
            // top items
            topIds = transactionStore.getTopItemIds();

            // top transactions
            topTransactionIds = transactionStore.getTopTransactionIds();

            long previousOccurences = 0;
            long occurences = 0;
            int depth = 0;
            int transactionNumber = 0;

            logger.info(
                "total transaction count {}, min count: {}, total items: {}",
                totalTransactionCount,
                minCount,
                transactionStore.getTotalItemCount()
            );
            logger.info("start iteration has {} items", topIds.size());

            // TODO: who owns topIds?
            ItemSetTraverser it = new ItemSetTraverser(topIds);

            // DEBUG, to be removed
            long oldMinCount = 0;
            while (it.next()) {
                // remember count only if we move down the tree, not up
                previousOccurences = it.getDepth() > depth ? occurences : 0;
                depth = it.getDepth();

                // at the 1st level, we can take the count directly from the transaction store
                if (depth == 1) {
                    occurences = transactionStore.getItemCount(it.getItemId());

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

                        if (transactionStore.transactionContainAllIds(it.getItemSet(), transactionId)) {
                            occurences += transactionCount;
                        } else if (transactionNumber < cacheNumberOfTransactions) {
                            // put this transaction to the skip list
                            skipCount += transactionCount;
                            transactionSkipList.set(cacheNumberOfTransactions * (depth - 1) + transactionNumber);
                        }

                        maxReachableTransactionCount -= transactionCount;
                        // exit early if min support can't be reached
                        if (maxReachableTransactionCount + occurences < minCount) {
                            // logger.info("quit early {} {}", countDown, occurences);
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

                        if (transactionStore.transactionContainAllIds(it.getItemSet(), transactionId)) {
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
                // logger.info("current item set: {}, occ: {}", Strings.collectionToDelimitedString(it.getItemSet(), ","), occurences);

                if (occurences < minCount) {
                    it.prune();
                    // logger.info("prune");
                    if (previousOccurences > minCount && it.getItemSet().size() >= minimumSetSize) {
                        minCount = collector.add(it.getItemSet(), previousOccurences);
                    }
                    continue;
                }

                // if the previous set has a higher count, add it to closed sets
                if (it.getItemSet().size() > minimumSetSize && occurences < previousOccurences) {
                    // add the set without the last item
                    minCount = collector.add(it.getItemSet().subList(0, depth - 1), previousOccurences);
                }

                // if we reached a leaf, we can not extend the itemset further and return the result
                if (it.hasNext() == false && it.getItemSet().size() >= minimumSetSize) {
                    minCount = collector.add(it.getItemSet(), occurences);
                }

                // DEBUG: to be removed
                if (oldMinCount != minCount) {
                    oldMinCount = minCount;
                    logger.info("min count is now {}", minCount);
                }
            }

        } finally {
            Releasables.close(topIds, topTransactionIds);
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

        logger.info("eclat runtime {} ms", TimeUnit.NANOSECONDS.toMillis(relativeEndNanos - relativeStartNanos));
    }

}
