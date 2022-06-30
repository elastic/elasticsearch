/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.LongsRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.FrequentItemSetCollector.FrequentItemSet;
import org.elasticsearch.xpack.ml.aggs.mapreduce.AbstractMapReducer;
import org.elasticsearch.xpack.ml.aggs.mapreduce.ValuesExtractor.Field;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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
public final class EclatMapReducer extends AbstractMapReducer<
    HashBasedTransactionStore,
    ImmutableTransactionStore,
    HashBasedTransactionStore,
    EclatMapReducer.EclatResult> {

    private static final int ITERATION_CHECK_INTERVAL = 100000;

    static class EclatResult implements ToXContent, Writeable {

        private final FrequentItemSet[] frequentItemSets;
        private final Map<String, Long> meta;

        EclatResult(FrequentItemSet[] frequentItemSets, Map<String, Long> meta) {
            this.frequentItemSets = frequentItemSets;
            this.meta = meta;
        }

        EclatResult(StreamInput in) throws IOException {
            this.frequentItemSets = in.readArray(FrequentItemSet::new, FrequentItemSet[]::new);
            this.meta = in.readMap(StreamInput::readString, StreamInput::readLong);
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
        super(aggregationName, NAME);

        assert size > 0;
        assert minimumSetSize > 0;

        this.minimumSupport = minimumSupport;
        this.minimumSetSize = minimumSetSize;
        this.size = size;
    }

    public EclatMapReducer(String aggregationName, StreamInput in) throws IOException {
        super(aggregationName, NAME);

        // parameters
        this.minimumSupport = in.readDouble();
        this.minimumSetSize = in.readVInt();
        this.size = in.readVInt();
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
        out.writeVInt(minimumSetSize);
        out.writeVInt(size);
    }

    @Override
    public HashBasedTransactionStore mapInit(BigArrays bigArrays) {
        return new HashBasedTransactionStore(bigArrays);
    }

    @Override
    public HashBasedTransactionStore map(Stream<Tuple<Field, List<Object>>> keyValues, HashBasedTransactionStore transactionStore) {
        transactionStore.add(keyValues);
        return transactionStore;
    }

    @Override
    protected ImmutableTransactionStore mapFinalize(HashBasedTransactionStore transactionStore) {
        return transactionStore.createImmutableTransactionStore();
    }

    @Override
    public ImmutableTransactionStore readMapReduceContext(StreamInput in, BigArrays bigArrays) throws IOException {
        return new ImmutableTransactionStore(in, bigArrays);
    }

    @Override
    protected ImmutableTransactionStore combine(
        Stream<ImmutableTransactionStore> partitions,
        HashBasedTransactionStore transactionStore,
        Supplier<Boolean> isCanceledSupplier
    ) {
        HashBasedTransactionStore t = reduce(partitions, transactionStore, isCanceledSupplier);
        return t.createImmutableTransactionStore();
    }

    @Override
    public EclatResult readResult(StreamInput in, BigArrays bigArrays) throws IOException {
        return new EclatResult(in);
    }

    @Override
    public HashBasedTransactionStore reduceInit(BigArrays bigArrays) {
        return new HashBasedTransactionStore(bigArrays);
    }

    @Override
    public HashBasedTransactionStore reduce(
        Stream<ImmutableTransactionStore> partitions,
        HashBasedTransactionStore transactionStore,
        Supplier<Boolean> isCanceledSupplier
    ) {
        // we must iterate one at a time, because the transaction store isn't thread-safe
        partitions.forEachOrdered(p -> {
            try {
                if (isCanceledSupplier.get()) {
                    throw new TaskCancelledException("Cancelled");
                }
                transactionStore.merge(p);
            } catch (IOException e) {
                throw new AggregationExecutionException("Failed to merge shard results", e);
            } finally {
                Releasables.close(p);
            }
        });

        return transactionStore;
    }

    @Override
    public EclatResult reduceFinalize(
        HashBasedTransactionStore transactionStore,
        List<String> fieldNames,
        Supplier<Boolean> isCanceledSupplier
    ) throws IOException {
        transactionStore.prune(minimumSupport);

        if (isCanceledSupplier.get()) {
            throw new TaskCancelledException("Cancelled");
        }

        // by dropping the hashes we can free some resources before the miner allocates again
        try (TransactionStore immutableTransactionStore = transactionStore.createImmutableTransactionStore()) {
            transactionStore.close();
            EclatResult frequentItemSets = eclat(
                immutableTransactionStore,
                minimumSupport,
                minimumSetSize,
                size,
                fieldNames,
                isCanceledSupplier
            );
            return frequentItemSets;
        }
    }

    @Override
    public EclatResult finalizeSampling(SamplingContext samplingContext, EclatResult eclatResult) {
        for (FrequentItemSet itemSet : eclatResult.getFrequentItemSets()) {
            itemSet.setDocCount(samplingContext.scaleUp(itemSet.getDocCount()));
        }
        return eclatResult;
    }

    private static EclatResult eclat(
        TransactionStore transactionStore,
        double minimumSupport,
        int minimumSetSize,
        int size,
        List<String> fieldNames,
        Supplier<Boolean> isCanceledSupplier
    ) throws IOException {
        final long relativeStartNanos = System.nanoTime();
        final long totalTransactionCount = transactionStore.getTotalTransactionCount();

        long minCount = (long) Math.ceil(totalTransactionCount * minimumSupport);
        FrequentItemSetCollector collector = new FrequentItemSetCollector(transactionStore, size, minCount);
        long numberOfSetsChecked = 0;

        try (
            CountingItemSetTraverser setTraverser = new CountingItemSetTraverser(
                transactionStore,
                BITSET_CACHE_TRAVERSAL_DEPTH,
                (int) Math.min(MAX_BITSET_CACHE_NUMBER_OF_TRANSACTIONS, totalTransactionCount),
                minCount
            );
        ) {
            logger.trace(
                "total transaction count {}, min count: {}, total items: {}",
                totalTransactionCount,
                minCount,
                transactionStore.getTotalItemCount()
            );

            long previousMinCount = 0;
            while (setTraverser.next(minCount)) {
                if (numberOfSetsChecked % ITERATION_CHECK_INTERVAL == 0) {
                    logger.debug("checked {} sets", numberOfSetsChecked);

                    if (isCanceledSupplier.get()) {
                        final long eclatRuntimeNanos = System.nanoTime() - relativeStartNanos;
                        logger.debug(
                            "eclat has been cancelled after {} iterations and a runtime of {}s",
                            numberOfSetsChecked,
                            TimeUnit.NANOSECONDS.toSeconds(eclatRuntimeNanos)
                        );
                        throw new TaskCancelledException("Cancelled");
                    }
                }
                numberOfSetsChecked++;

                // Step 2: check which item sets should be reported, this is done lazily
                // stop exploring the current branch if we fall below minCount
                if (setTraverser.getCount() < minCount) {

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
                    setTraverser.prune();

                    /**
                     * Closed item sets: if we did not go up the tree before, collect the previous set (e.g. [a, b])
                     */
                    if (setTraverser.atLeaf()
                        && setTraverser.hasBeenVisited() == false
                        && setTraverser.getCount() >= minCount
                        && setTraverser.getItemSet().length >= minimumSetSize) {
                        minCount = collector.add(setTraverser.getItemSet(), setTraverser.getCount());
                        // no need to set visited, as we are on a leaf
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
                if (setTraverser.hasPredecessorBeenVisited() == false
                    && setTraverser.getItemSet().length > minimumSetSize
                    && setTraverser.getCount() < setTraverser.getPreviousCount()) {
                    // add the set without the last item

                    LongsRef subItemSet = setTraverser.getItemSet().clone();
                    subItemSet.length--;
                    minCount = collector.add(subItemSet, setTraverser.getPreviousCount());
                }

                // closed set criteria: the predecessor is no longer of interest: either we reported in the previous step or we found a
                // super set
                setTraverser.setPredecessorVisited();

                /**
                 * Iff the traverser reached a leaf, the item set can not be further expanded, e.g. we reached [f]:
                 *
                 * a - b - c - d
                 * |   |    \- h
                 * |   |\- e - [f]
                 * |    \- h - j
                 *  \- x - y
                 *
                 * Advancing the iterator would bring us to [a, b, h], so we must report the found item set now
                 *
                 * Note: this also covers the last item, e.g. [a, x, y]
                 */
                if (setTraverser.atLeaf() && setTraverser.getItemSet().length >= minimumSetSize) {
                    minCount = collector.add(setTraverser.getItemSet(), setTraverser.getCount());
                    // no need to set visited, as we are on a leaf
                }

                /**
                 * Optimization for top-N: the collector returns the count for the last(n-th) item set, the next best
                 * item set must have a higher count. During mining we can set the bar higher and prune earlier
                 */
                if (previousMinCount != minCount) {
                    previousMinCount = minCount;
                    logger.debug("adjusting min count to {}", minCount);
                }
            }
        }

        FrequentItemSet[] topFrequentItems = collector.finalizeAndGetResults(fieldNames);

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
                TimeUnit.NANOSECONDS.toMillis(eclatRuntimeNanos),
                "item_sets_checked",
                numberOfSetsChecked
            )
        );
    }
}
