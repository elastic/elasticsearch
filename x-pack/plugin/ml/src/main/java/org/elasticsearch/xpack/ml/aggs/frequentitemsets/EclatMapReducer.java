/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.TransactionStore.TopItemIds;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.AbstractItemSetMapReducer;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.Field;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
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
public final class EclatMapReducer extends AbstractItemSetMapReducer<
    HashBasedTransactionStore,
    ImmutableTransactionStore,
    HashBasedTransactionStore,
    EclatMapReducer.EclatResult> {

    private static final int ITERATION_CHECK_INTERVAL = 100000;

    static class EclatResult implements ToXContent, Writeable {

        private final FrequentItemSet[] frequentItemSets;
        private final Map<String, Object> profilingInfo;

        EclatResult(FrequentItemSet[] frequentItemSets, @Nullable Map<String, Object> profilingInfo) {
            this.frequentItemSets = frequentItemSets;
            this.profilingInfo = profilingInfo == null ? Collections.emptyMap() : profilingInfo;
        }

        EclatResult(StreamInput in) throws IOException {
            this.frequentItemSets = in.readArray(FrequentItemSet::new, FrequentItemSet[]::new);
            this.profilingInfo = in.readOrderedMap(StreamInput::readString, StreamInput::readGenericValue);
        }

        FrequentItemSet[] getFrequentItemSets() {
            // we allow mutating the item sets for sampling
            return frequentItemSets;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeArray(frequentItemSets);
            out.writeMap(profilingInfo, StreamOutput::writeString, StreamOutput::writeGenericValue);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(CommonFields.BUCKETS.getPreferredName(), frequentItemSets);

            if (params != null && params.paramAsBoolean(SearchProfileResults.PROFILE_FIELD, false)) {
                builder.startObject("profile");
                builder.mapContents(profilingInfo);
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
    private static final int BITSET_CACHE_TRAVERSAL_DEPTH = 60;

    private final double minimumSupport;
    private final int minimumSetSize;
    private final int size;
    private final boolean profiling;

    // only for profiling, not serialized
    // we must differ between map and reduce parts
    private final Map<String, Object> profilingInfoReduce;
    private final Map<String, Object> profilingInfoMap;

    public EclatMapReducer(String aggregationName, double minimumSupport, int minimumSetSize, int size, boolean profiling) {
        super(aggregationName, NAME);

        assert size > 0;
        assert minimumSetSize > 0;

        this.minimumSupport = minimumSupport;
        this.minimumSetSize = minimumSetSize;
        this.size = size;
        this.profiling = profiling;
        this.profilingInfoReduce = profiling ? new LinkedHashMap<>() : null;
        this.profilingInfoMap = profiling ? new LinkedHashMap<>() : null;
    }

    public EclatMapReducer(String aggregationName, StreamInput in) throws IOException {
        super(aggregationName, NAME);

        // parameters
        this.minimumSupport = in.readDouble();
        this.minimumSetSize = in.readVInt();
        this.size = in.readVInt();
        this.profiling = in.readBoolean();
        this.profilingInfoReduce = profiling ? new LinkedHashMap<>() : null;
        this.profilingInfoMap = profiling ? new LinkedHashMap<>() : null;
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
        out.writeBoolean(profiling);
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

        // reported by shard in collectDebugInfo
        if (profiling) {
            // note: collect this before creating the immutable transaction store as memory gets freed
            profilingInfoMap.put("ram_bytes_transactionstore_after_map", transactionStore.ramBytesUsed());
            profilingInfoMap.put("total_items_after_map", transactionStore.getTotalItemCount());
            profilingInfoMap.put("total_transactions_after_map", transactionStore.getTotalTransactionCount());
            profilingInfoMap.put("unique_items_after_map", transactionStore.getUniqueItemsCount());
            profilingInfoMap.put("unique_transactions_after_map", transactionStore.getUniqueTransactionCount());
        }

        ImmutableTransactionStore transactionStoreMapFinalize = transactionStore.createImmutableTransactionStore();

        if (profiling) {
            profilingInfoMap.put("ram_bytes_transactionstore_map_finalize", transactionStoreMapFinalize.ramBytesUsed());
        }

        return transactionStoreMapFinalize;
    }

    @Override
    protected void collectDebugInfo(BiConsumer<String, Object> add) {
        profilingInfoMap.forEach(add);
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
        final AtomicLong ramBytesSum = new AtomicLong();

        // we must iterate one at a time, because the transaction store isn't thread-safe
        partitions.forEachOrdered(p -> {
            try {
                if (isCanceledSupplier.get()) {
                    throw new TaskCancelledException("Cancelled");
                }
                if (profiling) {
                    ramBytesSum.addAndGet(p.ramBytesUsed());
                }
                transactionStore.merge(p);
            } catch (IOException e) {
                throw new AggregationExecutionException("Failed to merge shard results", e);
            } finally {
                Releasables.close(p);
            }
        });

        // reduction can be executed in several steps, if reduce has been called before take the max
        if (profiling) {
            profilingInfoReduce.compute(
                "max_ram_bytes_sum_transactionstore_before_merge",
                (k, v) -> (v == null) ? ramBytesSum.get() : Math.max(ramBytesSum.get(), (long) v)
            );
        }

        return transactionStore;
    }

    @Override
    public EclatResult reduceFinalize(HashBasedTransactionStore transactionStore, List<Field> fields, Supplier<Boolean> isCanceledSupplier)
        throws IOException {
        if (profiling) {
            profilingInfoReduce.put("ram_bytes_transactionstore_after_reduce", transactionStore.ramBytesUsed());
            profilingInfoReduce.put("total_items_after_reduce", transactionStore.getTotalItemCount());
            profilingInfoReduce.put("total_transactions_after_reduce", transactionStore.getTotalTransactionCount());
            profilingInfoReduce.put("unique_items_after_reduce", transactionStore.getUniqueItemsCount());
            profilingInfoReduce.put("unique_transactions_after_reduce", transactionStore.getUniqueTransactionCount());
        }

        transactionStore.prune(minimumSupport);

        if (profiling) {
            profilingInfoReduce.put("ram_bytes_transactionstore_after_prune", transactionStore.ramBytesUsed());
            profilingInfoReduce.put("total_items_after_prune", transactionStore.getTotalItemCount());
            profilingInfoReduce.put("total_transactions_after_prune", transactionStore.getTotalTransactionCount());
            profilingInfoReduce.put("unique_items_after_prune", transactionStore.getUniqueItemsCount());
            profilingInfoReduce.put("unique_transactions_after_prune", transactionStore.getUniqueTransactionCount());
        }

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
                fields,
                isCanceledSupplier,
                profilingInfoReduce
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
        List<Field> fields,
        Supplier<Boolean> isCanceledSupplier,
        Map<String, Object> profilingInfoReduce
    ) throws IOException {
        final long relativeStartNanos = System.nanoTime();
        final long totalTransactionCount = transactionStore.getTotalTransactionCount();
        Map<String, Object> profilingInfo = null;
        long minCount = (long) Math.ceil(totalTransactionCount * minimumSupport);

        if (profilingInfoReduce != null) {
            profilingInfo = new LinkedHashMap<>(profilingInfoReduce);
            profilingInfo.put("start_min_count_eclat", minCount);
        }

        try (
            TopItemIds topItemIds = transactionStore.getTopItemIds();
            CountingItemSetTraverser setTraverser = new CountingItemSetTraverser(
                transactionStore,
                topItemIds,
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
            FrequentItemSetCollector collector = new FrequentItemSetCollector(transactionStore, topItemIds, size, minCount);
            long numberOfSetsChecked = 0;
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
                        && setTraverser.getItemSetBitSet().cardinality() >= minimumSetSize) {

                        logger.trace("add after prune");

                        minCount = collector.add(setTraverser.getItemSetBitSet(), setTraverser.getCount());
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
                if (setTraverser.hasParentBeenVisited() == false
                    && setTraverser.getItemSetBitSet().cardinality() > minimumSetSize
                    && setTraverser.getCount() < setTraverser.getParentCount()) {
                    // add the set without the last item

                    minCount = collector.add(setTraverser.getParentItemSetBitSet(), setTraverser.getParentCount());
                }

                // closed set criteria: the predecessor is no longer of interest: either we reported in the previous step or we found a
                // super set
                setTraverser.setParentVisited();

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
                if (setTraverser.atLeaf() && setTraverser.getItemSetBitSet().cardinality() >= minimumSetSize) {
                    minCount = collector.add(setTraverser.getItemSetBitSet(), setTraverser.getCount());
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
            FrequentItemSet[] topFrequentItems = collector.finalizeAndGetResults(fields);
            final long eclatRuntimeNanos = System.nanoTime() - relativeStartNanos;

            if (profilingInfoReduce != null) {
                profilingInfo.put("end_min_count_eclat", minCount);
                profilingInfo.put("runtime_ms_eclat", TimeUnit.NANOSECONDS.toMillis(eclatRuntimeNanos));
                profilingInfo.put("item_sets_checked_eclat", numberOfSetsChecked);
            }

            return new EclatResult(topFrequentItems, profilingInfo);
        }
    }
}
