/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.BytesRefStreamOutput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.Field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Implementation of "transactions", as they are called in
 *
 * Zaki, M. J. (2000). Scalable algorithms for association mining. Knowledge and Data Engineering, IEEE Transactions on, 12(3), 372-390.
 *
 * A transaction is defined as a set of items. For scaling transactions are de-duplicated and store a counter.
 *
 * NOTE: This class is not thread-safe!
 *
 * # Rough design
 *
 * ## Data:
 *
 * - transactions are split into items and items are stored together with their count
 *    - every item has an id and a count
 * - the transaction than gets rewritten into an array of items using the item ids, this dedups and compresses the storage
 *    - every transaction is a bytebuffer containing item ids and a count
 * - items and transactions are stored in a hashtable using ByteRefHash, which internally uses BigArrays
 *    - the item store is basically a map: ByteRef -> item_id, the ByteRef key consists of the fieldname and its value,
 *      the id is number (a count)
 *    - the transaction store is a map: ByteRef -> transaction_id, the ByteRef consists of item_id's using a variable length
 *      encoding (vlong)
 * - the counts are stored as array, the index is {item_, transaction_}the id, the cell stores the count
 * - the store can be serialized and send over the wire, serialization and deserialization uses variable length encodings to
 *   make messages smaller
 *
 * ## Merging
 *
 * - a transaction store contains the items and transactions of 1 shard, this must be merged
 * - transaction stores can be merged, this happens by rewriting and merging
 *
 * ## Pruning
 *
 * - prune removes all items that do not meet the support criteria and rewrites/drops transactions, this can significantly free memory
 * - TODO: pruning is only used after merging all stores from all shards, it could be beneficial to prune already on the shard level
 *
 * ## Top Items/Transactions
 *
 * - top_{items, transactions} creates lists of {item_, transaction_}ids reverse sorted by count
 *
 */
public final class HashBasedTransactionStore extends TransactionStore {

    private static final Logger logger = LogManager.getLogger(HashBasedTransactionStore.class);

    private static final int INITIAL_ITEM_CAPACITY = PageCacheRecycler.LONG_PAGE_SIZE;
    private static final int INITIAL_TRANSACTION_CAPACITY = PageCacheRecycler.LONG_PAGE_SIZE;
    private static final int CAPACITY_INCREMENT = PageCacheRecycler.LONG_PAGE_SIZE;

    // re-use bytes ref object
    private final BytesRefStreamOutput scratchItemBytesStreamOutput = new BytesRefStreamOutput();
    private final BytesRefStreamOutput scratchTransactionBytesStreamOutput = new BytesRefStreamOutput();

    // base size of transaction store, update if you add classes
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(HashBasedTransactionStore.class) + 2
        * RamUsageEstimator.shallowSizeOfInstance(BytesStreamOutput.class) + 2 * RamUsageEstimator.shallowSizeOfInstance(BytesRefHash.class)
        + 2 * RamUsageEstimator.shallowSizeOfInstance(LongArray.class);

    // data holders
    private BytesRefHash items;
    private LongArray itemCounts;
    private long totalItemCount;
    private BytesRefHash transactions;
    private LongArray transactionCounts;
    private long totalTransactionCount;

    public HashBasedTransactionStore(BigArrays bigArrays) {
        super(bigArrays);
        // we allocate big arrays so we have to `close` if we fail here or we'll leak them.
        boolean success = false;

        try {
            this.items = new BytesRefHash(INITIAL_ITEM_CAPACITY, bigArrays);
            this.itemCounts = bigArrays.newLongArray(INITIAL_ITEM_CAPACITY, true);
            this.transactions = new BytesRefHash(INITIAL_TRANSACTION_CAPACITY, bigArrays);
            this.transactionCounts = bigArrays.newLongArray(INITIAL_TRANSACTION_CAPACITY, true);
            success = true;
        } finally {
            if (false == success) {
                close();
            }
        }
    }

    public HashBasedTransactionStore(StreamInput in, BigArrays bigArrays) throws IOException {
        super(bigArrays);

        // we allocate big arrays so we have to `close` if we fail here or we'll leak them.
        boolean success = false;

        // these 2 arrays must be closed if cbe throws at construction
        BytesRefArray itemsArray = null;
        BytesRefArray transactionsArray = null;

        try {
            itemsArray = new BytesRefArray(in, bigArrays);
            this.items = new BytesRefHash(itemsArray, bigArrays);
            // unassign to not double close on error
            itemsArray = null;

            long itemCountsSize = in.readVLong();
            this.itemCounts = bigArrays.newLongArray(itemCountsSize, true);
            for (int i = 0; i < itemCountsSize; ++i) {
                itemCounts.set(i, in.readVLong());
            }
            this.totalItemCount = in.readVLong();
            transactionsArray = new BytesRefArray(in, bigArrays);
            this.transactions = new BytesRefHash(transactionsArray, bigArrays);
            // unassign to not double close on error
            transactionsArray = null;

            long transactionsCountsSize = in.readVLong();
            this.transactionCounts = bigArrays.newLongArray(transactionsCountsSize, true);
            for (int i = 0; i < transactionsCountsSize; ++i) {
                transactionCounts.set(i, in.readVLong());
            }
            this.totalTransactionCount = in.readVLong();

            success = true;
        } finally {
            if (false == success) {
                try (Releasable releasable = Releasables.wrap(itemsArray, transactionsArray)) {
                    close();
                }
            }
        }
    }

    /**
     * Add a single transaction to the store.
     *
     * @param keyValues a single transaction consisting of multiple keys(fields) with one or more values.
     */
    public void add(Stream<Tuple<Field, List<Object>>> keyValues) {
        scratchTransactionBytesStreamOutput.reset();
        keyValues.forEach(fieldAndValues -> {
            fieldAndValues.v2().stream().sorted().forEach(fieldValue -> {
                try {
                    scratchItemBytesStreamOutput.reset();

                    // step 1: add the single item to the item list
                    scratchItemBytesStreamOutput.writeVInt(fieldAndValues.v1().getId());
                    scratchItemBytesStreamOutput.writeGenericValue(fieldValue);
                    long id = items.add(scratchItemBytesStreamOutput.get());

                    // for existing keys add returns -1 - curId;
                    if (id < 0) {
                        id = -1 * (id + 1);
                    }

                    if (id >= itemCounts.size()) {
                        logger.trace("Resizing array for item counts");
                        itemCounts = bigArrays.resize(itemCounts, itemCounts.size() + CAPACITY_INCREMENT);
                    }
                    // step 2: increment the counter
                    itemCounts.increment(id, 1);
                    ++totalItemCount;
                    scratchTransactionBytesStreamOutput.writeVLong(id);
                    // unreachable: BytesRefStreamOutput does not throw an IOException,
                    // the catch is required because StreamOutput defines it
                } catch (IOException e) {
                    throw new AggregationExecutionException("Failed to add items", e);
                }
            });

        });
        long id = transactions.add(scratchTransactionBytesStreamOutput.get());
        ++totalTransactionCount;
        if (id < 0) {
            id = -1 * (id + 1);
        }

        if (id >= transactionCounts.size()) {
            transactionCounts = bigArrays.resize(transactionCounts, transactionCounts.size() + CAPACITY_INCREMENT);
        }
        transactionCounts.increment(id, 1);
    }

    @Override
    public long getTotalItemCount() {
        return totalItemCount;
    }

    @Override
    public long getTotalTransactionCount() {
        return totalTransactionCount;
    }

    @Override
    public BytesRefArray getItems() {
        return items.getBytesRefs();
    }

    @Override
    public LongArray getItemCounts() {
        return itemCounts;
    }

    @Override
    public BytesRefArray getTransactions() {
        return transactions.getBytesRefs();
    }

    @Override
    public LongArray getTransactionCounts() {
        return transactionCounts;
    }

    /**
     * Merge the other transaction store into this transaction store.
     *
     * @param other the other transaction store
     */
    public void merge(TransactionStore other) throws IOException {
        // merge the item stores
        for (int i = 0; i < other.getItems().size(); ++i) {
            long oldId = i;
            if (oldId >= 0) {
                other.getItems().get(oldId, scratchBytesRef);
                long newId = items.add(scratchBytesRef);
                long oldCount = other.getItemCounts().get(oldId);
                // if item already exists in this.items it is a rewrite
                if (newId < 0) {
                    newId = -1 * (newId + 1);
                } else if (newId >= itemCounts.size()) {
                    itemCounts = bigArrays.resize(itemCounts, itemCounts.size() + CAPACITY_INCREMENT);
                }

                // reuse the old counter structure to remember how to rewrite
                other.getItemCounts().set(oldId, newId);
                itemCounts.increment(newId, oldCount);
            }
        }

        // merge transactions
        for (int i = 0; i < other.getTransactions().size(); ++i) {
            long oldId = i;
            if (oldId >= 0) {
                other.getTransactions().get(oldId, scratchBytesRef);
                scratchByteArrayStreamInput.reset(scratchBytesRef.bytes, scratchBytesRef.offset, scratchBytesRef.length);
                scratchTransactionBytesStreamOutput.reset();

                while (scratchByteArrayStreamInput.available() > 0) {
                    long item = scratchByteArrayStreamInput.readVLong();

                    // rewrite and add to builder
                    scratchTransactionBytesStreamOutput.writeVLong(other.getItemCounts().get(item));
                }
                long newId = transactions.add(scratchTransactionBytesStreamOutput.get());
                if (newId < 0) {
                    newId = -1 * (newId + 1);
                } else if (newId >= transactionCounts.size()) {
                    transactionCounts = bigArrays.resize(transactionCounts, transactionCounts.size() + CAPACITY_INCREMENT);
                }
                long oldCount = other.getTransactionCounts().get(oldId);
                transactionCounts.increment(newId, oldCount);
            }
        }

        totalItemCount += other.getTotalItemCount();
        totalTransactionCount += other.getTotalTransactionCount();
    }

    /**
     * Prune transactions and items according to the given min count
     *
     * Prune rewrites the internal data structures by getting rid of items which are
     * below the given minSupport.
     *
     * Performance: In addition it re-arranges transactions in order to execute a
     * contains operation as linear scan, see [ITEM-BOW].
     *
     * Currently this is only used after merging all shard stores.
     *
     * TODO:
     *
     *  - explore whether we could prune per shard based on a heuristic
     *  - if minimum_set_size is 1, only keep `size` items
     *
     * @param minSupport the minimum support an item must have to be kept
     */
    public void prune(double minSupport) throws IOException {
        long minCount = (long) (minSupport * totalTransactionCount);

        logger.trace("prune items and transactions, using min count: {}", minCount);
        BytesRefHash prunedItems = null;
        LongArray prunedItemCounts = null;
        BytesRefHash prunedTransactions = null;
        LongArray prunedTransactionCounts = null;

        try {
            // start with a smaller array as we expect to cut off a looooong tail
            prunedItems = new BytesRefHash(items.capacity() >> 3, bigArrays);
            prunedItemCounts = bigArrays.newLongArray(items.capacity() >> 3, true);

            // step 1: prune items
            for (int i = 0; i < items.capacity(); ++i) {
                long id = items.id(i);
                if (id >= 0) {
                    items.get(id, scratchBytesRef);
                    long count = itemCounts.get(id);
                    if (count > minCount) {
                        long newId = prunedItems.add(scratchBytesRef);
                        assert newId >= 0 : "found illegal duplicate bytesRef";
                        if (newId >= prunedItemCounts.size()) {
                            prunedItemCounts = bigArrays.resize(prunedItemCounts, prunedItemCounts.size() + CAPACITY_INCREMENT);
                        }
                        prunedItemCounts.set(newId, count);

                        // remember the new id to rewrite the transactions
                        itemCounts.set(id, newId);
                    } else {
                        // magic -1 marks the item as cut
                        itemCounts.set(id, -1);
                    }
                }
            }

            logger.trace("Pruned items, before: {}, after: {}", items.size(), prunedItems.size());

            // step 2 prune transactions
            prunedTransactions = new BytesRefHash(transactions.capacity() >> 3, bigArrays);
            prunedTransactionCounts = bigArrays.newLongArray(transactions.capacity() >> 3, true);
            List<Long> itemBuffer = new ArrayList<>();

            for (int i = 0; i < transactions.capacity(); ++i) {
                long id = transactions.id(i);
                if (id >= 0) {
                    transactions.get(id, scratchBytesRef);
                    scratchByteArrayStreamInput.reset(scratchBytesRef.bytes, scratchBytesRef.offset, scratchBytesRef.length);
                    itemBuffer.clear();

                    while (scratchByteArrayStreamInput.available() > 0) {
                        // note: itemCounts is reused as translation table to map the old item id to the new item id
                        // if the item is mapped to -1, we pruned it in the step above
                        long item = itemCounts.get(scratchByteArrayStreamInput.readVLong());
                        if (item >= 0) {
                            // rewrite and add to buffer
                            itemBuffer.add(item);
                        }
                    }

                    // if we did not add any item a transaction might be empty and can be dropped
                    if (itemBuffer.size() > 0) {
                        // sort the items backwards by item count, that way we can use a linear scan later [ITEM-BOW]
                        Collections.sort(itemBuffer, compareItems(prunedItemCounts));

                        scratchTransactionBytesStreamOutput.reset();
                        for (Long l : itemBuffer) {
                            scratchTransactionBytesStreamOutput.writeVLong(l);
                        }

                        long newId = prunedTransactions.add(scratchTransactionBytesStreamOutput.get());
                        long count = transactionCounts.get(id);

                        // by dropping items previously different transaction can collapse into one
                        if (newId < 0) {
                            newId = -1 * (newId + 1);
                        } else {
                            if (newId >= prunedTransactionCounts.size()) {
                                prunedTransactionCounts = bigArrays.resize(
                                    prunedTransactionCounts,
                                    prunedTransactionCounts.size() + CAPACITY_INCREMENT
                                );
                            }
                        }

                        prunedTransactionCounts.increment(newId, count);
                    }
                }
            }

            if (logger.isTraceEnabled()) {
                logger.trace("Pruned transactions, before: {}, after: {}", transactions.size(), prunedTransactions.size());

                long bytesBeforePruning = items.ramBytesUsed() + itemCounts.ramBytesUsed() + transactions.ramBytesUsed() + transactionCounts
                    .ramBytesUsed();
                long bytesAfterPruning = prunedItems.ramBytesUsed() + prunedItemCounts.ramBytesUsed() + prunedTransactions.ramBytesUsed()
                    + prunedTransactionCounts.ramBytesUsed();

                logger.trace(
                    "Pruned item and transactions, memory reclaimed: {}, size of transaction store after pruning: {}",
                    RamUsageEstimator.humanReadableUnits(bytesBeforePruning - bytesAfterPruning),
                    RamUsageEstimator.humanReadableUnits(bytesAfterPruning)
                );
            }

            items.close();
            itemCounts.close();
            transactions.close();
            transactionCounts.close();

            // swap in the pruned versions
            items = prunedItems;
            prunedItems = null;
            itemCounts = prunedItemCounts;
            prunedItemCounts = null;
            transactions = prunedTransactions;
            prunedTransactions = null;
            transactionCounts = prunedTransactionCounts;
            prunedTransactionCounts = null;
        } finally {
            Releasables.close(prunedItems, prunedItemCounts, prunedTransactions, prunedTransactionCounts);
        }
    }

    public ImmutableTransactionStore createImmutableTransactionStore() {
        ImmutableTransactionStore immutableTransactionStore = new ImmutableTransactionStore(
            bigArrays,
            items.takeBytesRefsOwnership(),
            itemCounts,
            totalItemCount,
            transactions.takeBytesRefsOwnership(),
            transactionCounts,
            totalTransactionCount
        );

        items = null;
        transactions = null;
        itemCounts = null;
        transactionCounts = null;

        return immutableTransactionStore;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        items.getBytesRefs().writeTo(out);
        long itemCountsSize = items.size();
        long transactionCountsSize = transactions.size();
        out.writeVLong(itemCountsSize);
        for (int i = 0; i < itemCountsSize; ++i) {
            out.writeVLong(itemCounts.get(i));
        }
        out.writeVLong(totalItemCount);
        transactions.getBytesRefs().writeTo(out);
        out.writeVLong(transactionCountsSize);
        for (int i = 0; i < transactionCountsSize; ++i) {
            out.writeVLong(transactionCounts.get(i));
        }
        out.writeVLong(totalTransactionCount);
    }

    @Override
    public void close() {
        Releasables.close(items, itemCounts, transactions, transactionCounts);
        items = null;
        itemCounts = null;
        transactions = null;
        transactionCounts = null;
    }

    @Override
    public long ramBytesUsed() {
        return super.ramBytesUsed() + BASE_RAM_BYTES_USED + scratchItemBytesStreamOutput.ramBytesUsed()
            + scratchTransactionBytesStreamOutput.ramBytesUsed() + scratchBytesRef.length + scratchByteArrayStreamInput.length() + items
                .ramBytesUsed() + itemCounts.ramBytesUsed() + transactions.ramBytesUsed() + transactionCounts.ramBytesUsed();
    }

}
