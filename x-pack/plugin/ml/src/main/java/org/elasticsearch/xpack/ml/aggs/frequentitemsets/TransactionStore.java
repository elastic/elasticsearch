/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
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
 *
 * ## Top Items/Transactions
 *
 * - top_{items, transactions} creates lists of {item_, transaction_}ids reverse sorted by count
 *
 */
public final class TransactionStore implements Writeable, Releasable {

    /**
     * Container for holding item ids reverse sorted by count
     *
     * Only stores a list of ids, not the count
     */
    public static class TopItemIds implements Iterable<Long>, Releasable {

        public interface IdIterator extends Iterator<Long> {

            int getIndex();

            /**
             * Reset the Iterator
             */
            void reset();

            void reset(int startIndex);

        }

        private final LongArray sortedItems;

        TopItemIds(LongArray sortedItems) {
            this.sortedItems = sortedItems;
        }

        @Override
        public IdIterator iterator() {
            return iterator(0);
        }

        // TODO: would be good to avoid setting internals
        public IdIterator iterator(int startIndex) {
            IdIterator it = new IdIterator() {

                private int currentIndex = startIndex;

                @Override
                public boolean hasNext() {
                    return currentIndex < sortedItems.size();
                }

                @Override
                public Long next() {
                    return sortedItems.get(currentIndex++);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int getIndex() {
                    return currentIndex;
                }

                @Override
                public void reset() {
                    currentIndex = 0;
                }

                @Override
                public void reset(int startIndex) {
                    currentIndex = startIndex;
                }
            };
            return it;
        }

        public long size() {
            return sortedItems.size();
        }

        @Override
        public void close() {
            Releasables.close(sortedItems);
        }
    }

    /**
     * Container for holding transaction ids reverse sorted by count
     *
     * Only stores a list of ids, not the count
     */
    public static class TopTransactionIds implements Iterable<Long>, Releasable {
        private final LongArray sortedTransactions;

        TopTransactionIds(LongArray sortedTransactions) {
            this.sortedTransactions = sortedTransactions;
        }

        @Override
        public Iterator<Long> iterator() {
            Iterator<Long> it = new Iterator<Long>() {

                private int currentIndex = 0;

                @Override
                public boolean hasNext() {
                    return currentIndex < sortedTransactions.size();
                }

                @Override
                public Long next() {
                    return sortedTransactions.get(currentIndex++);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
            return it;
        }

        public long size() {
            return sortedTransactions.size();
        }

        @Override
        public void close() {
            Releasables.close(sortedTransactions);
        }
    }

    private static final Logger logger = LogManager.getLogger(TransactionStore.class);

    private static final int INITIAL_ITEM_CAPACITY = PageCacheRecycler.LONG_PAGE_SIZE;
    private static final int INITIAL_TRANSACTION_CAPACITY = PageCacheRecycler.LONG_PAGE_SIZE;
    private static final int CAPACITY_INCREMENT = PageCacheRecycler.LONG_PAGE_SIZE;

    // TODO: remove?? how many unique items to consider before pruning
    private static final long UNIQUE_ITEMS_PRUNE_BOUNDARY = 30;

    private final BigArrays bigArrays;

    // data holders
    private BytesRefHash items;
    private LongArray itemCounts;
    private long totalItemCount;
    private BytesRefHash transactions;
    private LongArray transactionCounts;
    private long totalTransactionCount;

    // re-use bytes ref object
    private BytesRefBuilder scratchItemBytesRefBuilder = new BytesRefBuilder();
    private BytesRefBuilder scratchTransactionBytesRefBuilder = new BytesRefBuilder();
    private BytesRef scratchBytesRef = new BytesRef();
    private ByteArrayDataInput scratchBytesRefDataInput = new ByteArrayDataInput();

    public TransactionStore(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
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

    public TransactionStore(StreamInput in, BigArrays bigArrays) throws IOException {
        this.bigArrays = bigArrays;

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
    public void add(Stream<Tuple<String, List<Object>>> keyValues) {

        // TODO: avoid creating new bytesref for fieldnames??
        scratchTransactionBytesRefBuilder.clear();

        keyValues.forEach(fieldName -> {
            fieldName.v2().stream().sorted().forEach(fieldValue -> {
                scratchItemBytesRefBuilder.clear();

                // step 1: add the single item to the item list
                writeString(scratchItemBytesRefBuilder, fieldName.v1());
                // TODO: change fieldValue into a ByteRef
                // itemBytesRefBuilder.append(new BytesRef(fieldValue.toString()));
                // TODO: don't turn numbers into strings
                writeString(scratchItemBytesRefBuilder, fieldValue.toString());

                long id = items.add(scratchItemBytesRefBuilder.get());
                // logger.info("added item with id {}", id);

                // for existing keys add returns -1 - curId;
                if (id < 0) {
                    id = -1 * (id + 1);
                }

                if (id >= itemCounts.size()) {
                    logger.info("Resizing itemCounts");
                    itemCounts = bigArrays.resize(itemCounts, itemCounts.size() + CAPACITY_INCREMENT);
                }

                // step 2: increment the counter
                itemCounts.increment(id, 1);
                ++totalItemCount;

                writeSignedVLong(scratchTransactionBytesRefBuilder, id);
            });

        });
        long id = transactions.add(scratchTransactionBytesRefBuilder.get());
        ++totalTransactionCount;
        if (id < 0) {
            id = -1 * (id + 1);
        }

        if (id >= transactionCounts.size()) {
            transactionCounts = bigArrays.resize(transactionCounts, transactionCounts.size() + CAPACITY_INCREMENT);
        }
        transactionCounts.increment(id, 1);
    }

    /*
     * Get an item as key value pair.
     *
     * @param id the item id
     * @return the item as key value pair
     * TODO: currently field values are hardcoded to strings
     */
    public Tuple<String, String> getItem(long id) throws IOException {
        items.get(id, scratchBytesRef);
        scratchBytesRefDataInput.reset(scratchBytesRef.bytes, scratchBytesRef.offset, scratchBytesRef.length);
        return new Tuple<>(scratchBytesRefDataInput.readString(), scratchBytesRefDataInput.readString());
    }

    /*
     * Get the count of an item.
     *
     * @param id the item id
     * @return the count of this item
     */
    public long getItemCount(long id) {
        return itemCounts.get(id);
    }

    /*
     * Get the count of an transaction.
     *
     * @param id the transaction id
     * @return the count of this transaction
     */
    public long getTransactionCount(long id) {
        return transactionCounts.get(id);
    }

    /**
     * Check if a transaction specified by id contains the item
     *
     * @param ids a list of item ids
     * @param transactionId the transaction id to check
     * @return true if all ids are part of this transaction or not
     */
    public boolean transactionContainAllIds(List<Long> ids, long transactionId) {
        transactions.get(transactionId, scratchBytesRef);
        scratchBytesRefDataInput.reset(scratchBytesRef.bytes, scratchBytesRef.offset, scratchBytesRef.length);

        int pos = 0;
        while (scratchBytesRefDataInput.eof() == false) {
            long item = scratchBytesRefDataInput.readVLong();
            if (item == ids.get(pos)) {
                pos++;
                if (ids.size() == pos) {
                    return true;
                }
            }

            // transactions are sorted by id, a match is not possible if the id in the transaction is already above
            // if (item > ids.get(pos)) {
            // return false;
            // }
        }

        return false;
    }

    /**
     * Get the number of unique items
     *
     * @return count of unique items
     */
    public long getUniqueItemsCount() {
        return items.size();
    }

    /**
     * Get the total number of items
     *
     * @return total count of items
     */
    public long getTotalItemCount() {
        return totalItemCount;
    }

    public long getTotalTransactionCount() {
        return totalTransactionCount;
    }

    /**
     * Destructively merges the other transaction store into this transaction store.
     *
     * @param other
     */
    public void mergeAndClose(TransactionStore other) {
        if (items.size() == 0) {
            this.items.close();
            this.items = other.items;
            this.itemCounts.close();
            this.itemCounts = other.itemCounts;
            this.transactions.close();
            this.transactions = other.transactions;
            this.transactionCounts.close();
            this.transactionCounts = other.transactionCounts;
            this.totalItemCount = other.totalItemCount;
            this.totalTransactionCount = other.totalTransactionCount;
            other.close();
            return;
        }

        // merge the item stores
        for (int i = 0; i < other.items.capacity(); ++i) {
            long oldId = other.items.id(i);
            if (oldId >= 0) {
                other.items.get(oldId, scratchBytesRef);
                long newId = items.add(scratchBytesRef);
                long oldCount = other.itemCounts.get(oldId);
                // if item already exists in this.items it is a rewrite
                if (newId < 0) {
                    newId = -1 * (newId + 1);
                } else if (newId >= itemCounts.size()) {
                    logger.info("Resizing itemCounts");
                    itemCounts = bigArrays.resize(itemCounts, itemCounts.size() + CAPACITY_INCREMENT);
                }

                // reuse the old counter structure to remember how to rewrite
                other.itemCounts.set(oldId, newId);
                itemCounts.increment(newId, oldCount);
            }
        }

        // early closing, so memory can be freed before we allocate new memory below
        other.items.close();

        // merge transactions
        for (int i = 0; i < other.transactions.capacity(); ++i) {
            long oldId = other.transactions.id(i);
            if (oldId >= 0) {
                other.transactions.get(oldId, scratchBytesRef);
                scratchBytesRefDataInput.reset(scratchBytesRef.bytes, scratchBytesRef.offset, scratchBytesRef.length);
                scratchTransactionBytesRefBuilder.clear();

                while (scratchBytesRefDataInput.eof() == false) {
                    long item = scratchBytesRefDataInput.readVLong();

                    // rewrite and add to builder
                    writeSignedVLong(scratchTransactionBytesRefBuilder, other.itemCounts.get(item));
                }
                long newId = transactions.add(scratchTransactionBytesRefBuilder.get());
                // logger.info("added transaction with id {}", id);
                if (newId < 0) {
                    newId = -1 * (newId + 1);
                } else if (newId >= transactionCounts.size()) {
                    transactionCounts = bigArrays.resize(transactionCounts, transactionCounts.size() + CAPACITY_INCREMENT);
                }
                long oldCount = other.transactionCounts.get(oldId);
                transactionCounts.increment(newId, oldCount);
            }
        }

        totalItemCount += other.totalItemCount;
        totalTransactionCount += other.totalTransactionCount;
        other.close();
    }

    // TODO: this prunes _AND_ rearranges transactions for faster lookup
    // that's why shortcuts are disabled for now!
    public void prune(double minSupport) {

        // skip pruning if the number of unique items is low
        /*if (getUniqueItemsCount() < UNIQUE_ITEMS_PRUNE_BOUNDARY) {
            return;
        }*/

        long minCount = (long) (minSupport * totalTransactionCount);

        logger.info("prune items and transactions, using min count: {}", minCount);

        // even a minCount of 1 is considered to cut a lot of items
        // TODO: collect statistics during data collection to make a smarter decision
        /*if (minCount < 2) {
            return;
        }*/

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
                            logger.info("Resizing prunedItemCounts");
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

            // TODO: change to debug
            logger.info("Pruned items, before: {}, after: {}", items.size(), prunedItems.size());

            // step2 prune transactions, TODO: can we use a better factor after item pruning?
            prunedTransactions = new BytesRefHash(transactions.capacity() >> 3, bigArrays);
            prunedTransactionCounts = bigArrays.newLongArray(transactions.capacity() >> 3, true);
            List<Long> itemBuffer = new ArrayList<>();

            for (int i = 0; i < transactions.capacity(); ++i) {
                long id = transactions.id(i);
                if (id >= 0) {
                    transactions.get(id, scratchBytesRef);
                    scratchBytesRefDataInput.reset(scratchBytesRef.bytes, scratchBytesRef.offset, scratchBytesRef.length);
                    itemBuffer.clear();

                    while (scratchBytesRefDataInput.eof() == false) {
                        // note: itemCounts is reused as translation table to map the old item id to the new item id
                        // if the item is mapped to -1, we pruned it in the step above
                        long item = itemCounts.get(scratchBytesRefDataInput.readVLong());
                        if (item >= 0) {
                            // rewrite and add to buffer
                            itemBuffer.add(item);
                        }
                    }

                    // if we did not add any item a transaction might be empty and can be dropped
                    if (itemBuffer.size() > 0) {
                        // sort the items backwards by id count
                        Collections.sort(itemBuffer, Comparator.comparingLong(prunedItemCounts::get).reversed());

                        /*logger.info(
                            "sorted transaction {}",
                            Strings.collectionToDelimitedString(itemBuffer, ",")
                        );*/

                        scratchTransactionBytesRefBuilder.clear();
                        itemBuffer.forEach(l -> writeSignedVLong(scratchTransactionBytesRefBuilder, l));

                        long newId = prunedTransactions.add(scratchTransactionBytesRefBuilder.get());
                        long count = transactionCounts.get(id);

                        // by dropping items previously different transaction can collapse into one
                        if (newId < 0) {
                            newId = -1 * (newId + 1);
                        } else {
                            if (newId >= prunedTransactionCounts.size()) {
                                logger.info("Resizing prunedTransactionCounts");
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

            logger.info("Pruned transactions, before: {}, after: {}", transactions.size(), prunedTransactions.size());

            // TODO: change to debug
            if (logger.isInfoEnabled()) {
                long bytesBeforePruning = items.ramBytesUsed() + itemCounts.ramBytesUsed() + transactions.ramBytesUsed() + transactionCounts
                    .ramBytesUsed();
                long bytesAfterPruning = prunedItems.ramBytesUsed() + prunedItemCounts.ramBytesUsed() + prunedTransactions.ramBytesUsed()
                    + prunedTransactionCounts.ramBytesUsed();

                logger.info(
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
        } catch (Exception e) {
            // NORELEASE get the stack trace in the logs
            logger.error("exception during pruning", e);
        } finally {
            Releasables.close(prunedItems, prunedItemCounts, prunedTransactions, prunedTransactionCounts);
        }
    }

    /**
     * Returns a sorted collection of item ids
     *
     * @return TopItemIds object
     */
    public TopItemIds getTopItemIds() {
        return getTopItemIds(items.capacity());
    }

    /**
     * Returns a sorted collection of item ids
     *
     * @param n top-n items to return
     * @return TopItemIds object
     */
    public TopItemIds getTopItemIds(long n) {
        // TODO: heap based and wasteful, this should instead use some lucene magic
        List<Tuple<Long, Long>> idsHelperBuffer = new ArrayList<>();
        for (long i = 0; i < itemCounts.size(); ++i) {
            long count = itemCounts.get(i);
            if (count > 0) {
                idsHelperBuffer.add(new Tuple<Long, Long>(i, count));
            }
        }
        idsHelperBuffer.sort((e1, e2) -> e2.v2().compareTo(e1.v2()));

        long topN = Math.min(n, idsHelperBuffer.size());
        LongArray sortedIds = bigArrays.newLongArray(topN);
        for (long i = 0; i < topN; ++i) {
            sortedIds.set(i, idsHelperBuffer.get((int) i).v1());
        }

        return new TopItemIds(sortedIds);
    }

    /**
     * Returns a sorted collection of transaction ids
     *
     * @return TopItemIds object
     */
    public TopTransactionIds getTopTransactionIds() {
        return getTopTransactionIds(transactions.capacity());
    }

    /**
     * Returns a sorted collection of transaction ids
     *
     * @param n top-n transactions to return
     * @return TopTransactionIds object
     */
    public TopTransactionIds getTopTransactionIds(long n) {
        // TODO: heap based and wasteful, this should instead use some lucene magic
        List<Tuple<Long, Long>> idsHelperBuffer = new ArrayList<>();
        for (long i = 0; i < transactionCounts.size(); ++i) {
            long count = transactionCounts.get(i);
            if (count > 0) {
                idsHelperBuffer.add(new Tuple<Long, Long>(i, count));
            }
        }
        idsHelperBuffer.sort((e1, e2) -> e2.v2().compareTo(e1.v2()));

        long topN = Math.min(n, idsHelperBuffer.size());
        LongArray sortedIds = bigArrays.newLongArray(topN);
        for (long i = 0; i < topN; ++i) {
            sortedIds.set(i, idsHelperBuffer.get((int) i).v1());
        }

        return new TopTransactionIds(sortedIds);
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

    private static void writeString(BytesRefBuilder builder, String s) {
        final BytesRef utf8Result = new BytesRef(s);
        writeVInt(builder, utf8Result.length);
        builder.append(utf8Result.bytes, utf8Result.offset, utf8Result.length);
    }

    /**
     * helper method to write signed vlongs, inspired by lucene dataoutput
     *
     * TODO: candidate for re-factoring
     */
    private static void writeSignedVLong(BytesRefBuilder builder, long i) {
        while ((i & ~0x7FL) != 0L) {
            builder.append((byte) ((i & 0x7FL) | 0x80L));
            i >>>= 7;
        }
        builder.append((byte) i);
    }

    private static void writeVInt(BytesRefBuilder builder, int i) {
        while ((i & ~0x7F) != 0) {
            builder.append((byte) ((i & 0x7F) | 0x80));
            i >>>= 7;
        }
        builder.append((byte) i);
    }

}
