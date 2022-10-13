/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

abstract class TransactionStore implements Writeable, Releasable, Accountable {

    /**
     * Container for holding item ids reverse sorted by count
     *
     * Only stores a list of ids, not the count
     */
    static class TopItemIds implements Iterable<Long>, Releasable {

        class IdIterator implements Iterator<Long> {

            private int currentIndex;

            IdIterator(int startIndex) {
                currentIndex = startIndex;
            }

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

            public int getIndex() {
                return currentIndex;
            }

            public void reset() {
                currentIndex = 0;
            }

            public void reset(int startIndex) {
                currentIndex = startIndex;
            }
        }

        private final LongArray sortedItems;

        private TopItemIds(LongArray sortedItems) {
            this.sortedItems = sortedItems;
        }

        @Override
        public IdIterator iterator() {
            return iterator(0);
        }

        public IdIterator iterator(int startIndex) {
            return new IdIterator(startIndex);
        }

        public long getItemIdAt(long index) {
            return sortedItems.get(index);
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
    static class TopTransactionIds implements Iterable<Long>, Releasable {
        private final LongArray sortedTransactions;

        private TopTransactionIds(LongArray sortedTransactions) {
            this.sortedTransactions = sortedTransactions;
        }

        @Override
        public Iterator<Long> iterator() {
            return new Iterator<Long>() {
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
        }

        public long size() {
            return sortedTransactions.size();
        }

        public long getItemIdAt(long index) {
            return sortedTransactions.get(index);
        }

        @Override
        public void close() {
            Releasables.close(sortedTransactions);
        }
    }

    /*
     * Comparator for comparing items by count, the 1st value is the item id, the 2nd the count
     */
    static final Comparator<Tuple<Long, Long>> ITEMS_BY_COUNT_COMPARATOR = new Comparator<Tuple<Long, Long>>() {
        @Override
        public int compare(Tuple<Long, Long> o1, Tuple<Long, Long> o2) {

            // if counts are equal take the smaller item id first
            if (o1.v2() == o2.v2()) {
                return o1.v1().compareTo(o2.v1());
            }

            return o2.v2().compareTo(o1.v2());
        }
    };

    /**
     * variant of ITEMS_BY_COUNT_COMPARATOR that reads counts from the given array
     */
    static Comparator<Long> compareItems(final LongArray counts) {
        return (Comparator<Long>) (e1, e2) -> {
            long count1 = counts.get(e1);
            long count2 = counts.get(e2);

            if (count1 == count2) {
                return Long.compare(e1, e2);
            }
            return Long.compare(count2, count1);
        };
    }

    // as this is an abstract class, don't account for the shallow size of this class
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) + RamUsageEstimator
        .shallowSizeOfInstance(ByteArrayStreamInput.class);

    protected final BigArrays bigArrays;

    // re-usable objects
    protected final BytesRef scratchBytesRef = new BytesRef();
    protected final ByteArrayStreamInput scratchByteArrayStreamInput = new ByteArrayStreamInput();

    TransactionStore(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
    }

    /**
     * Get the total number of items
     *
     * @return total count of items
     */
    abstract long getTotalItemCount();

    /**
     * Get the total number of transactions
     *
     * @return total count of transactions
     */
    abstract long getTotalTransactionCount();

    abstract BytesRefArray getItems();

    abstract LongArray getItemCounts();

    abstract BytesRefArray getTransactions();

    abstract LongArray getTransactionCounts();

    /*
     * Get an item decoded as key value pair.
     *
     * @param id the item id
     * @return the item as key value pair
     */
    public Tuple<Integer, Object> getItem(long id) throws IOException {
        getItems().get(id, scratchBytesRef);
        scratchByteArrayStreamInput.reset(scratchBytesRef.bytes, scratchBytesRef.offset, scratchBytesRef.length);
        return new Tuple<>(scratchByteArrayStreamInput.readVInt(), scratchByteArrayStreamInput.readGenericValue());
    }

    /**
     * Get the number of unique transactions.
     *
     * @return count of unique transactions
     */
    public long getUniqueTransactionCount() {
        return getTransactions().size();
    }

    /*
     * Get an item
     */
    public void getItem(long id, BytesRef dest) {
        getItems().get(id, dest);
    }

    /*
     * Get a transaction
     */
    public void getTransaction(long id, BytesRef dest) {
        getTransactions().get(id, dest);
    }

    /*
     * Get the count of an item.
     *
     * @param id the item id
     * @return the count of this item
     */
    public long getItemCount(long id) {
        return getItemCounts().get(id);
    }

    /*
     * Get the count of an transaction.
     *
     * @param id the transaction id
     * @return the count of this transaction
     */
    public long getTransactionCount(long id) {
        return getTransactionCounts().get(id);
    }

    /**
     * Get the number of unique items
     *
     * @return count of unique items
     */
    public long getUniqueItemsCount() {
        return getItems().size();
    }

    /**
     * Returns a sorted collection of item ids
     *
     * @param n top-n items to return
     * @return TopItemIds object
     */
    public TopItemIds getTopItemIds(long n) {
        // TODO: heap based and wasteful, this should instead use some lucene magic if possible
        List<Tuple<Long, Long>> idsHelperBuffer = new ArrayList<>();
        final LongArray itemCounts = getItemCounts();

        for (long i = 0; i < itemCounts.size(); ++i) {
            long count = itemCounts.get(i);
            if (count > 0) {
                idsHelperBuffer.add(Tuple.tuple(i, count));
            }
        }
        // [ITEM-BOW] sort items by count
        idsHelperBuffer.sort(ITEMS_BY_COUNT_COMPARATOR);

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
        return getTopTransactionIds(getTransactions().size());
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
        final LongArray transactionCounts = getTransactionCounts();

        for (long i = 0; i < transactionCounts.size(); ++i) {
            long count = transactionCounts.get(i);
            if (count > 0) {
                idsHelperBuffer.add(Tuple.tuple(i, count));
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

    /**
     * Returns a sorted collection of item ids
     *
     * @return TopItemIds object
     */
    public TopItemIds getTopItemIds() {
        return getTopItemIds(getItems().size());
    }

    /**
     * Create a lookup table (bit matrix) containing a so-called "horizontal" representation of transactions to item ids.
     *
     * A bit is set according to the position in topItems, if a transaction contains an item the bit is set.
     * The lookup table rows correspond to the order in top transactions.
     *
     * @param topItems the top items
     * @param topTransactions the top transactions
     * @return a transaction lookup table
     * @throws IOException
     */
    public TransactionsLookupTable createLookupTableByTopTransactions(TopItemIds topItems, TopTransactionIds topTransactions)
        throws IOException {
        try (IntArray positions = bigArrays.newIntArray(topItems.size())) {

            // helper lookup table that maps an item id to the position in the top items vector
            for (int i = 0; i < topItems.size(); ++i) {
                positions.set(topItems.getItemIdAt(i), i);
            }

            BytesRefArray transactions = getTransactions();
            TransactionsLookupTable lookupTable = new TransactionsLookupTable(transactions.size(), bigArrays);
            ItemSetBitSet bitSet = new ItemSetBitSet();

            for (Long id : topTransactions) {
                bitSet.clear();
                transactions.get(id, scratchBytesRef);
                scratchByteArrayStreamInput.reset(scratchBytesRef.bytes, scratchBytesRef.offset, scratchBytesRef.length);

                while (scratchByteArrayStreamInput.available() > 0) {
                    // flip the bit according to the position in top items
                    bitSet.set(1 + positions.get(scratchByteArrayStreamInput.readVLong()));
                }
                lookupTable.append(bitSet);
            }
            return lookupTable;
        }
    }

    /**
     * Check if a transaction specified by id contains the item
     *
     * @param ids a list of item ids
     * @param transactionId the transaction id to check
     * @return true if all ids are part of this transaction or not
     */
    public boolean transactionContainsAllIds(LongsRef ids, long transactionId) throws IOException {

        getTransactions().get(transactionId, scratchBytesRef);
        scratchByteArrayStreamInput.reset(scratchBytesRef.bytes, scratchBytesRef.offset, scratchBytesRef.length);

        int itemsLeft = ids.length;
        while (scratchByteArrayStreamInput.length() - scratchByteArrayStreamInput.getPosition() >= itemsLeft) {
            long item = scratchByteArrayStreamInput.readVLong();

            // we can do a linear scan, because we sorted the ids in the transaction in prune, see [ITEM-BOW]
            if (item == ids.longs[ids.length - itemsLeft]) {
                --itemsLeft;
                if (itemsLeft == 0) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + scratchBytesRef.length + scratchByteArrayStreamInput.length();
    }
}
