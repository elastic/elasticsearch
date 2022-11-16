/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.TransactionStore.TopItemIds;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;

/**
 * Item set traverser to find the next interesting item set.
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
 */
final class CountingItemSetTraverser implements Releasable {
    private static final Logger logger = LogManager.getLogger(CountingItemSetTraverser.class);

    // start size and size increment for the occurences stack
    private static final int OCCURENCES_SIZE_INCREMENT = 10;

    private final TransactionStore transactionStore;
    private final ItemSetTraverser topItemSetTraverser;
    private final TransactionStore.TopTransactionIds topTransactionIds;

    private final TransactionsLookupTable transactionsLookupTable;
    private final int cacheTraversalDepth;
    private final int cacheNumberOfTransactions;

    // implementation of a cache that remembers which transactions are still of interest up to depth and number of top transactions
    private final long[] transactionSkipCounts;
    private final BitSet transactionSkipList;

    private long[] occurencesStack;
    // growable bit set from java util
    private BitSet visited;

    CountingItemSetTraverser(
        TransactionStore transactionStore,
        TopItemIds topItemIds,
        int cacheTraversalDepth,
        int cacheNumberOfTransactions,
        long minCount
    ) throws IOException {
        this.transactionStore = transactionStore;

        boolean success = false;
        try {
            // we allocate 2 big arrays, if the 2nd allocation fails, ensure we clean up
            this.topItemSetTraverser = new ItemSetTraverser(topItemIds);
            this.topTransactionIds = transactionStore.getTopTransactionIds();
            this.transactionsLookupTable = transactionStore.createLookupTableByTopTransactions(topItemIds, topTransactionIds);
            success = true;
        } finally {
            if (false == success) {
                close();
            }
        }

        this.cacheTraversalDepth = cacheTraversalDepth;
        this.cacheNumberOfTransactions = cacheNumberOfTransactions;
        transactionSkipCounts = new long[cacheTraversalDepth - 1];
        transactionSkipList = new BitSet((cacheTraversalDepth - 1) * cacheNumberOfTransactions);
        occurencesStack = new long[OCCURENCES_SIZE_INCREMENT];
        visited = new java.util.BitSet();
    }

    public boolean next(long earlyStopMinCount) throws IOException {

        if (topItemSetTraverser.next() == false) {
            return false;
        }

        final long totalTransactionCount = transactionStore.getTotalTransactionCount();

        int depth = topItemSetTraverser.getNumberOfItems();
        long occurencesOfSingleItem = transactionStore.getItemCount(topItemSetTraverser.getItemId());

        if (depth == 1) {
            // at the 1st level, we can take the count directly from the transaction store
            occurencesStack[0] = occurencesOfSingleItem;
            return true;
        } else if (occurencesOfSingleItem < earlyStopMinCount) {
            rememberCountInStack(depth, occurencesOfSingleItem);
            return true;
            // till a certain depth store results in a cache matrix
        } else if (depth < cacheTraversalDepth) {
            // get the cached skip count
            long skipCount = transactionSkipCounts[depth - 2];

            // use the countdown from a previous iteration
            long maxReachableTransactionCount = totalTransactionCount - skipCount;

            // we recalculate the row for this depth, so we have to clear the bits first
            transactionSkipList.clear((depth - 1) * cacheNumberOfTransactions, ((depth) * cacheNumberOfTransactions));

            int topTransactionPos = 0;
            long occurrences = 0;

            // for whatever reason this turns out to be faster than a for loop
            while (topTransactionPos < topTransactionIds.size()) {
                // caching: if the transaction is already marked for skipping, quickly continue
                if (topTransactionPos < cacheNumberOfTransactions
                    && transactionSkipList.get(cacheNumberOfTransactions * (depth - 2) + topTransactionPos)) {
                    // set the bit for the next iteration
                    transactionSkipList.set(cacheNumberOfTransactions * (depth - 1) + topTransactionPos);
                    topTransactionPos++;
                    continue;
                }

                long transactionCount = transactionStore.getTransactionCount(topTransactionIds.getItemIdAt(topTransactionPos));

                if (transactionsLookupTable.isSubsetOf(topTransactionPos, topItemSetTraverser.getItemSetBitSet())) {
                    occurrences += transactionCount;
                } else if (topTransactionPos < cacheNumberOfTransactions) {
                    // put this transaction to the skip list
                    skipCount += transactionCount;
                    transactionSkipList.set(cacheNumberOfTransactions * (depth - 1) + topTransactionPos);
                }

                maxReachableTransactionCount -= transactionCount;
                // exit early if min support given for early stop can't be reached
                if (maxReachableTransactionCount + occurrences < earlyStopMinCount) {
                    break;
                }

                topTransactionPos++;
            }
            transactionSkipCounts[depth - 1] = skipCount;

            rememberCountInStack(depth, occurrences);
            return true;
        }

        // else: deep traversal (we only cache until a certain depth, while depth == number of items in a transaction)
        // use the last cached values, but don't store any
        // this is exactly the same (unrolled) code as before, but without writing to the cache

        // get the last cached skip count
        long skipCount = transactionSkipCounts[cacheTraversalDepth - 2];

        // use the countdown from a previous iteration
        long maxReachableTransactionCount = totalTransactionCount - skipCount;

        int transactionNumber = 0;
        long occurrences = 0;
        for (Long transactionId : topTransactionIds) {
            // caching: if the transaction is already marked for skipping, quickly continue
            if (transactionNumber < cacheNumberOfTransactions
                && transactionSkipList.get(cacheNumberOfTransactions * (cacheTraversalDepth - 2) + transactionNumber)) {
                transactionNumber++;

                continue;
            }
            long transactionCount = transactionStore.getTransactionCount(transactionId);

            if (transactionsLookupTable.isSubsetOf(transactionNumber, topItemSetTraverser.getItemSetBitSet())) {
                occurrences += transactionCount;
            }

            maxReachableTransactionCount -= transactionCount;

            // exit early if min support given for early stop can't be reached
            if (maxReachableTransactionCount + occurrences < earlyStopMinCount) {
                break;
            }

            transactionNumber++;
        }

        rememberCountInStack(depth, occurrences);
        return true;
    }

    /**
     * Get the count of the current item set
     */
    public long getCount() {
        if (topItemSetTraverser.getNumberOfItems() > 0) {
            return occurencesStack[topItemSetTraverser.getNumberOfItems() - 1];
        }
        return 0;
    }

    /**
     * Get the count of the item set without the last item
     */
    public long getParentCount() {
        if (topItemSetTraverser.getNumberOfItems() > 1) {
            return occurencesStack[topItemSetTraverser.getNumberOfItems() - 2];
        }
        return 0;
    }

    public boolean hasBeenVisited() {
        if (topItemSetTraverser.getNumberOfItems() > 0) {
            return visited.get(topItemSetTraverser.getNumberOfItems() - 1);
        }
        return true;
    }

    public boolean hasParentBeenVisited() {
        if (topItemSetTraverser.getNumberOfItems() > 1) {
            return visited.get(topItemSetTraverser.getNumberOfItems() - 2);
        }
        return true;
    }

    public void setVisited() {
        if (topItemSetTraverser.getNumberOfItems() > 0) {
            visited.set(topItemSetTraverser.getNumberOfItems() - 1);
        }
    }

    public void setParentVisited() {
        if (topItemSetTraverser.getNumberOfItems() > 1) {
            visited.set(topItemSetTraverser.getNumberOfItems() - 2);
        }
    }

    /**
     * Get the number of items in the current set
     */
    public int getNumberOfItems() {
        return topItemSetTraverser.getNumberOfItems();
    }

    /**
     *
     * Get a bitset representation of the current item set
     */
    public ItemSetBitSet getItemSetBitSet() {
        return topItemSetTraverser.getItemSetBitSet();
    }

    public ItemSetBitSet getParentItemSetBitSet() {
        return topItemSetTraverser.getParentItemSetBitSet();
    }

    /**
     * Prune the traversal. This stops exploring the current branch
     */
    public void prune() {
        topItemSetTraverser.prune();
    }

    /**
     * Return true if the item set tree is on a leaf, which mean no further items can be added to the candidate set.
     */
    public boolean atLeaf() {
        return topItemSetTraverser.atLeaf();
    }

    @Override
    public void close() {
        Releasables.close(topTransactionIds, transactionsLookupTable);
    }

    // remember the count in the stack without tracking push and pop
    private void rememberCountInStack(int index, long occurences) {
        if (occurencesStack.length < index) {
            occurencesStack = Arrays.copyOf(occurencesStack, index + OCCURENCES_SIZE_INCREMENT);
        }

        occurencesStack[index - 1] = occurences;
        visited.clear(index - 1);
    }
}
