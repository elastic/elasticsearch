/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongsRef;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.Arrays;

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
class CountingItemSetTraverser implements Releasable {

    // start size and size increment for the occurences stack
    private static final int OCCURENCES_SIZE_INCREMENT = 10;

    private final TransactionStore transactionStore;
    private final ItemSetTraverser topItemSetTraverser;
    private final TransactionStore.TopTransactionIds topTransactionIds;
    private final int cacheTraversalDepth;
    private final int cacheNumberOfTransactions;

    // implementation of a cache that remembers which transactions are still of interest up to depth and number of top transactions
    private final long[] transactionSkipCounts;
    private final BitSet transactionSkipList;

    private long[] occurencesStack;
    // growable bit set from java util
    private java.util.BitSet visited;

    CountingItemSetTraverser(TransactionStore transactionStore, int cacheTraversalDepth, int cacheNumberOfTransactions, long minCount) {
        this.transactionStore = transactionStore;

        boolean success = false;
        try {
            // we allocate 2 big arrays, if the 2nd allocation fails, ensure we clean up
            this.topItemSetTraverser = transactionStore.getTopItemIdTraverser();
            this.topTransactionIds = transactionStore.getTopTransactionIds();
            success = true;
        } finally {
            if (false == success) {
                close();
            }
        }

        this.cacheTraversalDepth = cacheTraversalDepth;
        this.cacheNumberOfTransactions = cacheNumberOfTransactions;
        transactionSkipCounts = new long[cacheTraversalDepth - 1];
        transactionSkipList = new FixedBitSet((cacheTraversalDepth - 1) * cacheNumberOfTransactions);
        occurencesStack = new long[OCCURENCES_SIZE_INCREMENT];
        visited = new java.util.BitSet();
    }

    public boolean next(long earlyStopMinCount) throws IOException {

        if (topItemSetTraverser.next() == false) {
            return false;
        }

        final long totalTransactionCount = transactionStore.getTotalTransactionCount();

        int depth = topItemSetTraverser.getNumberOfItems();
        if (depth == 1) {
            // at the 1st level, we can take the count directly from the transaction store
            occurencesStack[0] = transactionStore.getItemCount(topItemSetTraverser.getItemId());
            return true;

            // till a certain depth store results in a cache matrix
        } else if (depth < cacheTraversalDepth) {
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

                if (transactionStore.transactionContainsAllIds(topItemSetTraverser.getItemSet(), transactionId)) {
                    occurences += transactionCount;
                } else if (transactionNumber < cacheNumberOfTransactions) {
                    // put this transaction to the skip list
                    skipCount += transactionCount;
                    transactionSkipList.set(cacheNumberOfTransactions * (depth - 1) + transactionNumber);
                }

                maxReachableTransactionCount -= transactionCount;
                // exit early if min support given for early stop can't be reached
                if (maxReachableTransactionCount + occurences < earlyStopMinCount) {
                    break;
                }

                transactionNumber++;
            }

            transactionSkipCounts[depth - 1] = skipCount;

            rememberCountInStack(depth, occurences);
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
        long occurences = 0;
        for (Long transactionId : topTransactionIds) {
            // caching: if the transaction is already marked for skipping, quickly continue
            if (transactionNumber < cacheNumberOfTransactions
                && transactionSkipList.get(cacheNumberOfTransactions * (cacheTraversalDepth - 2) + transactionNumber)) {
                transactionNumber++;

                continue;
            }
            long transactionCount = transactionStore.getTransactionCount(transactionId);

            if (transactionStore.transactionContainsAllIds(topItemSetTraverser.getItemSet(), transactionId)) {
                occurences += transactionCount;
            }

            maxReachableTransactionCount -= transactionCount;

            // exit early if min support given for early stop can't be reached
            if (maxReachableTransactionCount + occurences < earlyStopMinCount) {
                break;
            }

            transactionNumber++;
        }

        rememberCountInStack(depth, occurences);
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
    public long getPreviousCount() {
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

    public boolean hasPredecessorBeenVisited() {
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

    public void setPredecessorVisited() {
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
     * Get the current item set
     */
    public LongsRef getItemSet() {
        return topItemSetTraverser.getItemSet();
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
        Releasables.close(topItemSetTraverser, topTransactionIds);
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
