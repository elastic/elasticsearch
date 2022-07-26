/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.util.LongsRef;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * A traverser that explores the item set tree, so item sets are generated exactly once.
 *
 * For example: A tree that builds all combinations would create traverse the set [a, b, c]
 * several times: a->b->c, a->c->b, b->a->c, b->c->a, c->a->b, c->b->a
 *
 * This traverser avoids those duplicates and only traverses [a, b, c] via a->b->c
 *
 * With other words: this traverser is only useful if order does not matter ("bag-of-words model").
 *
 * Note: In order to avoid churn, the traverser is reusing objects as much as it can,
 *       see the comments containing the non-optimized code
 */
class ItemSetTraverser implements Releasable {

    // start size and size increment for array holding items
    private static final int SIZE_INCREMENT = 100;

    private final TransactionStore.TopItemIds topItemIds;

    // stack implementation: to avoid object churn this is not implemented as classical stack, but optimized for re-usage
    // non-optimized: Stack<TransactionStore.TopItemIds.IdIterator> itemIterators = new Stack<>();
    private final List<TransactionStore.TopItemIds.IdIterator> itemIterators = new ArrayList<>();
    private LongsRef itemIdStack = new LongsRef(SIZE_INCREMENT);

    private int stackPosition = 0;

    ItemSetTraverser(TransactionStore.TopItemIds topItemIds) {
        this.topItemIds = topItemIds;
        // push the first iterator
        itemIterators.add(topItemIds.iterator());
    }

    /**
     * Return true if the iterator is at a leaf, which means it would backtrack on next()
     *
     * @return true if on a leaf
     */
    public boolean atLeaf() {
        // check if we are already exhausted
        // non-optimized: itemIterators.isEmpty()
        if (stackPosition == -1) {
            return false;
        }
        return itemIterators.get(stackPosition).hasNext() == false;
    }

    public boolean next() {
        // check if we are already exhausted
        // non-optimized: itemIterators.isEmpty()
        if (stackPosition == -1) {
            return false;
        }

        long itemId;
        for (;;) {
            if (itemIterators.get(stackPosition).hasNext()) {
                itemId = itemIterators.get(stackPosition).next();
                break;
            } else {
                // non-optimized: itemIterators.pop();
                --stackPosition;
                // non-optimized: itemIterators.isEmpty()
                if (stackPosition == -1) {
                    return false;
                }
                itemIdStack.length--;
            }
        }

        // push a new iterator on the stack
        // non-optimized: itemIterators.add(topItemIds.iterator(itemIteratorStack.peek().getIndex()));
        if (itemIterators.size() == stackPosition + 1) {
            itemIterators.add(topItemIds.iterator(itemIterators.get(stackPosition).getIndex()));
        } else {
            itemIterators.get(stackPosition + 1).reset(itemIterators.get(stackPosition).getIndex());
        }

        if (itemIdStack.longs.length == itemIdStack.length) {
            LongsRef resizedItemIdStack2 = new LongsRef(itemIdStack.length + SIZE_INCREMENT);
            System.arraycopy(itemIdStack.longs, 0, resizedItemIdStack2.longs, 0, itemIdStack.length);
            resizedItemIdStack2.length = itemIdStack.length;
            itemIdStack = resizedItemIdStack2;
        }

        itemIdStack.longs[itemIdStack.length++] = itemId;
        ++stackPosition;

        return true;
    }

    public long getItemId() {
        return itemIdStack.longs[itemIdStack.length - 1];
    }

    public LongsRef getItemSet() {
        return itemIdStack;
    }

    public int getNumberOfItems() {
        return stackPosition;
    }

    public void prune() {
        // already empty
        // non-optimized: itemIterators.isEmpty()
        if (stackPosition == -1) {
            return;
        }

        // non-optimized: itemIterators.pop();
        --stackPosition;

        // the id stack has 1 item less
        if (stackPosition == -1) {
            return;
        }
        itemIdStack.length--;
    }

    @Override
    public void close() {
        Releasables.close(topItemIds);
    }

}
