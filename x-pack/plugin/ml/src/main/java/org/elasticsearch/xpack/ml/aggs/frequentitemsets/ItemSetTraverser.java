/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

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

    private final TransactionStore.TopItemIds topItemIds;

    // stack implementation: to avoid object churn this is not implemented as classical stack, but optimized for re-usage
    // non-optimized: Stack<TransactionStore.TopItemIds.IdIterator> itemIterators = new Stack<>();
    private List<TransactionStore.TopItemIds.IdIterator> itemIterators = new ArrayList<>();
    private int stackPosition = 0;

    private Stack<Long> itemIdStack = new Stack<>();

    ItemSetTraverser(TransactionStore.TopItemIds topItemIds) {
        this.topItemIds = topItemIds;

        // push the first iterator
        itemIterators.add(topItemIds.iterator());
    }

    public boolean hasNext() {
        // check if we are already exhausted
        // non-optimized: itemIterators.isEmpty()
        if (stackPosition == -1) {
            return false;
        }
        return itemIterators.get(stackPosition).hasNext();
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

                // the way the tree is traversed, it should not create dups
                assert itemIdStack.contains(itemId) == false : "detected duplicate";
                break;
            } else {
                // non-optimized: itemIterators.pop();
                --stackPosition;
                // non-optimized: itemIterators.isEmpty()
                if (stackPosition == -1) {
                    return false;
                }
                itemIdStack.pop();
            }
        }

        // push a new iterator on the stack
        // non-optimized: itemIterators.add(topItemIds.iterator(itemIteratorStack.peek().getIndex()));
        if (itemIterators.size() == stackPosition + 1) {
            itemIterators.add(topItemIds.iterator(itemIterators.get(stackPosition).getIndex()));
        } else {
            itemIterators.get(stackPosition + 1).reset(itemIterators.get(stackPosition).getIndex());
        }

        itemIdStack.add(itemId);
        ++stackPosition;

        return true;
    }

    public long getItemId() {
        return itemIdStack.peek();
    }

    public List<Long> getItemSet() {
        return itemIdStack;
    }

    public int getDepth() {
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
        itemIdStack.pop();
    }

    @Override
    public void close() {
        Releasables.close(topItemIds);
    }

}
