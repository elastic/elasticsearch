/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LongsRef;

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
class ItemSetTraverser {

    // start size and size increment for array holding items
    private static final int SIZE_INCREMENT = 100;

    private final TransactionStore.TopItemIds topItemIds;

    // stack implementation: to avoid object churn this is not implemented as classical stack, but optimized for re-usage
    // non-optimized: Stack<TransactionStore.TopItemIds.IdIterator> itemIterators = new Stack<>();
    private final List<TransactionStore.TopItemIds.IdIterator> itemIterators = new ArrayList<>();
    private LongsRef itemIdStack = new LongsRef(SIZE_INCREMENT);

    private final ItemSetBitSet itemPositionsVector;
    private final ItemSetBitSet itemPositionsVectorParent;
    private IntsRef itemPositionsStack = new IntsRef(SIZE_INCREMENT);

    private int stackPosition = 0;

    ItemSetTraverser(TransactionStore.TopItemIds topItemIds) {
        this.topItemIds = topItemIds;
        // push the first iterator
        itemIterators.add(topItemIds.iterator());

        // create a bit vector that corresponds to the number of items
        itemPositionsVector = new ItemSetBitSet((int) topItemIds.size());
        // create a bit vector that corresponds to the item set
        itemPositionsVectorParent = new ItemSetBitSet((int) topItemIds.size());
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
                itemPositionsStack.length--;
                itemPositionsVectorParent.clear(itemPositionsStack.ints[itemPositionsStack.length]);
                itemPositionsVector.clear(itemPositionsStack.ints[itemPositionsStack.length]);
            }
        }

        // push a new iterator on the stack

        int itemPosition = itemIterators.get(stackPosition).getIndex();
        // non-optimized: itemIterators.add(topItemIds.iterator(itemIteratorStack.peek().getIndex()));
        if (itemIterators.size() == stackPosition + 1) {
            itemIterators.add(topItemIds.iterator(itemPosition));
        } else {
            itemIterators.get(stackPosition + 1).reset(itemPosition);
        }

        growStacksIfNecessary();
        itemIdStack.longs[itemIdStack.length++] = itemId;

        // set the position from the previous step
        if (itemPositionsStack.length > 0) {
            itemPositionsVectorParent.set(itemPositionsStack.ints[itemPositionsStack.length - 1]);
        }

        // set the position from the this step
        itemPositionsStack.ints[itemPositionsStack.length++] = itemPosition;
        itemPositionsVector.set(itemPosition);
        ++stackPosition;

        return true;
    }

    public long getItemId() {
        return itemIdStack.longs[itemIdStack.length - 1];
    }

    public LongsRef getItemSet() {
        return itemIdStack;
    }

    public ItemSetBitSet getItemSetBitSet() {
        return itemPositionsVector;
    }

    public ItemSetBitSet getParentItemSetBitSet() {
        return itemPositionsVectorParent;
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
        itemPositionsStack.length--;
        itemPositionsVectorParent.clear(itemPositionsStack.ints[itemPositionsStack.length]);
        itemPositionsVector.clear(itemPositionsStack.ints[itemPositionsStack.length]);
    }

    private void growStacksIfNecessary() {
        if (itemIdStack.longs.length == itemIdStack.length) {
            itemIdStack.longs = ArrayUtil.grow(itemIdStack.longs, itemIdStack.length + SIZE_INCREMENT);
        }

        if (itemPositionsStack.ints.length == itemPositionsStack.length) {
            itemPositionsStack.ints = ArrayUtil.grow(itemPositionsStack.ints, itemPositionsStack.length + SIZE_INCREMENT);
        }
    }

}
