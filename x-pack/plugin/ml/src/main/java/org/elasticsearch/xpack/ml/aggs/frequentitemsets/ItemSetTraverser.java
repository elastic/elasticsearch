/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.List;
import java.util.Stack;

public class ItemSetTraverser implements Releasable {

    private final TransactionStore.TopItemIds topItemIds;

    private long itemId = -1;
    private Stack<TransactionStore.TopItemIds.IdIterator> itemIteratorStack = new Stack<>();
    private Stack<Long> itemIdStack = new Stack<>();

    public ItemSetTraverser(TransactionStore.TopItemIds topItemIds) {
        this.topItemIds = topItemIds;

        // push the first iterator
        itemIteratorStack.add(topItemIds.iterator());
    }

    public boolean hasNext() {
        // check if we are already exhausted
        if (itemIteratorStack.isEmpty()) {
            return false;
        }
        return itemIteratorStack.peek().hasNext();
    }

    public boolean next() {
        // check if we are already exhausted
        if (itemIteratorStack.isEmpty()) {
            return false;
        }

        for (;;) {
            if (itemIteratorStack.peek().hasNext()) {
                itemId = itemIteratorStack.peek().next();

                // the way the tree is traversed, it should not create dups
                assert itemIdStack.contains(itemId) == false : "detected duplicate";
                break;
            } else {
                itemIteratorStack.pop();
                if (itemIteratorStack.isEmpty()) {
                    return false;
                }
                itemIdStack.pop();
            }
        }

        // push a new iterator to the stack, TODO: avoid object churn
        // the iterator starts at the position of the current one, so it won't create dups
        itemIteratorStack.add(topItemIds.iterator(itemIteratorStack.peek().getIndex()));
        itemIdStack.add(itemId);

        return true;
    }

    public long getItemId() {
        return itemId;
    }

    public List<Long> getItemSet() {
        return itemIdStack;
    }

    public int getDepth() {
        return itemIteratorStack.size() - 1;
    }

    public void prune() {
        // already empty
        if (itemIteratorStack.isEmpty()) {
            return;
        }
        itemIteratorStack.pop();

        // the id stack has 1 item less
        if (itemIteratorStack.isEmpty()) {
            return;
        }
        itemIdStack.pop();
    }

    @Override
    public void close() {
        Releasables.close(topItemIds);
    }

}
