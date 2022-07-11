/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.List;

/** Based on {@link org.apache.lucene.search.DisjunctionDISIApproximation} */
public class DisjunctionDocIdSetIterator extends DocIdSetIterator {

    private final PriorityQueue<DocIdSetIterator> priorityQueue;
    private final long cost;

    public DisjunctionDocIdSetIterator(List<DocIdSetIterator> subIterators) {
        this.priorityQueue = new PriorityQueue<>(subIterators.size()) {
            @Override
            protected boolean lessThan(DocIdSetIterator a, DocIdSetIterator b) {
                return a.docID() < b.docID();
            }
        };
        long cost = 0;
        for (DocIdSetIterator w : subIterators) {
            priorityQueue.add(w);
            cost += w.cost();
        }
        this.cost = cost;
    }

    @Override
    public long cost() {
        return cost;
    }

    @Override
    public int docID() {
        return priorityQueue.top().docID();
    }

    @Override
    public int nextDoc() throws IOException {
        DocIdSetIterator top = priorityQueue.top();
        final int doc = top.docID();
        do {
            top.nextDoc();
            top = priorityQueue.updateTop();
        } while (top.docID() == doc);
        return top.docID();
    }

    @Override
    public int advance(int target) throws IOException {
        DocIdSetIterator top = priorityQueue.top();
        do {
            top.advance(target);
            top = priorityQueue.updateTop();
        } while (top.docID() < target);
        return top.docID();
    }
}
