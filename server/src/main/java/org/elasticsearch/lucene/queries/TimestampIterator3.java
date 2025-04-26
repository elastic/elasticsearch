/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * If doc values skippers aren't helpful, then just fallback to doc values. (linear execution)
 */
// NOT USED
final class TimestampIterator3 extends DocIdSetIterator {

    final NumericDocValues timestamps;

    final long minTimestamp;
    final long maxTimestamp;

    int docID = -1;

    TimestampIterator3(NumericDocValues timestamps, long minTimestamp, long maxTimestamp) {
        this.timestamps = timestamps;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    @Override
    public int docID() {
        return docID;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(docID + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        target = timestamps.advance(target);
        if (target == DocIdSetIterator.NO_MORE_DOCS) {
            return docID = target;
        }
        while (true) {
            long value = timestamps.longValue();
            if (value >= minTimestamp && value <= maxTimestamp) {
                return docID = target;
            } else {
                target = timestamps.nextDoc();
                if (target == DocIdSetIterator.NO_MORE_DOCS) {
                    return docID = target;
                }
            }
        }
    }

    @Override
    public long cost() {
        return timestamps.cost();
    }
}
