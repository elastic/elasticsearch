/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

/**
 * Forward-only iterator over the present (non-absent) doc ids of a column, ascending in
 * {@code [0, docCount)}. For sparse columns it delegates to a Lucene {@link BitSetIterator} over
 * the validity bitset; for dense columns ({@code validity == null}, every doc present) it counts
 * through every doc.
 */
final class PresentDocIterator {
    private final BitSetIterator sparse; // null => dense
    private final int docCount;
    private int doc = -1;

    PresentDocIterator(FixedBitSet validity, int docCount) {
        this.sparse = validity == null ? null : new BitSetIterator(validity, docCount);
        this.docCount = docCount;
    }

    /** The next present doc id, or {@link DocIdSetIterator#NO_MORE_DOCS} when exhausted. */
    int nextDoc() {
        if (sparse != null) {
            return doc = sparse.nextDoc();
        }
        return doc = (doc + 1 < docCount) ? doc + 1 : DocIdSetIterator.NO_MORE_DOCS;
    }
}
