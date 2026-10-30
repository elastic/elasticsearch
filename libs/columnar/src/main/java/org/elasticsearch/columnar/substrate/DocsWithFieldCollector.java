/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

/**
 * Remembers which documents hold a value while a pass that has to walk them anyway goes by, so that
 * {@link ColumnIteratorWriter} can write the column's presence from what was remembered instead of walking the
 * field once more. Where walking the field is what costs — on ingest every step over a binary field can copy
 * its value out of Lucene's indexing buffer — a sparse column goes from a pass of its own for presence to none.
 *
 * <p>Nothing is allocated while the documents seen are 0, 1, 2, and so on without a gap, which is all that a
 * dense column ever shows, so only a column that turns out sparse pays for a bit set, and it pays for it
 * from the first gap on. The bit set is sized to the segment, {@code maxDoc / 8} bytes, and lives no longer
 * than the column it was collected for.
 */
public final class DocsWithFieldCollector {

    private final int maxDoc;
    /** Null while every document seen so far was the next in line from 0. */
    private FixedBitSet bits;
    private int count;

    public DocsWithFieldCollector(int maxDoc) {
        this.maxDoc = maxDoc;
    }

    /** Records that {@code doc} holds a value; documents must arrive in increasing order. */
    public void add(int doc) {
        if (bits == null) {
            if (doc == count) {
                count++;
                return;
            }
            bits = new FixedBitSet(maxDoc);
            bits.set(0, count);
        }
        assert bits.nextSetBit(doc) == DocIdSetIterator.NO_MORE_DOCS : "document " + doc + " is out of order";
        bits.set(doc);
        count++;
    }

    /** The number of documents recorded. */
    public int count() {
        return count;
    }

    /** The recorded documents in increasing order. */
    public DocIdSetIterator iterator() {
        if (bits != null) {
            return new BitSetIterator(bits, count);
        }
        return count == 0 ? DocIdSetIterator.empty() : DocIdSetIterator.range(0, count);
    }
}
