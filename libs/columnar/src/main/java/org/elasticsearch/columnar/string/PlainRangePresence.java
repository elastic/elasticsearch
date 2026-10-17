/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;

/**
 * The documents that hold a value, without those whose every slot lies in a block the bounds rule out for a query.
 * A kept document may still not match, so the caller compares its values.
 *
 * <p>It moves forward only, so {@code advanceExact} and {@code ranks} throw. Where every document holds one slot a
 * run ends before the first document of the next block, which the bounds may rule out. Elsewhere a document may
 * hold none and is not returned, so a run is the document alone.
 */
final class PlainRangePresence extends ColumnIterator {

    interface Slots {
        long firstAddress(int rank) throws IOException;

        long count(int rank) throws IOException;

        boolean oneSlotADocument();
    }

    private final ColumnIterator presence;
    private final PlainSkipBounds.Reader bounds;
    private final PlainSkipBounds.Query query;
    private final Slots slots;
    private final int blockShift;
    private final boolean oneSlotADocument;

    private int keptBlock = -1;
    private int runLimit;

    PlainRangePresence(ColumnIterator presence, PlainSkipBounds.Reader bounds, PlainSkipBounds.Query query, Slots slots, int blockShift) {
        this.presence = presence;
        this.bounds = bounds;
        this.query = query;
        this.slots = slots;
        this.blockShift = blockShift;
        this.oneSlotADocument = slots.oneSlotADocument();
    }

    @Override
    public int rank() {
        return presence.rank();
    }

    @Override
    public boolean advanceExact(int target) {
        throw new UnsupportedOperationException("a window reads documents in order");
    }

    @Override
    public int docID() {
        return presence.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return settle(presence.nextDoc());
    }

    @Override
    public int advance(int target) throws IOException {
        return settle(presence.advance(target));
    }

    @Override
    public int docIDRunEnd() throws IOException {
        if (oneSlotADocument == false) {
            return presence.docID() + 1;
        }
        return Math.min(presence.docIDRunEnd(), runLimit);
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
        if (oneSlotADocument == false) {
            super.intoBitSet(upTo, bitSet, offset);
            return;
        }
        int doc = docID();
        while (doc < upTo) {
            presence.intoBitSet(Math.min(docIDRunEnd(), upTo), bitSet, offset);
            doc = settle(presence.docID());
        }
    }

    @Override
    public long cost() {
        return presence.cost();
    }

    private int settle(int doc) throws IOException {
        while (doc != NO_MORE_DOCS) {
            final int rank = presence.rank();
            final long count = slots.count(rank);
            if (count == 0) {
                doc = presence.nextDoc();
                continue;
            }
            final long first = slots.firstAddress(rank);
            final int firstBlock = (int) (first >>> blockShift);
            final int lastBlock = (int) ((first + count - 1) >>> blockShift);
            if (firstBlock == lastBlock && firstBlock == keptBlock) {
                return doc;
            }
            if (keptIn(firstBlock, lastBlock)) {
                keep(doc, lastBlock);
                return doc;
            }
            int next = lastBlock + 1;
            while (next < bounds.blockCount() && bounds.excludes(next, query)) {
                next++;
            }
            if (next >= bounds.blockCount()) {
                return presence.advance(NO_MORE_DOCS);
            }
            final int target = bounds.firstDoc(next);
            if (target <= doc) {
                throw new CorruptIndexException(
                    "plain skip block [" + next + "] starts at the document [" + target + "]",
                    "plain skip bounds"
                );
            }
            doc = presence.advance(target);
        }
        return NO_MORE_DOCS;
    }

    private boolean keptIn(int firstBlock, int lastBlock) throws IOException {
        for (int block = firstBlock; block <= lastBlock; block++) {
            if (bounds.excludes(block, query) == false) {
                return true;
            }
        }
        return false;
    }

    private void keep(int doc, int lastBlock) throws IOException {
        if (bounds.excludes(lastBlock, query)) {
            keptBlock = -1;
            runLimit = doc + 1;
            return;
        }
        keptBlock = lastBlock;
        runLimit = lastBlock + 1 < bounds.blockCount() ? bounds.firstDoc(lastBlock + 1) : NO_MORE_DOCS;
    }
}
