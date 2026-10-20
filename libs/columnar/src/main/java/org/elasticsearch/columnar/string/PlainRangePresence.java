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
 * The documents that hold a value, without those whose every slot is owned by an entry the bounds rule out for a
 * query. A kept document may still not match, so the caller compares its values.
 *
 * <p>It moves forward only, so {@code advanceExact} and {@code ranks} throw. Where every document holds one slot a
 * run ends before the first document of the next entry, which the bounds may rule out. Elsewhere a document may
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
    private final boolean oneSlotADocument;

    private int keptEntry = -1;
    private long keptEndSlot;
    private int runLimit;
    private int currentEntry = -1;
    private long currentFirstSlot;
    private long currentEndSlot;

    PlainRangePresence(ColumnIterator presence, PlainSkipBounds.Reader bounds, PlainSkipBounds.Query query, Slots slots) {
        this.presence = presence;
        this.bounds = bounds;
        this.query = query;
        this.slots = slots;
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
        final int doc = presence.nextDoc();
        if (oneSlotADocument && doc != NO_MORE_DOCS && keptEntry >= 0 && presence.rank() < keptEndSlot) {
            runLimit = runLimit(doc, presence.rank());
            return doc;
        }
        return settle(doc);
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
            final int firstEntry = entryOf(first);
            final int lastEntry = count == 1 ? firstEntry : entryOf(first + count - 1);
            if (firstEntry == lastEntry && firstEntry == keptEntry) {
                runLimit = runLimit(doc, first);
                return doc;
            }
            if (keptIn(firstEntry, lastEntry)) {
                keep(doc, first, lastEntry);
                return doc;
            }
            int next = lastEntry + 1;
            while (next < bounds.entryCount() && bounds.excludes(next, query)) {
                next++;
            }
            if (next >= bounds.entryCount()) {
                return presence.advance(NO_MORE_DOCS);
            }
            final int target = bounds.firstDoc(next);
            if (target <= doc) {
                throw corrupt(next, target);
            }
            doc = presence.advance(target);
            if (doc == NO_MORE_DOCS || ownsEntryStart(next) == false) {
                throw corrupt(next, target);
            }
        }
        return NO_MORE_DOCS;
    }

    private int entryOf(long slot) throws IOException {
        if (currentEntry < 0 || slot < currentFirstSlot || slot >= currentEndSlot) {
            currentEntry = bounds.entryOf(slot);
            currentFirstSlot = bounds.firstSlot(currentEntry);
            currentEndSlot = bounds.endSlot(currentEntry);
        }
        return currentEntry;
    }

    private boolean ownsEntryStart(int entry) throws IOException {
        final int rank = presence.rank();
        final long first = slots.firstAddress(rank);
        final long start = bounds.firstSlot(entry);
        return first <= start && start < first + slots.count(rank);
    }

    private static CorruptIndexException corrupt(int entry, int document) {
        return new CorruptIndexException("plain skip entry [" + entry + "] starts at the document [" + document + "]", "plain skip bounds");
    }

    private boolean keptIn(int firstEntry, int lastEntry) throws IOException {
        for (int entry = firstEntry; entry <= lastEntry; entry++) {
            if (bounds.excludes(entry, query) == false) {
                return true;
            }
        }
        return false;
    }

    private void keep(int doc, long first, int lastEntry) throws IOException {
        if (bounds.excludes(lastEntry, query)) {
            keptEntry = -1;
            runLimit = doc + 1;
            return;
        }
        keptEntry = lastEntry;
        keptEndSlot = lastEntry == currentEntry ? currentEndSlot : bounds.endSlot(lastEntry);
        runLimit = runLimit(doc, first);
    }

    private int runLimit(int doc, long slot) {
        return (int) Math.min((long) doc + keptEndSlot - slot, NO_MORE_DOCS);
    }
}
