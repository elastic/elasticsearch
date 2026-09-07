/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.MonotonicReader;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * A column that stores its values. Nothing names a value but its own bytes, so every filter the column's
 * order cannot answer compares them, and a page hands them over as they are.
 *
 * <p>What makes that affordable is that a repeated value is stored once: the store answers two addresses in
 * the same run with the same token, so a run is decided once and copied once however many documents carry
 * it.
 *
 * <p>A null is stored as a zero-length value and its address tabled, bytes having no spare value to mean
 * null with the way an ordinal does. So the table is the only thing separating a null from an empty string
 * here, and every read and every filter has to ask it.
 */
public final class PlainStringColumnReader extends StringColumnReader {

    private final ValueStream.Reader values;

    /** The value addresses holding a null, ascending; null when no slot in the column is one. */
    private final LongValues nullSlots;
    private final long numNullSlots;

    /** Index of the first null-slot entry at or after {@link #lastNullQuery}, and that entry's address. */
    private long nullCursor;
    private long nullCursorAddress;
    private long lastNullQuery = -1;

    PlainStringColumnReader(StringColumnMetadata.Plain column, IndexInput data) throws IOException {
        super(column, data, column.values().valuesPerBlock());
        this.values = column.numDocsWithField() == 0 ? null : column.values().open(data);
        this.numNullSlots = column.numNullSlots();
        this.nullSlots = column.hasNullSlots()
            ? MonotonicReader.open(
                data,
                column.nullSlots().meta(),
                column.numNullSlots(),
                column.nullSlots().dataOffset(),
                column.nullSlots().dataLength()
            )
            : null;
        this.nullCursorAddress = nullSlots == null ? Long.MAX_VALUE : nullSlots.get(0);
    }

    /**
     * Whether the slot at {@code valueAddress} is null, which only the null-slot table says. Callers walk a
     * document's addresses in order and documents in order, so this keeps a cursor into that table and
     * advances it, making a full scan cost one pass over it. A caller that asks about an address behind the
     * one it last asked about re-seeks by binary search.
     */
    @Override
    public boolean isNullSlot(long valueAddress) throws IOException {
        if (nullSlots == null) {
            return false;
        }
        if (valueAddress < lastNullQuery) {
            seekNullCursor(valueAddress);
        }
        lastNullQuery = valueAddress;
        while (nullCursorAddress < valueAddress) {
            nullCursor++;
            nullCursorAddress = nullCursor < numNullSlots ? nullSlots.get(nullCursor) : Long.MAX_VALUE;
        }
        return nullCursorAddress == valueAddress;
    }

    /** Positions the cursor on the first null slot at or after {@code valueAddress}. */
    private void seekNullCursor(long valueAddress) {
        long low = 0;
        long high = numNullSlots - 1;
        long found = numNullSlots;
        while (low <= high) {
            final long mid = (low + high) >>> 1;
            if (nullSlots.get(mid) >= valueAddress) {
                found = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        nullCursor = found;
        nullCursorAddress = found < numNullSlots ? nullSlots.get(found) : Long.MAX_VALUE;
    }

    /**
     * What one match decided about the last value it saw. A document holding the same value as the one
     * before it matches exactly as it did, so a run is decided once. Held per match rather than on the
     * reader, since what it remembers is the answer to one term.
     */
    private static final class LastSeen {
        private long identity = -1;
        private int length = -1;
        private boolean matched;
    }

    @Override
    public BytesRef valueAt(long valueAddress) throws IOException {
        if (isNullSlot(valueAddress)) {
            return null;
        }
        values.get(valueAddress, value);
        return value;
    }

    @Override
    protected DocIdSetIterator valueMatches(Predicate<BytesRef> matcher) throws IOException {
        final ColumnIterator presence = iterator();
        final BytesRef value = new BytesRef();
        final LastSeen lastSeen = new LastSeen();
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(presence) {
            @Override
            public boolean matches() throws IOException {
                final int rank = presence.rank();
                final long first = firstValueAddress(rank);
                final long count = valueCount(rank);
                if (count == 1) {
                    if (isNullSlot(first)) {
                        return false;
                    }
                    // A value repeating the one before it answers as it answered.
                    final long identity = values.read(first, value);
                    if (identity == lastSeen.identity && value.length == lastSeen.length) {
                        return lastSeen.matched;
                    }
                    final boolean matched = matcher.test(value);
                    lastSeen.identity = identity;
                    lastSeen.length = value.length;
                    lastSeen.matched = matched;
                    return matched;
                }
                for (long i = 0; i < count; i++) {
                    // A null is stored as no bytes, so without this it would be offered as an empty string.
                    if (isNullSlot(first + i)) {
                        continue;
                    }
                    values.get(first + i, value);
                    if (matcher.test(value)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public float matchCost() {
                return 10f;
            }
        });
    }

    /**
     * Compares the values, for a column with no order to bisect and no ordinals to match instead. A
     * two-phase iterator, so a scorer fills a window at a time rather than asking one document at a time.
     */
    @Override
    protected DocIdSetIterator unorderedMatches(BytesRef prefix, BytesRef exact) throws IOException {
        final ColumnIterator presence = iterator();
        final LastSeen lastSeen = new LastSeen();
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(presence) {
            @Override
            public boolean matches() throws IOException {
                return matchesRank(presence.rank(), prefix, exact, lastSeen);
            }

            @Override
            public float matchCost() {
                return 10f;
            }

        });
    }

    /** Whether any of a document's values matches, comparing the bytes of each one. */
    private boolean matchesRank(int rank, BytesRef prefix, BytesRef exact, LastSeen lastSeen) throws IOException {
        final long first = firstValueAddress(rank);
        final long count = valueCount(rank);
        // A document holding the same value as the one before it matches exactly as it did. On a column of
        // runs that answers most documents without looking at a value at all. A lone null is turned away
        // first: it is stored as no bytes, so it would otherwise be compared as an empty string.
        if (count == 1) {
            if (isNullSlot(first)) {
                return false;
            }
            final long identity = values.read(first, scratch);
            if (identity == lastSeen.identity && scratch.length == lastSeen.length) {
                return lastSeen.matched;
            }
            final boolean matched = matches(scratch, prefix, exact);
            lastSeen.identity = identity;
            lastSeen.length = scratch.length;
            lastSeen.matched = matched;
            return matched;
        }
        for (long i = 0; i < count; i++) {
            final BytesRef value = valueAt(first + i);
            // A null is no term and starts with no prefix, so it is passed over rather than compared.
            if (value != null && matches(value, prefix, exact)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A run is stored once, so consecutive values of it answer with the same token and only the first is
     * copied into the page. A column sorted on this field is made of runs, and this is where that pays: one
     * entry a run rather than one a document, without comparing any bytes.
     */
    @Override
    protected boolean appendPage(int count, StringBlockSink sink) throws IOException {
        pageBytesLength = 0;
        startPageSlots(count);
        int slots = 0;
        long previous = -1;
        int previousLength = -1;
        int previousSlot = -1;
        for (int i = 0; i < count; i++) {
            final long identity = values.read(pageRanks[i], scratch);
            // A run is stored once, so where a value was read tells a repeat of the one before it from a
            // new value without looking at any bytes. A value the page held earlier is a different address
            // and has to be found by its bytes, or the same value would take two slots.
            if (previousSlot < 0 || identity != previous || scratch.length != previousLength) {
                // Runs are staged a block at a time, so a run reaching into the next block is stored again
                // and answers with an address the one before it did not. The slot before is the only one a
                // column in term order can be repeating, so it is compared before anything is hashed, and
                // a column in term order then hashes once a value rather than once a block it spans.
                final int slot = previousSlot >= 0 && pageSlotHolds(previousSlot, scratch) ? previousSlot : pageSlotFor(scratch, slots);
                if (slot == slots) {
                    slots++;
                }
                previous = identity;
                previousLength = scratch.length;
                previousSlot = slot;
            }
            pageOrdinals[i] = previousSlot;
        }
        point(pageDictionary, slots);
        // As many entries as documents is no shorter as ordinals than as values.
        if ((long) slots * MIN_PAGE_REPEAT > count) {
            for (int i = 0; i < count; i++) {
                pageValues[i] = pageDictionary[pageOrdinals[i]];
            }
            sink.appendValues(pageValues, count);
            return true;
        }
        sink.appendOrdinals(pageOrdinals, count, pageDictionary, slots);
        return true;
    }
}
