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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ColumnInputs;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * A column that stores its values. Nothing names a value but its own bytes, so every filter the column's
 * order cannot answer compares them, and a page hands them over as they are.
 *
 * <p>A null takes an address like any other slot and stores no bytes. Its stored length is what tells it
 * from an empty string, so every read and every filter that can meet one asks the lengths.
 */
public final class PlainStringColumnReader extends StringColumnReader {

    /** Whether the column's values repeat often enough that naming a page's values pays, as the writer found. */
    private final boolean valuesWorthNaming;

    private final PlainValues.Reader values;

    private final boolean hasNullSlots;

    PlainStringColumnReader(StringColumnMetadata.Plain column, ColumnInputs inputs) throws IOException {
        super(column, inputs, column.values() == null ? StringColumnOptions.DEFAULT_VALUES_PER_BLOCK : column.values().valuesPerBlock());
        this.valuesWorthNaming = column.valuesWorthNaming();
        this.values = column.numDocsWithField() == 0 ? null : column.values().open(inputs);
        this.hasNullSlots = column.hasNullSlots();
    }

    @Override
    public int byteLengthAt(long valueAddress) throws IOException {
        return values.length(valueAddress);
    }

    /** Whether the slot at {@code valueAddress} is null, which its stored length says. */
    @Override
    public boolean isNullSlot(long valueAddress) throws IOException {
        return hasNullSlots && values.isNull(valueAddress);
    }

    /**
     * What one match decided about the last value it saw. A value read from the same stored bytes as the one
     * before it matches exactly as it did. Held per match rather than on the reader, since what it remembers is
     * the answer to one term.
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
     * Consecutive equal values take one entry in the page, so a column made of runs takes one a run rather than
     * one a document.
     */
    @Override
    protected boolean appendPage(int docCount, StringBlockSink sink) throws IOException {
        if (pageable()) {
            // One value a document and every one of them present: the page is the documents, with none of the
            // accounting below.
            return appendSingleValuedPage(docCount, sink);
        }
        final int values = countPageValues(docCount);
        growPageValues(Math.max(values, 1));
        pageBytesLength = 0;
        startPageSlots(values);
        int slots = 0;
        int at = 0;
        for (int i = 0; i < docCount; i++) {
            final int rank = pageRanks[i];
            final long first = firstValueAddress(rank);
            final long held = valueCount(rank);
            for (long s = 0; s < held; s++) {
                final long address = first + s;
                if (isNullSlot(address)) {
                    continue;
                }
                this.values.get(address, scratch);
                final int slot = pageSlotFor(scratch, slots);
                if (slot == slots) {
                    slots++;
                }
                pageOrdinals[at++] = slot;
            }
        }
        assert at == values : "wrote " + at + " values, counted " + values;
        point(pageDictionary, slots);
        if ((long) slots * MIN_PAGE_REPEAT > values) {
            for (int i = 0; i < values; i++) {
                pageValues[i] = pageDictionary[pageOrdinals[i]];
            }
            sink.appendValues(pageValues, values, pageValueCounts, docCount);
            return true;
        }
        sink.appendOrdinals(pageOrdinals, values, pageValueCounts, docCount, pageDictionary, slots);
        return true;
    }

    /** A page of a column holding one value a document, which is the shape a run-encoded column pays off on. */
    private boolean appendSingleValuedPage(int count, StringBlockSink sink) throws IOException {
        if (valuesWorthNaming == false) {
            return appendSingleValuedPageAsValues(count, sink);
        }
        growPageValues(count);
        pageBytesLength = 0;
        startPageSlots(count);
        int slots = 0;
        long previous = -1;
        int previousLength = -1;
        int previousSlot = -1;
        for (int i = 0; i < count; i++) {
            final long identity = values.read(pageRanks[i], scratch);
            // A value read from the same stored bytes as the one before it is a repeat without looking at them.
            if (previousSlot < 0 || identity != previous || scratch.length != previousLength) {
                // The slot before is the only one a column in term order can be repeating, so it is compared
                // before anything is hashed, and a column in term order then hashes once a run rather than once
                // a value. A value the page held earlier is found by its bytes, or it would take two slots.
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
            sink.appendValues(pageValues, count, null, count);
            return true;
        }
        sink.appendOrdinals(pageOrdinals, count, null, count, pageDictionary, slots);
        return true;
    }

    /**
     * The same page, without a dictionary being built for it. A page handed over as values never reads the one
     * the method above builds, and building it hashes every value and probes a table for it. So a column whose
     * values do not repeat is read this way instead: runs are still collapsed, which costs no bytes to find,
     * but nothing is hashed.
     *
     * <p>Only the way the values are found changes. What the sink is given is what it would have been given.
     */
    private boolean appendSingleValuedPageAsValues(int count, StringBlockSink sink) throws IOException {
        growPageValues(count);
        pageBytesLength = 0;
        int runs = 0;
        long previous = -1;
        int previousLength = -1;
        int previousRun = -1;
        for (int i = 0; i < count; i++) {
            final long identity = values.read(pageRanks[i], scratch);
            if (previousRun < 0 || identity != previous || scratch.length != previousLength) {
                // The run before is compared by its bytes before a new one is started.
                if (previousRun < 0 || pageSlotHolds(previousRun, scratch) == false) {
                    appendToPage(runs, scratch);
                    previousRun = runs++;
                }
                previous = identity;
                previousLength = scratch.length;
            }
            pageOrdinals[i] = previousRun;
        }
        point(pageDictionary, runs);
        for (int i = 0; i < count; i++) {
            pageValues[i] = pageDictionary[pageOrdinals[i]];
        }
        sink.appendValues(pageValues, count, null, count);
        return true;
    }
}
