/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.numeric.NumericColumnReader;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;
import org.elasticsearch.columnar.substrate.MonotonicReader;

import java.io.IOException;
import java.util.List;

/**
 * Reads a string column written by {@link StringColumnWriter}, single- or multi-valued.
 *
 * <p>Slots are addressed by <b>value address</b> — a slot's 0-based position in the column's block-encoded
 * store, in {@code [0, numValues)}. A document maps to its value addresses through {@link #iterator()}: a
 * single-valued column maps a document's rank straight to its value address, while a multi-valued one looks
 * the range up in the value-address table.
 *
 * <p>A null slot holds an address like any other and stores zero bytes; {@link #isNullSlot} tells the two
 * apart, which is what keeps an inline null distinguishable from an empty string. That holds under either
 * layout — a dictionary names a null slot with the empty term's ordinal, or lets it escape as a zero-length
 * value, and the null-slot table settles it either way.
 *
 * <p>The values sit in a {@link ValueStream}: addressed in blocks of a fixed count of values, compressed in
 * chunks of a fixed number of bytes, with a chunk closing only on a block boundary so no value spans two of
 * them. That is the byte-derived chunking in {@code docs/PLAN.md} — a block of long urls and a block of single
 * characters are the same count of values and nothing like the same amount of data, so the unit that is
 * compressed is bounded by bytes and the unit that is addressed by values.
 */
public final class StringColumnReader {

    private final StringColumnMetadata meta;
    private final ColumnIteratorReader iteratorReader;
    private final ValueStream.Reader values;

    /** Set on a dictionary column: the terms, and an ordinal into them for every value. */
    private final ValueStream.Reader dictionary;
    private final NumericColumnReader ordinals;
    /** Set on a dictionary column that any value escaped: their bytes, and where each one's is. */
    private final ValueStream.Reader escapes;
    private final LongValues escapeRanks;

    /** How many terms the dictionary holds, and how many values it did not name; zero on a plain column. */
    private final int dictionarySize;
    private final long escapeCount;
    private final int blockSize;

    /** The last value {@link #escapeRankOf} answered, and its rank, so an ascending pass carries on. */
    private long escapeCursorAddress = -1;
    private long escapeCursorRank;

    /** Where a document's slots begin, and which addresses hold a null; null when the column has neither. */
    private final LongValues valueAddresses;
    private final LongValues nullSlots;
    private final long numNullSlots;

    /** Index of the first null-slot entry at or after {@link #lastNullQuery}, and that entry's address. */
    private long nullCursor;
    private long nullCursorAddress;
    private long lastNullQuery = -1;

    private final BytesRef value = new BytesRef();

    /** Held so a summary can be read on demand; a merge reads it, an ordinary search never does. */
    private final IndexInput data;

    public StringColumnReader(StringColumnMetadata meta, IndexInput data) throws IOException {
        this.data = data;
        this.meta = meta;
        this.iteratorReader = new ColumnIteratorReader(meta.iterator(), data);
        final StringColumnMetadata.Addressing addressing = meta.addressing();
        this.valueAddresses = meta.hasValueAddresses()
            ? MonotonicReader.open(
                data,
                addressing.valueAddresses().meta(),
                meta.numDocsWithField() + 1L,
                addressing.valueAddresses().dataOffset(),
                addressing.valueAddresses().dataLength()
            )
            : null;
        this.numNullSlots = addressing.numNullSlots();
        this.nullSlots = meta.hasNullSlots()
            ? MonotonicReader.open(
                data,
                addressing.nullSlots().meta(),
                numNullSlots,
                addressing.nullSlots().dataOffset(),
                addressing.nullSlots().dataLength()
            )
            : null;
        this.nullCursorAddress = nullSlots == null ? Long.MAX_VALUE : nullSlots.get(0);
        switch (meta) {
            case StringColumnMetadata.Dictionary column -> {
                this.values = null;
                this.dictionary = column.dictionary().open(data);
                this.ordinals = new NumericColumnReader(column.ordinals(), data);
                this.dictionarySize = column.dictionarySize();
                this.blockSize = column.dictionary().valuesPerBlock();
                if (column.hasEscapes()) {
                    this.escapes = column.escapes().open(data);
                    this.escapeCount = column.escapes().numValues();
                    this.escapeRanks = MonotonicReader.open(
                        data,
                        column.escapeRanks().meta(),
                        StringColumnWriter.escapeRankEntries(column.numValues()),
                        column.escapeRanks().dataOffset(),
                        column.escapeRanks().dataLength()
                    );
                } else {
                    this.escapes = null;
                    this.escapeCount = 0;
                    this.escapeRanks = null;
                }
            }
            case StringColumnMetadata.Plain column -> {
                this.values = column.numDocsWithField() == 0 ? null : column.values().open(data);
                this.dictionary = null;
                this.ordinals = null;
                this.escapes = null;
                this.escapeRanks = null;
                this.dictionarySize = 0;
                this.escapeCount = 0;
                this.blockSize = column.values().valuesPerBlock();
            }
        }
    }

    /** A fresh iterator over the documents that have a value; positioned by {@link ColumnIterator#rank()}. */
    public ColumnIterator iterator() throws IOException {
        return iteratorReader.iterator();
    }

    /**
     * Whether a document's value address has to be looked up rather than being its rank. False only when the
     * column holds exactly one slot per document.
     */
    public boolean hasValueAddresses() {
        return valueAddresses != null;
    }

    /** The value address of a document's first slot, given its rank. */
    public long firstValueAddress(int rank) {
        return valueAddresses == null ? rank : valueAddresses.get(rank);
    }

    /** The number of slots a document has, given its rank; null slots are counted. */
    public long valueCount(int rank) {
        return valueAddresses == null ? 1 : valueAddresses.get(rank + 1) - valueAddresses.get(rank);
    }

    /**
     * Whether the slot at {@code valueAddress} is null rather than a value.
     *
     * <p>Both callers walk a document's addresses in order and documents in order, so this keeps a cursor
     * into the null-slot table and advances it, making a full scan cost one pass over that table. A caller
     * that asks about an address behind the one it last asked about re-seeks by binary search.
     */
    public boolean isNullSlot(long valueAddress) {
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
     * The value at {@code valueAddress} in {@code [0, numValues)}. The returned {@link BytesRef} points into a
     * buffer this reader reuses, so it is only valid until the next call. A null slot reads back as a
     * zero-length value; use {@link #isNullSlot} to tell it from an empty string.
     */
    public BytesRef valueAt(long valueAddress) throws IOException {
        if (dictionary != null) {
            // The ordinals are one per value in the same order, so a value address addresses them directly.
            final long ordinal = ordinals.valueAt(valueAddress);
            if (ordinal == dictionarySize) {
                escapes.get(escapeRankOf(valueAddress), value);
            } else {
                dictionary.get(ordinal, value);
            }
        } else {
            values.get(valueAddress, value);
        }
        return value;
    }

    /**
     * Where an escaped value's bytes are: how many escaped before it. The table gives that for the start
     * of its block and the ordinals in between give the rest.
     *
     * <p>Values are asked for in ascending order, so counting carries on from the last value answered
     * rather than from the start of its block whenever that value is nearer. A pass over a column then
     * reads each ordinal once instead of once for every escape that shares a block with it.
     */
    private long escapeRankOf(long valueAddress) throws IOException {
        final long block = valueAddress / StringColumnWriter.ESCAPE_RANK_BLOCK;
        final long blockStart = block * StringColumnWriter.ESCAPE_RANK_BLOCK;
        long at;
        long rank;
        if (escapeCursorAddress >= blockStart && escapeCursorAddress <= valueAddress) {
            at = escapeCursorAddress;
            rank = escapeCursorRank;
        } else {
            at = blockStart;
            rank = escapeRanks.get(block);
        }
        for (; at < valueAddress; at++) {
            if (ordinals.valueAt(at) == dictionarySize) {
                rank++;
            }
        }
        escapeCursorAddress = valueAddress;
        escapeCursorRank = rank;
        return rank;
    }

    /** Whether this column recorded what it surveyed, so a merge need not read its values again. */
    public boolean hasSummary() {
        return meta.hasSummary();
    }

    /** The values the summary's counts are a share of. */
    public long summaryValues() {
        return meta.hasSummary() ? meta.summary().numValues() : 0;
    }

    /**
     * The summarised terms and how often each was seen. The counts are the survey's and so are lower
     * bounds, which is what makes a vocabulary combined from several of them under-state its coverage
     * rather than over-state it.
     */
    public void readSummary(List<BytesRef> terms, List<Long> counts) throws IOException {
        final StringColumnMetadata.Summary summary = meta.summary();
        final ValueStream.Reader source = summary.terms() == null ? dictionary : summary.terms().open(data);
        final int size = summary.terms() == null ? dictionarySize : Math.toIntExact(summary.terms().numValues());
        final BytesRef term = new BytesRef();
        for (int ordinal = 0; ordinal < size; ordinal++) {
            source.get(ordinal, term);
            terms.add(BytesRef.deepCopyOf(term));
        }
        // Cloned rather than read in place: the caller's own reads are interleaved with these.
        final IndexInput in = data.clone();
        in.seek(summary.countsOffset());
        for (int ordinal = 0; ordinal < size; ordinal++) {
            counts.add(in.readVLong());
        }
    }

    /** How many terms the dictionary holds, or zero on a column that stores its values. */
    public int dictionarySize() {
        return dictionarySize;
    }

    /** Whether this column names its values with ordinals rather than storing them. */
    public boolean hasDictionary() {
        return dictionary != null;
    }

    /** How many values the dictionary did not name. */
    public long escapeCount() {
        return escapeCount;
    }

    /** What this column's values would occupy stored plainly, which a decision about it is weighed against. */
    public long valueBytes() {
        return meta.valueBytes();
    }

    /** The term at {@code ordinal}, on a column that has a dictionary. */
    public BytesRef termAt(int ordinal, BytesRef dst) throws IOException {
        dictionary.get(ordinal, dst);
        return dst;
    }

    /**
     * The ordinal the value at {@code valueAddress} takes, or {@link #dictionarySize()} when it escaped.
     * Only meaningful on a column that has a dictionary.
     */
    public int ordinalAt(long valueAddress) throws IOException {
        return Math.toIntExact(ordinals.valueAt(valueAddress));
    }

    /** Values behind one offset in the byte stream. */
    public int blockSize() {
        return blockSize;
    }

    /** Total number of slots across all documents, null slots included. */
    public long numValues() {
        return meta.numValues();
    }

    /** How many documents hold at least one slot. */
    public int numDocsWithField() {
        return meta.numDocsWithField();
    }

    /** How many of the column's slots are null. */
    public long numNullSlots() {
        return meta.numNullSlots();
    }

}
