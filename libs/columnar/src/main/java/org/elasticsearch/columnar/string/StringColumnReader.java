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
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;
import org.elasticsearch.columnar.substrate.MonotonicReader;

import java.io.IOException;

/**
 * Reads a string column written by {@link StringColumnWriter}, single- or multi-valued.
 *
 * <p>Slots are addressed by <b>value address</b> — a slot's 0-based position in the column's block-encoded
 * store, in {@code [0, numValues)}. A document maps to its value addresses through {@link #iterator()}: a
 * single-valued column maps a document's rank straight to its value address, while a multi-valued one looks
 * the range up in the value-address table.
 *
 * <p>A null slot holds an address like any other and stores zero bytes; {@link #isNullSlot} tells the two
 * apart, which is what keeps an inline null distinguishable from an empty string.
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
    private final LongValues valueAddresses;
    private final LongValues nullSlots;
    private final long numNullSlots;

    private final BytesRef value = new BytesRef();

    /** Index of the first null-slot entry at or after {@link #lastNullQuery}, and that entry's address. */
    private long nullCursor;
    private long nullCursorAddress;
    private long lastNullQuery = -1;

    public StringColumnReader(StringColumnMetadata meta, IndexInput data) throws IOException {
        this.meta = meta;
        this.iteratorReader = new ColumnIteratorReader(meta.iterator(), data);
        if (meta.numDocsWithField() == 0) {
            this.values = null;
            this.valueAddresses = null;
            this.nullSlots = null;
            this.numNullSlots = 0;
            return;
        }
        this.values = meta.values().open(data);
        this.valueAddresses = meta.multiValued()
            ? MonotonicReader.open(
                data,
                meta.valueAddressesMeta(),
                meta.numDocsWithField() + 1L,
                meta.valueAddressesDataOffset(),
                meta.valueAddressesDataLength()
            )
            : null;
        this.numNullSlots = meta.numNullSlots();
        this.nullSlots = meta.hasNullSlots()
            ? MonotonicReader.open(data, meta.nullSlotsMeta(), numNullSlots, meta.nullSlotsDataOffset(), meta.nullSlotsDataLength())
            : null;
        this.nullCursorAddress = nullSlots == null ? Long.MAX_VALUE : nullSlots.get(0);
    }

    /** A fresh iterator over the documents that have a value; positioned by {@link ColumnIterator#rank()}. */
    public ColumnIterator iterator() throws IOException {
        return iteratorReader.iterator();
    }

    /**
     * Whether any document holds more than one slot. A single-valued column maps a rank straight to a value
     * address.
     */
    public boolean multiValued() {
        return valueAddresses != null;
    }

    /** Which of the mapper's framings this column re-encodes into at the {@code BinaryDocValues} surface. */
    public StringBinaryPayload.Framing framing() {
        return meta.framing();
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
        values.get(valueAddress, value);
        return value;
    }

    /** Values behind one offset in the byte stream. */
    public int blockSize() {
        return meta.values().valuesPerBlock();
    }

    /** Total number of slots across all documents, null slots included. */
    public long numValues() {
        return meta.numValues();
    }

}
