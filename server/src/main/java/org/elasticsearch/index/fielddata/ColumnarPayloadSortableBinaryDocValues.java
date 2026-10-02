/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnSource;

import java.io.IOException;
import java.util.Arrays;

/**
 * A keyword field's values at the fielddata surface, read from a {@code BINARY_COLUMNAR_PAYLOAD} column.
 *
 * <p>Null slots are dropped, as they are for every other format at this surface. Nothing is sorted here: this
 * format writes a document's slots in the order they were given and reads them back the same way, which is what
 * {@link ValueOrder#ARRAY} says. A caller wanting them ordered orders them itself.
 *
 * <p>How many values a document holds is answered from the column, which records it, so an aggregation that asks only
 * for the count — {@code value_count}, or anything deciding on arity before it reads — never has a value decoded on
 * its behalf.
 *
 * <p>The values themselves come out of the column one slot at a time, as they are asked for, and are handed over
 * where the column holds them. Nothing is buffered and nothing is copied: a document is never assembled, only read
 * through. Holding a document's values together is what sorting them needed, and this does not sort.
 *
 * <p>A column read through something other than the codec's own doc values has no slots to walk. Those values come
 * out of the payload {@link org.apache.lucene.index.BinaryDocValues#binaryValue()} rebuilds, which has to be taken
 * apart in one pass, so that route alone keeps a document's values while it reads them.
 */
public final class ColumnarPayloadSortableBinaryDocValues extends SortableBinaryDocValues {

    private final BinaryDocValues binary;
    private final StringColumnSource source;
    private final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
    private final Sparsity sparsity;
    private final ValueMode valueMode;

    /** Where the payload route keeps a document's values. The column route hands them over where they lie. */
    private BytesRefBuilder[] values = new BytesRefBuilder[] { new BytesRefBuilder() };

    private int count;
    private int index;
    /** The next slot of the current document for the column route to read, null slots included. */
    private int slot;
    /** The document's slot count, kept while assertions are on so {@link #nextValue()} can bound {@link #slot}. */
    private int slotCount;

    public ColumnarPayloadSortableBinaryDocValues(BinaryDocValues binary) {
        this(binary, null, Sparsity.UNKNOWN, ValueMode.UNKNOWN);
    }

    private ColumnarPayloadSortableBinaryDocValues(
        BinaryDocValues binary,
        StringColumnSource source,
        Sparsity sparsity,
        ValueMode valueMode
    ) {
        super(null);
        this.binary = binary;
        this.source = source;
        this.sparsity = sparsity;
        this.valueMode = valueMode;
    }

    public static ColumnarPayloadSortableBinaryDocValues from(LeafReader leafReader, String valuesFieldName) throws IOException {
        final BinaryDocValues binary = DocValues.getBinary(leafReader, valuesFieldName);
        // A column records both of these, so neither costs a walk over the documents. A segment that arrives
        // as an overlay records neither and leaves them unknown.
        if (binary instanceof StringColumnSource source) {
            final StringColumnReader column = source.reader();
            return new ColumnarPayloadSortableBinaryDocValues(
                binary,
                source,
                column.numDocsWithField() == leafReader.maxDoc() ? Sparsity.DENSE : Sparsity.SPARSE,
                // No value addresses means one slot a document, so none holds two. A null slot is no value,
                // which single valued allows: it says at most one.
                column.hasValueAddresses() ? ValueMode.UNKNOWN : ValueMode.SINGLE_VALUED
            );
        }
        return new ColumnarPayloadSortableBinaryDocValues(binary);
    }

    @Override
    public Sparsity getSparsity() {
        return sparsity;
    }

    @Override
    public ValueMode getValueMode() {
        return valueMode;
    }

    @Override
    public ValueOrder getValueOrder() {
        return ValueOrder.ARRAY;
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        if (binary.advanceExact(doc) == false) {
            count = 0;
            return false;
        }
        index = 0;
        if (source != null) {
            slot = 0;
            count = source.nonNullValueCount();
            assert trackSlotCount();
            return count > 0;
        }
        decodePayload();
        return count > 0;
    }

    @Override
    public int docValueCount() {
        return count;
    }

    @Override
    public BytesRef nextValue() throws IOException {
        assert index < count;
        index++;
        if (source == null) {
            return values[index - 1].get();
        }
        BytesRef value;
        do {
            // The column counted this document's non-null slots, so one is always left to find. Past the last
            // slot the source would hand back the next document's value rather than fail, so this holds it here.
            assert slot < slotCount : "slot " + slot + " beyond the document's " + slotCount;
            value = source.slotAt(slot++);
        } while (value == null);
        return value;
    }

    /** Records the document's slot count so {@link #nextValue()} can bound {@link #slot}. Assertions only. */
    private boolean trackSlotCount() throws IOException {
        slotCount = source.slotCount();
        assert slotCount >= count : "a document of " + slotCount + " slots cannot hold " + count + " values";
        return true;
    }

    /** Reads the document's values out of the payload, for values that do not come from a column this can walk. */
    private void decodePayload() throws IOException {
        final int payloadSlots = decoder.reset(binary.binaryValue());
        // Size the scratch to the slot count — an upper bound on the surviving non-null values — then trim to the non-null total.
        grow(payloadSlots);
        int nonNull = 0;
        for (int i = 0; i < payloadSlots; i++) {
            final BytesRef value = decoder.next();
            if (value == null) {
                continue; // null slot
            }
            values[nonNull++].copyBytes(value);
        }
        count = nonNull;
    }

    private void grow(int size) {
        if (values.length < size) {
            final int oldLen = values.length;
            final int newLen = ArrayUtil.oversize(size, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            values = Arrays.copyOf(values, newLen);
            for (int i = oldLen; i < newLen; ++i) {
                values[i] = new BytesRefBuilder();
            }
        }
    }
}
