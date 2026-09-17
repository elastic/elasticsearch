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
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnSource;

import java.io.IOException;
import java.util.Arrays;

/**
 * A keyword field's values at the fielddata surface, read from a {@code BINARY_COLUMNAR_PAYLOAD} column.
 *
 * <p>Null slots are dropped, as they are for every other format at this surface, and the surviving values are sorted.
 *
 * <p>How many values a document holds is answered from the column, which records it, so an aggregation that asks only
 * for the count — {@code value_count}, or anything deciding on arity before it reads — never has a value decoded on
 * its behalf. The values are read on the first read of the document, and a document that is never read costs nothing
 * beyond its count.
 *
 * <p>The values come out of the column itself, slot by slot, rather than out of the payload
 * {@link org.apache.lucene.index.BinaryDocValues#binaryValue()} would rebuild for the document. A document holding one
 * value — every document of a column that stores no value addresses — is handed that value where the column holds it,
 * so it is neither copied nor sorted. A document holding several is copied out, because sorting them means holding
 * them all at once.
 *
 * <p>Sorting is what this surface promises rather than what the column stores: the ColumNAR codec writes a keyword's
 * slots in document order so that array order survives, and the order is re-established here. A column read through
 * something other than the codec's own doc values has no slots to walk and is read from its payload.
 */
public final class ColumnarPayloadSortableBinaryDocValues extends SortableBinaryDocValues {

    private final BinaryDocValues binary;
    private final StringColumnSource source;
    private final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
    private final Sparsity sparsity;
    private final ValueMode valueMode;

    private BytesRefBuilder[] values = new BytesRefBuilder[] { new BytesRefBuilder() };
    private final InPlaceMergeSorter sorter = new InPlaceMergeSorter() {
        @Override
        protected void swap(int i, int j) {
            ArrayUtil.swap(values, i, j);
        }

        @Override
        protected int compare(int i, int j) {
            return values[i].get().compareTo(values[j].get());
        }
    };

    /** The lone value of a document that holds one, aliased onto the column's own bytes rather than copied. */
    private final BytesRef single = new BytesRef();

    private int count;
    private int index;
    /** Whether the document {@link #binary} stands on has been read; false while only its count is known. */
    private boolean decoded;

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
    public boolean advanceExact(int doc) throws IOException {
        if (binary.advanceExact(doc) == false) {
            count = 0;
            decoded = true;
            return false;
        }
        if (source != null) {
            count = source.nonNullValueCount();
            decoded = false;
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
        if (count == 1 && source != null) {
            if (decoded == false) {
                final int found = source.nonNullValues(single);
                assert found == 1 : "column counted one non-null value, found " + found;
                index = 0;
                decoded = true;
            }
            assert index < count;
            index++;
            return single;
        }
        if (decoded == false) {
            decodeColumn();
        }
        assert index < count;
        return values[index++].get();
    }

    /** Reads the document's values out of the column's slots, drops the null ones, and sorts what is left. */
    private void decodeColumn() throws IOException {
        final int slotCount = source.slotCount();
        // Size the scratch to the slot count — an upper bound on the surviving non-null values.
        grow(slotCount);
        int nonNull = 0;
        for (int slot = 0; slot < slotCount; slot++) {
            final BytesRef value = source.slotAt(slot);
            if (value == null) {
                continue; // null slot
            }
            // Copied because the next slot invalidates this one, and sorting needs them all at once.
            values[nonNull++].copyBytes(value);
        }
        assert nonNull == count : "column counted " + count + " non-null values, its slots hold " + nonNull;
        sort();
    }

    /** Reads the document's values out of the payload, for values that do not come from a column this can walk. */
    private void decodePayload() throws IOException {
        final int slotCount = decoder.reset(binary.binaryValue());
        // Size the scratch to the slot count — an upper bound on the surviving non-null values — then trim to the non-null total.
        grow(slotCount);
        int nonNull = 0;
        for (int slot = 0; slot < slotCount; slot++) {
            final BytesRef value = decoder.next();
            if (value == null) {
                continue; // null slot
            }
            values[nonNull++].copyBytes(value);
        }
        count = nonNull;
        sort();
    }

    private void sort() {
        sorter.sort(0, count);
        index = 0;
        decoded = true;
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
