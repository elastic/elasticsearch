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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;
import org.elasticsearch.columnar.substrate.MonotonicReader;

import java.io.IOException;

/**
 * Reads a string column written by {@link StringColumnWriter}.
 *
 * <p>Values are addressed by <b>value address</b> — a value's 0-based position in the column, in
 * {@code [0, numValues)}. A document maps to its value addresses through {@link #iterator()}: a single-valued
 * column maps a document's rank straight to its value address.
 *
 * <p>Reading one value is two lookups in the offset table and a read of exactly that span: no block is decoded,
 * so nothing is read that the caller did not ask for. The only heap this holds is one value's bytes, grown to
 * the longest value asked for so far.
 */
public final class StringColumnReader {

    private final StringColumnMetadata meta;
    private final ColumnIteratorReader iteratorReader;
    private final IndexInput data;
    private final LongValues valueOffsets;
    private final long valuesOffset;

    private final BytesRef value = new BytesRef();

    public StringColumnReader(StringColumnMetadata meta, IndexInput data) throws IOException {
        assert meta.multiValued() == false : "multi-valued string columns are not implemented yet";
        this.meta = meta;
        this.iteratorReader = new ColumnIteratorReader(meta.iterator(), data);
        this.data = data.clone();
        if (meta.numDocsWithField() == 0) {
            this.valueOffsets = null;
            this.valuesOffset = 0;
            return;
        }
        this.valueOffsets = MonotonicReader.open(
            data,
            meta.valueOffsetsMeta(),
            meta.numValues() + 1L,
            meta.valueOffsetsDataOffset(),
            meta.valueOffsetsDataLength()
        );
        this.valuesOffset = meta.valuesOffset();
        this.value.bytes = new byte[0];
    }

    /** A fresh iterator over the documents that have a value; positioned by {@link ColumnIterator#rank()}. */
    public ColumnIterator iterator() throws IOException {
        return iteratorReader.iterator();
    }

    /**
     * The value address of a document's first value, given its rank. String columns are single-valued for now,
     * so a document's rank is its value address; the seam is kept so multi-valued support stays a localized
     * change (the numeric column resolves this through a value-address table).
     */
    public long firstValueAddress(int rank) {
        return rank;
    }

    /** The number of values a document has, given its rank — always one until multi-valued columns land. */
    public long valueCount(int rank) {
        return 1;
    }

    /**
     * The value at {@code valueAddress} in {@code [0, numValues)}. The returned {@link BytesRef} points into a
     * buffer this reader reuses, so it is only valid until the next call.
     */
    public BytesRef valueAt(long valueAddress) throws IOException {
        long start = valueOffsets.get(valueAddress);
        int length = (int) (valueOffsets.get(valueAddress + 1) - start);
        if (value.bytes.length < length) {
            value.bytes = new byte[ArrayUtil.oversize(length, Byte.BYTES)];
        }
        data.seek(valuesOffset + start);
        data.readBytes(value.bytes, 0, length);
        value.offset = 0;
        value.length = length;
        return value;
    }

    /** Total number of values across all documents. */
    public long numValues() {
        return meta.numValues();
    }
}
