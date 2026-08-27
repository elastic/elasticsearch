/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;

/**
 * A string column at the {@code BINARY} surface: {@link #binaryValue} hands back a document's value as the
 * bytes it was given, which is what a keyword field writes for a lone value. Which {@link StringColumnLayout}
 * the segment used is invisible here — a layout resolves its own encoding inside the reader, so nothing
 * layout-specific reaches this surface.
 */
public final class ColumnarStringBinaryDocValues extends BinaryDocValues {

    private final StringColumnReader reader;
    private final ColumnIterator iterator;

    public ColumnarStringBinaryDocValues(StringColumnReader reader, ColumnIterator iterator) {
        this.reader = reader;
        this.iterator = iterator;
    }

    /**
     * The document's value, as the bytes the mapper handed over. A keyword field writes a lone value as its
     * raw bytes — no count, no length prefix — under both of the encodings the mapper uses, so a
     * single-valued column needs no encoding of its own here and hands the value straight back.
     *
     * <p>A column holding several values for one document has no representation at this surface. The writer
     * refuses to build one, so this cannot be reached; the assert says so for whoever lifts that.
     */
    @Override
    public BytesRef binaryValue() throws IOException {
        final int rank = iterator.rank();
        assert reader.valueCount(rank) == 1
            : "multi-valued string column reached binaryValue with "
                + reader.valueCount(rank)
                + " values; this surface carries one value per document";
        return reader.valueAt(reader.firstValueAddress(rank));
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return iterator.advanceExact(target);
    }

    @Override
    public int docID() {
        return iterator.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return iterator.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        return iterator.advance(target);
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
        iterator.intoBitSet(upTo, bitSet, offset);
    }

    @Override
    public long cost() {
        return iterator.cost();
    }

    /**
     * A streaming cursor that reads this column's values directly off the data input — block-decoded, without
     * a payload round-trip. Used on merge to feed one segment's values into the writer.
     */
    public StringColumnValues directValues() {
        return new StringColumnValues() {
            private long first;
            private long count;
            private int upto;

            @Override
            public int valueCount() {
                return (int) count;
            }

            @Override
            public BytesRef nextValue() throws IOException {
                return reader.valueAt(first + upto++);
            }

            @Override
            public int docID() {
                return iterator.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return position(iterator.nextDoc());
            }

            @Override
            public int advance(int target) throws IOException {
                return position(iterator.advance(target));
            }

            @Override
            public long cost() {
                return iterator.cost();
            }

            private int position(int doc) {
                if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    int rank = iterator.rank();
                    first = reader.firstValueAddress(rank);
                    count = reader.valueCount(rank);
                    upto = 0;
                }
                return doc;
            }
        };
    }

    /**
     * Wraps a foreign {@link BinaryDocValues} as a write-path cursor, one value per document, the value
     * being the bytes themselves. This is the ingest path — a keyword field writes a lone value as its raw
     * bytes — and the merge fallback for a segment written by some other implementation of this surface.
     */
    public static StringColumnValues singleValues(BinaryDocValues binary) {
        return new StringColumnValues() {

            @Override
            public int valueCount() {
                return 1;
            }

            @Override
            public BytesRef nextValue() throws IOException {
                return binary.binaryValue();
            }

            @Override
            public int docID() {
                return binary.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return binary.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return binary.advance(target);
            }

            @Override
            public long cost() {
                return binary.cost();
            }

        };
    }
}
