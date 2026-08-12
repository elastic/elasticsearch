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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;

/**
 * A string column at the {@code BINARY} surface: {@link #binaryValue} re-emits a document's values as a
 * {@link StringBinaryPayload}. Which {@link StringColumnLayout} the segment picked is invisible here — a
 * dictionary column resolves its ordinals internally, so ordinals never reach this surface.
 */
public final class ColumnarStringBinaryDocValues extends BinaryDocValues {

    private final StringColumnReader reader;
    private final ColumnIterator iterator;

    private final BytesRefBuilder payload = new BytesRefBuilder();
    private BytesRef[] values = new BytesRef[1];

    public ColumnarStringBinaryDocValues(StringColumnReader reader, ColumnIterator iterator) {
        this.reader = reader;
        this.iterator = iterator;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        final int rank = iterator.index();
        final int first = reader.firstOrdinal(rank);
        final int count = reader.valueCount(rank);
        if (values.length < count) {
            values = new BytesRef[ArrayUtil.oversize(count, Integer.BYTES)];
        }
        // Correct while columns are single-valued: a PLAIN column hands back one reused BytesRef, so
        // collecting several ordinals here would alias them onto the last value. Multi-valued support has to
        // copy each value out, or encode straight into the payload as it walks the ordinals.
        for (int i = 0; i < count; i++) {
            values[i] = reader.valueForOrdinal(first + i);
        }
        return StringBinaryPayload.encode(values, count, payload);
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
            private int first;
            private int count;
            private int upto;

            @Override
            public int valueCount() {
                return count;
            }

            @Override
            public BytesRef nextValue() throws IOException {
                return reader.valueForOrdinal(first + upto++);
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
                    int rank = iterator.index();
                    first = reader.firstOrdinal(rank);
                    count = reader.valueCount(rank);
                    upto = 0;
                }
                return doc;
            }
        };
    }

    /**
     * Wraps a foreign {@link BinaryDocValues} of {@link StringBinaryPayload}s as a write-path cursor. This is
     * the ingest path (the mapper hands the format payloads) and the merge fallback for a segment written by
     * some other implementation of this surface.
     */
    public static StringColumnValues decodePayloads(BinaryDocValues binary) {
        return new StringColumnValues() {
            private final StringBinaryPayload.Reader payloadReader = new StringBinaryPayload.Reader();
            private int count;

            @Override
            public int valueCount() {
                return count;
            }

            @Override
            public BytesRef nextValue() {
                return payloadReader.next();
            }

            @Override
            public int docID() {
                return binary.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return position(binary.nextDoc());
            }

            @Override
            public int advance(int target) throws IOException {
                return position(binary.advance(target));
            }

            @Override
            public long cost() {
                return binary.cost();
            }

            private int position(int doc) throws IOException {
                if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    count = payloadReader.reset(binary.binaryValue());
                }
                return doc;
            }
        };
    }
}
