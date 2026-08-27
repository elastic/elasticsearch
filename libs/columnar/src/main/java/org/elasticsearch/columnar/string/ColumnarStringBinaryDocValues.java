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
 * A string column at the {@code BINARY} surface. The API carries one {@link BytesRef} per document, so a
 * document's slots are re-encoded on the way out: {@link #binaryValue} rebuilds the {@link StringBinaryPayload}
 * the mapper wrote, count and all, which is what every reader of these fields decodes.
 *
 * <p>Ingest is the mirror image — {@link #decodePayloads} splits the payload the mapper writes.
 *
 * <p>Which {@link StringColumnLayout} the segment used is invisible here — a layout resolves its own encoding
 * inside the reader, so nothing layout-specific reaches this surface.
 */
public final class ColumnarStringBinaryDocValues extends BinaryDocValues {

    private final StringColumnReader reader;
    private final ColumnIterator iterator;
    private final StringBinaryPayload.Encoder encoder = new StringBinaryPayload.Encoder();

    public ColumnarStringBinaryDocValues(StringColumnReader reader, ColumnIterator iterator) {
        this.reader = reader;
        this.iterator = iterator;
    }

    /**
     * The document's slots, re-encoded as the {@link StringBinaryPayload} they arrived as. Rebuilt from the
     * column rather than stored, so the bytes are equal to what the mapper wrote without ever having been
     * kept in that form.
     */
    @Override
    public BytesRef binaryValue() throws IOException {
        final int rank = iterator.rank();
        final long first = reader.firstValueAddress(rank);
        final long count = reader.valueCount(rank);
        encoder.begin((int) count);
        for (long i = 0; i < count; i++) {
            final long address = first + i;
            encoder.append(reader.isNullSlot(address) ? null : reader.valueAt(address));
        }
        return encoder.get();
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
     * A streaming cursor that reads this column's slots directly off the data input — block-decoded, without
     * a payload round-trip, nulls included. Used on merge to feed one segment's slots into the writer.
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
            public int nullCount() {
                // Only the null-slot table is touched, so the counting pass costs no block decoding.
                int nulls = 0;
                for (long i = 0; i < count; i++) {
                    if (reader.isNullSlot(first + i)) {
                        nulls++;
                    }
                }
                return nulls;
            }

            @Override
            public BytesRef nextValue() throws IOException {
                final long address = first + upto++;
                return reader.isNullSlot(address) ? null : reader.valueAt(address);
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
     * Wraps a foreign {@link BinaryDocValues} as a write-path cursor by splitting each document's
     * {@link StringBinaryPayload}. This is the ingest path — the mapper writes that format precisely so the
     * count travels with the bytes — and the merge fallback for a segment written by some other
     * implementation of this surface.
     */
    public static StringColumnValues decodePayloads(BinaryDocValues binary) {
        return new StringColumnValues() {

            private final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
            private int count;

            @Override
            public int valueCount() {
                return count;
            }

            @Override
            public int nullCount() {
                return decoder.nullSlotCount();
            }

            @Override
            public BytesRef nextValue() {
                return decoder.next();
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
                    count = decoder.reset(binary.binaryValue());
                }
                return doc;
            }
        };
    }

}
