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
    private final StringBinaryPayload.Builder payload = new StringBinaryPayload.Builder();

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
        payload.reset();
        for (long i = 0; i < count; i++) {
            final long address = first + i;
            payload.appendSlot(reader.isNullSlot(address) ? null : reader.valueAt(address));
        }
        return payload.build();
    }

    /** The column behind this surface, so a merge can read what it recorded rather than its values. */
    public StringColumnReader reader() {
        return reader;
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
        return directValues(null);
    }

    /**
     * As {@link #directValues()}, but reporting each value's ordinal translated through {@code ordinalMap}
     * so a merge can carry it over instead of resolving the value's bytes and looking them up again. A null
     * map, or a value that escaped this column's dictionary, falls back to the bytes.
     */
    public StringColumnValues directValues(int[] ordinalMap) {
        return new StringColumnValues() {
            private long first;
            private long count;
            private int upto;
            private long at = -1;

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
            public void nextValue() {
                at = first + upto++;
            }

            @Override
            public boolean isNull() {
                return reader.isNullSlot(at);
            }

            @Override
            public int ordinal() throws IOException {
                if (ordinalMap == null) {
                    return -1;
                }
                final int ordinal = reader.ordinalAt(at);
                if (ordinal >= ordinalMap.length) {
                    // Escaped this column's dictionary, so only its bytes say what it is.
                    return -1;
                }
                return ordinalMap[ordinal];
            }

            @Override
            public BytesRef value() throws IOException {
                return reader.valueAt(at);
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
            private final BytesRef empty = new BytesRef(BytesRef.EMPTY_BYTES);
            private int count;
            private BytesRef slot;

            @Override
            public int valueCount() {
                return count;
            }

            @Override
            public int nullCount() throws IOException {
                return decoder.nullSlotCount();
            }

            @Override
            public void nextValue() throws IOException {
                slot = decoder.next();
            }

            @Override
            public boolean isNull() {
                return slot == null;
            }

            @Override
            public BytesRef value() {
                return slot == null ? empty : slot;
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
