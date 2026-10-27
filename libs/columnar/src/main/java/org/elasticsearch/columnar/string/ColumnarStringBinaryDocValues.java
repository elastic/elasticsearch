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
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.columnar.substrate.BytesSink;
import org.elasticsearch.columnar.substrate.ChunkedBytesReader;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnPrefetcher;
import org.elasticsearch.columnar.substrate.internal.ByteArrayInts;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.store.PrefetchableDocValues;

import java.io.IOException;

/**
 * A string column at the {@code BINARY} surface. The API carries one {@link BytesRef} per document, so a
 * document's slots are re-encoded on the way out: {@link #binaryValue} rebuilds the {@link StringBinaryPayload}
 * the mapper wrote, count and all, or hands back the value's own bytes for a single-valued column.
 *
 * <p>Ingest is the mirror image — {@link #decodePayloads} splits the payload the mapper writes.
 *
 * <p>Which {@link StringColumnLayout} the segment used is invisible here — a layout resolves its own encoding
 * inside the reader, so nothing layout-specific reaches this surface.
 */
public final class ColumnarStringBinaryDocValues extends BinaryDocValues implements StringColumnSource, PrefetchableDocValues {

    private final StringColumnReader reader;
    private final ColumnIterator iterator;
    private final boolean singleValued;
    private final StringBinaryPayload.Builder payload = new StringBinaryPayload.Builder();
    @Nullable
    private final IOSupplier<StringColumnReader> prefetchColumn;
    private ColumnPrefetcher prefetcher;

    /** The reader as a dictionary one, or null when the column has no dictionary. Typed once so a document is not asked. */
    private final DictionaryStringColumnReader dictionary;

    public ColumnarStringBinaryDocValues(StringColumnReader reader, ColumnIterator iterator, boolean singleValued) {
        this(reader, iterator, singleValued, null);
    }

    /** @param prefetchColumn opens another reader of the column, which prefetches */
    public ColumnarStringBinaryDocValues(
        StringColumnReader reader,
        ColumnIterator iterator,
        boolean singleValued,
        @Nullable IOSupplier<StringColumnReader> prefetchColumn
    ) {
        this.reader = reader;
        this.iterator = iterator;
        this.singleValued = singleValued;
        this.dictionary = reader instanceof DictionaryStringColumnReader d ? d : null;
        this.prefetchColumn = prefetchColumn;
    }

    /** Prefetches through a reader of its own, opened on first use; nothing without {@code prefetchColumn}. */
    @Override
    public boolean prefetch(int stage, int[] docs, int from, int to) throws IOException {
        if (prefetchColumn == null) {
            return false;
        }
        if (prefetcher == null) {
            prefetcher = new ColumnPrefetcher(prefetchColumn.get());
        }
        return prefetcher.prefetch(stage, docs, from, to);
    }

    /**
     * The document's slots, re-encoded as the {@link StringBinaryPayload} they arrived as. Rebuilt from the
     * column rather than stored, so the bytes are equal to what the mapper wrote without ever having been
     * kept in that form.
     *
     * <p>When the column was written with {@code singleValued = true} the blob is the raw value bytes —
     * no count prefix, no framing — exactly as {@code BinaryDocValuesFormat.PLAIN} specifies.
     */
    @Override
    public BytesRef binaryValue() throws IOException {
        final int rank = iterator.rank();
        if (singleValued) {
            return reader.valueAt(reader.firstValueAddress(rank));
        }
        final long first = reader.firstValueAddress(rank);
        final long count = reader.valueCount(rank);
        if ((long) reader.maxLength() * count > ChunkedBytesReader.RETAINED_SPAN_BYTES) {
            return largePayload(first, count);
        }
        payload.reset();
        for (long i = 0; i < count; i++) {
            // A null slot reads back as null, which is what appendSlot takes for one.
            payload.appendSlot(reader.valueAt(first + i));
        }
        return payload.build();
    }

    /**
     * A document whose payload may run past {@link ChunkedBytesReader#RETAINED_SPAN_BYTES}: sized from the stored lengths,
     * each value copied into it straight from the column, and built in a buffer of its own when it is that large.
     */
    private BytesRef largePayload(long first, long count) throws IOException {
        long bytes = 0;
        for (long i = 0; i < count; i++) {
            bytes += ByteArrayInts.MAX_VINT_BYTES + (reader.isNullSlot(first + i) ? 0 : reader.byteLengthAt(first + i));
        }
        final StringBinaryPayload.Builder target = bytes > ChunkedBytesReader.RETAINED_SPAN_BYTES
            ? new StringBinaryPayload.Builder()
            : payload;
        target.reset();
        target.reserve(bytes);
        for (long i = 0; i < count; i++) {
            final long address = first + i;
            if (reader.isNullSlot(address)) {
                target.appendSlot(null);
            } else {
                target.appendSlot(reader.byteLengthAt(address), sink -> reader.transferValueAt(address, sink));
            }
        }
        return target.build();
    }

    /** The column behind this surface, so a merge can read what it recorded rather than its values. */
    @Override
    public StringColumnReader reader() {
        return reader;
    }

    @Override
    public boolean singleValued() {
        return singleValued;
    }

    /**
     * The address of the slot {@link #denseTermOrdinal} last answered for, which {@link #escapedValue} reads. It is -1
     * after an answer of {@link #NO_VALUE} or {@link #MULTI_VALUED}, so a stale slot is never read.
     */
    private long valueSlot = -1;

    @Override
    public int denseTermOrdinal() throws IOException {
        if (dictionary == null) {
            throw new IllegalStateException("the column has no dictionary");
        }
        valueSlot = -1;
        final int rank = iterator.rank();
        if (reader.hasValueAddresses() == false) {
            // One slot a document, whose address is the rank: read its ordinal once and decide.
            final int ordinal = dictionary.ordinalAt(rank);
            if (ordinal == StringColumnMetadata.Dictionary.NULL_ORDINAL) {
                return NO_VALUE;
            }
            valueSlot = rank;
            return denseOrdinal(ordinal);
        }
        final long first = reader.firstValueAddress(rank);
        final long slots = reader.valueCount(rank);
        int found = NO_VALUE;
        for (long slot = first; slot < first + slots; slot++) {
            final int ordinal = dictionary.ordinalAt(slot);
            if (ordinal == StringColumnMetadata.Dictionary.NULL_ORDINAL) {
                continue;
            }
            if (found != NO_VALUE) {
                valueSlot = -1;
                return MULTI_VALUED;
            }
            found = denseOrdinal(ordinal);
            valueSlot = slot;
        }
        return found;
    }

    /** The dense ordinal of a stored one that names a value, or {@link #ESCAPED}. */
    private int denseOrdinal(int storedOrdinal) {
        assert storedOrdinal != StringColumnMetadata.Dictionary.NULL_ORDINAL : "a null slot names no value";
        return storedOrdinal == dictionary.escapeOrdinal() ? ESCAPED : storedOrdinal - StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL;
    }

    @Override
    public BytesRef termAt(int denseOrdinal, BytesRef dst) throws IOException {
        assert dictionary != null : "the column has no dictionary";
        assert denseOrdinal >= 0 && denseOrdinal < dictionary.dictionarySize()
            : "ordinal [" + denseOrdinal + "] names no term in a dictionary of " + dictionary.dictionarySize();
        return dictionary.termAt(denseOrdinal + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL, dst);
    }

    @Override
    public BytesRef escapedValue(BytesRef dst) throws IOException {
        assert dictionary != null : "the column has no dictionary";
        assert valueSlot >= 0 : "no document has answered with a value";
        return dictionary.resolveEscape(valueSlot, dst);
    }

    @Override
    public BytesRef extreme(boolean max, BytesRef dst) throws IOException {
        return reader.extreme(iterator.rank(), max, dst);
    }

    @Override
    public int nonNullValueCount() throws IOException {
        final int rank = iterator.rank();
        final long first = reader.firstValueAddress(rank);
        final long count = reader.valueCount(rank);
        int found = 0;
        for (long i = 0; i < count; i++) {
            if (reader.isNullSlot(first + i) == false) {
                found++;
            }
        }
        return found;
    }

    @Override
    public int slotCount() throws IOException {
        return Math.toIntExact(reader.valueCount(iterator.rank()));
    }

    /** The document {@link #firstAddress} was looked up for, so reading its slots does not look it up again. */
    private int firstAddressRank = -1;
    private long firstAddress;

    @Override
    public BytesRef slotAt(int slot) throws IOException {
        final int rank = iterator.rank();
        if (rank != firstAddressRank) {
            // Held so that reading a document's slots one at a time costs one address lookup, not one a slot.
            firstAddressRank = rank;
            firstAddress = reader.firstValueAddress(rank);
        }
        return reader.valueAt(firstAddress + slot);
    }

    @Override
    public int nonNullValues(BytesRef dst) throws IOException {
        final int rank = iterator.rank();
        final long first = reader.firstValueAddress(rank);
        final long count = reader.valueCount(rank);
        int found = 0;
        for (long i = 0; i < count; i++) {
            final long address = first + i;
            if (reader.isNullSlot(address)) {
                continue;
            }
            if (++found > 1) {
                // The caller wants the arity, not the values, and has what it needs the moment there are two.
                return 2;
            }
            final BytesRef value = reader.valueAt(address);
            dst.bytes = value.bytes;
            dst.offset = value.offset;
            dst.length = value.length;
        }
        return found;
    }

    @Override
    public int nonNullLength(int[] length) throws IOException {
        final int rank = iterator.rank();
        final long first = reader.firstValueAddress(rank);
        final long count = reader.valueCount(rank);
        int found = 0;
        for (long i = 0; i < count; i++) {
            final long address = first + i;
            if (reader.isNullSlot(address)) {
                continue;
            }
            if (++found > 1) {
                // As with the values: the caller wants the arity, and has it the moment there are two.
                return 2;
            }
            length[0] = reader.byteLengthAt(address);
        }
        return found;
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
    public int docIDRunEnd() throws IOException {
        return iterator.docIDRunEnd();
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
        // A map is only ever built from a dictionary, so that is the only column with ordinals to carry over.
        final DictionaryStringColumnReader dictionary = reader instanceof DictionaryStringColumnReader typed ? typed : null;
        return new StringColumnValues() {
            private long first;
            private long count;
            private int upto;
            private long at = -1;
            private long previousAt = -1;

            @Override
            public int valueCount() {
                return (int) count;
            }

            @Override
            public int nullCount() throws IOException {
                // Whichever layout this is, only what already says which slots are null is touched: the
                // lengths, or the ordinals. The values themselves are never decoded.
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
                previousAt = at;
                at = first + upto++;
            }

            @Override
            public int ordinal() throws IOException {
                if (ordinalMap == null || dictionary == null) {
                    return -1;
                }
                final int ordinal = dictionary.ordinalAt(at);
                if (ordinal == StringColumnMetadata.Dictionary.NULL_ORDINAL || ordinal >= ordinalMap.length) {
                    // Null, or escaped this column's dictionary. Neither names a term the column being
                    // written would recognise, so value() is what settles it.
                    return -1;
                }
                return ordinalMap[ordinal];
            }

            @Override
            public BytesRef value() throws IOException {
                return reader.valueAt(at);
            }

            @Override
            public void transferValue(BytesSink sink) throws IOException {
                reader.transferValueAt(at, sink);
            }

            @Override
            public boolean streamsValues() {
                return reader instanceof PlainStringColumnReader;
            }

            @Override
            public boolean repeatsPrevious() throws IOException {
                // A stored repeat is of the slot before, which is the previous value only if no slot was skipped since.
                return at > 0 && previousAt == at - 1 && reader.repeatsPreviousSlot(at);
            }

            @Override
            public boolean mayBeSorted() {
                return reader.valuesSorted();
            }

            @Override
            public int valueLength() throws IOException {
                return reader.isNullSlot(at) ? -1 : reader.byteLengthAt(at);
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

            private int position(int doc) throws IOException {
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
     * Wraps a foreign {@link BinaryDocValues} as a write-path cursor treating each document's blob as its
     * own raw value — one non-null slot, no count prefix. Used for {@code BinaryDocValuesFormat.PLAIN} fields
     * on the ingest path and as the merge fallback for foreign segments of the same format.
     */
    public static StringColumnValues decodeRawValues(BinaryDocValues binary) {
        return new StringColumnValues() {

            private BytesRef slot;

            @Override
            public int valueCount() {
                return 1;
            }

            @Override
            public int nullCount() {
                return 0;
            }

            @Override
            public void nextValue() throws IOException {
                slot = binary.binaryValue();
            }

            @Override
            public BytesRef value() {
                return slot;
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
            public BytesRef value() {
                return slot;
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
