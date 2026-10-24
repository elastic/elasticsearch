/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 * Loads {@code _source} for high-cardinality fields in strictly columnar index mode that store their values in document order via the
 * {@link MultiValuedBinaryDocValuesField.ArrayOrderInlineNull ArrayOrderInlineNull} format (no sidecar {@code .offsets} field). Unlike
 * {@link BinaryDocValuesSyntheticFieldLoaderLayer}, this layer preserves array order, duplicates, and inline {@code null} positions, and
 * it reconstructs all-null and empty arrays — so it advances on the {@code .counts} field (a document with no binary blob but a present
 * count is an all-null or empty array).
 */
public final class ArrayOrderBinaryDocValuesSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {

    private final String name;
    private final String countFieldName;
    private final Function<BytesRef, BytesRef> converter;

    private NumericDocValues counts;
    private BinaryDocValues values;

    // Per-document decoded state.
    private boolean hasField;
    private final DecodedDocument decoded = new DecodedDocument();

    public ArrayOrderBinaryDocValuesSyntheticFieldLoaderLayer(String name) {
        this(name, Function.identity());
    }

    /**
     * @param converter converts each non-null decoded slot before it is written as a utf8 string. Doc values that aren't directly
     *                  serializable as utf8 (ex. the {@code IpFieldMapper} encoded form) supply a converter; others use the identity.
     */
    public ArrayOrderBinaryDocValuesSyntheticFieldLoaderLayer(String name, Function<BytesRef, BytesRef> converter) {
        this.name = Objects.requireNonNull(name);
        this.countFieldName = name + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;
        this.converter = Objects.requireNonNull(converter);
    }

    @Override
    public String fieldName() {
        return name;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader.DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        counts = leafReader.getNumericDocValues(countFieldName);
        if (counts == null) {
            hasField = false;
            return null;
        }
        values = DocValues.getBinary(leafReader, name);
        return this::advanceToDoc;
    }

    private boolean advanceToDoc(int docId) throws IOException {
        // The .counts field is the presence signal: an all-null or empty array writes a count but no binary blob.
        hasField = counts.advanceExact(docId);
        if (hasField == false) {
            return false;
        }

        decoded.reset(Math.toIntExact(counts.longValue()), values.advanceExact(docId) ? values.binaryValue() : null);
        return true;
    }

    @Override
    public boolean hasValue() {
        return hasField;
    }

    @Override
    public long valueCount() {
        return hasField ? decoded.valueCount() : 0;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (hasField) {
            decoded.write(b, converter);
        }
    }

    /**
     * The direct counterpart of this layer on the columnar batch path: it reads the count and the blob of each document from the
     * columns the mapper has just built, instead of from doc values, and renders them through the same {@link DecodedDocument} the
     * loader uses, so that the {@code columnar_stored} blob comes out identical whichever path wrote it. As on the read side, the
     * counts column is the presence signal, and a document with a count but no blob is an all-null or empty array.
     *
     * @param blobColumn  the scalar binary column holding one {@code ArrayOrderInlineNull} blob per document with a non-null value,
     *                    or {@code null} when no document in the batch had one
     * @param countColumn the scalar long column holding the slot count per document with the field, or {@code null} when no
     *                    document in the batch had it
     */
    public SourceColumn.Layer sourceLayer(@Nullable EscfColumnData blobColumn, @Nullable EscfColumnData countColumn) {
        return new SourceLayer(blobColumn, countColumn, converter);
    }

    private static final class SourceLayer implements SourceColumn.Layer {
        @Nullable
        private final ForwardBytesRefColumn blobs;
        @Nullable
        private final EscfColumn counts;
        private final Function<BytesRef, BytesRef> converter;
        private final DecodedDocument decoded = new DecodedDocument();
        private boolean present;

        SourceLayer(@Nullable EscfColumnData blobColumn, @Nullable EscfColumnData countColumn, Function<BytesRef, BytesRef> converter) {
            this.blobs = blobColumn != null ? new ForwardBytesRefColumn(blobColumn) : null;
            this.counts = countColumn != null ? EscfColumn.from(countColumn) : null;
            this.converter = converter;
        }

        @Override
        public void advance(int doc) throws IOException {
            present = counts != null && counts.isPresent(doc);
            if (present) {
                final BytesRef blob = blobs != null && blobs.advance(doc) ? blobs.value() : null;
                decoded.reset(Math.toIntExact(counts.getLongValue(doc)), blob);
            }
        }

        @Override
        public long valueCount() {
            return present ? decoded.valueCount() : 0;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (present) {
                decoded.write(b, converter);
            }
        }
    }

    /**
     * One document's slots, decoded from its count and its {@code ArrayOrderInlineNull} blob. Shared by the loader layer and its direct
     * counterpart so that the two cannot disagree on what a count without a blob, a lone raw value, or a {@code len+1 == 0} slot means.
     */
    private static final class DecodedDocument {
        private final ByteArrayStreamInput scratchInput = new ByteArrayStreamInput();

        // lengths[i] < 0 marks a null slot.
        private boolean binaryPresent;
        private int slotCount;
        private byte[] blobBytes;
        private int[] offsets = new int[8];
        private int[] lengths = new int[8];

        /**
         * @param slotCount the document's count
         * @param bytes     the document's blob, or {@code null} for an all-null or empty array, which writes a count but no blob
         */
        void reset(int slotCount, @Nullable BytesRef bytes) throws IOException {
            this.slotCount = slotCount;
            this.binaryPresent = bytes != null;
            if (binaryPresent == false) {
                // all-null array or empty array, so still a value and the presence of counts signifies this
                return;
            }

            blobBytes = bytes.bytes;
            ensureCapacity(slotCount);

            if (slotCount == 1) {
                // single non-null value stored raw (a lone null writes no binary blob)
                offsets[0] = bytes.offset;
                lengths[0] = bytes.length;
            } else {
                // point the stream reader at the blob, then walk the slotCount [len+1][bytes] slots (len+1 == 0 marks a null)
                scratchInput.reset(bytes.bytes, bytes.offset, bytes.length);

                for (int i = 0; i < slotCount; i++) {
                    int encodedLength = scratchInput.readVInt();
                    if (encodedLength == 0) {
                        lengths[i] = -1; // null slot
                    } else {
                        // lengths are always encoded as len+1 to distinguish between empty strings and nulls, so here we must subtract 1
                        // to get back the actual length of the value
                        int length = encodedLength - 1;
                        int offset = scratchInput.getPosition();

                        // skip over the value bytes so the next readVInt lands on the following slot's length prefix
                        scratchInput.setPosition(offset + length);

                        offsets[i] = offset;
                        lengths[i] = length;
                    }
                }
            }
        }

        long valueCount() {
            if (binaryPresent == false) {
                // all-null array (slotCount nulls) or empty array (slotCount == 0): always serialized as an array
                return 2;
            }
            // A single non-null value collapses to a scalar; two or more slots are serialized as an array. The exact value beyond the
            // 0/1/>=2 distinction is irrelevant to CompositeSyntheticFieldLoader.
            return slotCount == 1 ? 1 : slotCount;
        }

        void write(XContentBuilder b, Function<BytesRef, BytesRef> converter) throws IOException {
            if (binaryPresent) {
                for (int i = 0; i < slotCount; i++) {
                    if (lengths[i] < 0) {
                        b.nullValue();
                    } else {
                        BytesRef converted = converter.apply(new BytesRef(blobBytes, offsets[i], lengths[i]));
                        b.utf8Value(converted.bytes, converted.offset, converted.length);
                    }
                }
            } else {
                // all-null array: emit one null per slot (empty array emits nothing, but valueCount() forces the surrounding array)
                for (int i = 0; i < slotCount; i++) {
                    b.nullValue();
                }
            }
        }

        /**
         * Grows the {@code offsets} and {@code lengths} scratch arrays so they hold at least {@code minSize} slots, reused across
         * documents.
         */
        private void ensureCapacity(int minSize) {
            if (offsets.length < minSize) {
                offsets = ArrayUtil.grow(offsets, minSize);
                lengths = ArrayUtil.grow(lengths, minSize);
            }
        }
    }
}
