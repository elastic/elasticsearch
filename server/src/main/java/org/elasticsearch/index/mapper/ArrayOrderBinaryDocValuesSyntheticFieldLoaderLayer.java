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
    private final ByteArrayStreamInput scratchInput = new ByteArrayStreamInput();

    // Per-document decoded state. lengths[i] < 0 marks a null slot.
    private boolean hasField;
    private boolean binaryPresent;
    private int slotCount;
    private byte[] blobBytes;
    private int[] offsets = new int[8];
    private int[] lengths = new int[8];

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

        slotCount = Math.toIntExact(counts.longValue());
        binaryPresent = values.advanceExact(docId);

        if (binaryPresent == false) {
            // all-null array or empty array, so still a value and the presence of counts signifies this
            return true;
        }

        BytesRef bytes = values.binaryValue();
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

        return true;
    }

    @Override
    public boolean hasValue() {
        return hasField;
    }

    @Override
    public long valueCount() {
        if (hasField == false) {
            return 0;
        }
        if (binaryPresent == false) {
            // all-null array (slotCount nulls) or empty array (slotCount == 0): always serialized as an array
            return 2;
        }
        // A single non-null value collapses to a scalar; two or more slots are serialized as an array. The exact value beyond the
        // 0/1/>=2 distinction is irrelevant to CompositeSyntheticFieldLoader.
        return slotCount == 1 ? 1 : slotCount;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (hasField == false) {
            return;
        }
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
     * Grows the {@code offsets} and {@code lengths} scratch arrays so they hold at least {@code minSize} slots, reused across documents.
     */
    private void ensureCapacity(int minSize) {
        if (offsets.length < minSize) {
            offsets = ArrayUtil.grow(offsets, minSize);
            lengths = ArrayUtil.grow(lengths, minSize);
        }
    }
}
