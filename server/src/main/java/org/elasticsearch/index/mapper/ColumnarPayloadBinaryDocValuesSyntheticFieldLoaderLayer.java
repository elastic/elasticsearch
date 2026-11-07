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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Loads {@code _source} for a field whose doc values are stored by the ColumNAR codec, which writes them as a
 * {@link ColumnarBinaryDocValuesField} payload. Like {@link ArrayOrderBinaryDocValuesSyntheticFieldLoaderLayer} it preserves array order,
 * duplicates and inline {@code null} positions; unlike it, the slot count travels in the blob, so there is no companion field to advance
 * on and every shape a document takes — values, nulls, an empty array — arrives as one payload.
 */
public class ColumnarPayloadBinaryDocValuesSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {
    private final String name;
    private final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();

    private BinaryDocValues values;

    // Per-document decoded state.
    private boolean hasField;
    private BytesRef payload;
    private int slotCount;
    // Only computed for payloads of at most one slot: more slots always render as an array whatever they hold.
    private boolean allNull;

    public ColumnarPayloadBinaryDocValuesSyntheticFieldLoaderLayer(String name) {
        this.name = Objects.requireNonNull(name);
    }

    @Override
    public String fieldName() {
        return name;
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        values = DocValues.getBinary(leafReader, name);
        return this::advanceToDoc;
    }

    private boolean advanceToDoc(int docId) throws IOException {
        hasField = values.advanceExact(docId);
        if (hasField == false) {
            return false;
        }
        // The blob stays valid until the next advance, and nothing touches it between here and write().
        payload = values.binaryValue();
        slotCount = decoder.reset(payload);
        allNull = isAllNull(decoder, slotCount);
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
        return valueCount(slotCount, allNull);
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (hasField == false) {
            return;
        }
        decoder.reset(payload);
        writeSlots(b, decoder, slotCount);
    }

    /** Writes one value; UTF-8 text unless a field stores its values in another form. */
    protected void writeValue(XContentBuilder b, BytesRef value) throws IOException {
        b.utf8Value(value.bytes, value.offset, value.length);
    }

    /**
     * Whether a payload that {@code decoder} was just reset on holds no value at all: an empty array, or a single null slot. Only
     * payloads of at most one slot are asked, as more slots always render as an array whatever they hold.
     */
    private static boolean isAllNull(StringBinaryPayload.Decoder decoder, int slotCount) throws IOException {
        return slotCount == 0 || (slotCount == 1 && decoder.nullSlotCount() == 1);
    }

    /** The value count a present payload reports to {@link CompositeSyntheticFieldLoader}. */
    private static long valueCount(int slotCount, boolean allNull) {
        if (allNull) {
            // An empty array, or one holding nothing but nulls, still has to render as an array. Two is simply
            // "more than one" to CompositeSyntheticFieldLoader, which is all it takes to get the brackets.
            return 2;
        }
        // One slot renders as a scalar, more than one as an array; CompositeSyntheticFieldLoader reads nothing
        // into the count beyond that.
        return slotCount;
    }

    /** Writes the slots of the payload {@code decoder} was just reset on, a null slot as {@code null}. */
    private void writeSlots(XContentBuilder b, StringBinaryPayload.Decoder decoder, int slotCount) throws IOException {
        for (int slot = 0; slot < slotCount; slot++) {
            final BytesRef value = decoder.next();
            if (value == null) {
                b.nullValue();
            } else {
                writeValue(b, value);
            }
        }
    }

    /**
     * The direct counterpart of this layer on the columnar batch path: it reads the payloads from {@code payloadColumn}, the column
     * the mapper has just built, instead of from doc values, and renders them through the same {@link #valueCount(int, boolean)} and
     * {@link #writeSlots} rules, so that the {@code columnar_stored} blob comes out identical whichever path wrote it.
     *
     * @param payloadColumn the scalar binary column holding one {@link StringBinaryPayload} per document that has the field
     */
    public SourceColumn.Layer sourceLayer(EscfColumnData payloadColumn) {
        return new SourceLayer(payloadColumn);
    }

    private final class SourceLayer implements SourceColumn.Layer {
        private final ForwardBytesRefColumn payloads;
        private final StringBinaryPayload.Decoder sourceDecoder = new StringBinaryPayload.Decoder();

        private boolean present;
        private int sourceSlotCount;
        private boolean sourceAllNull;

        SourceLayer(EscfColumnData payloadColumn) {
            this.payloads = new ForwardBytesRefColumn(payloadColumn);
        }

        @Override
        public void advance(int doc) throws IOException {
            present = payloads.advance(doc);
            if (present) {
                sourceSlotCount = sourceDecoder.reset(payloads.value());
                sourceAllNull = isAllNull(sourceDecoder, sourceSlotCount);
            }
        }

        @Override
        public long valueCount() {
            return present ? ColumnarPayloadBinaryDocValuesSyntheticFieldLoaderLayer.valueCount(sourceSlotCount, sourceAllNull) : 0;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (present == false) {
                return;
            }
            sourceDecoder.reset(payloads.value());
            writeSlots(b, sourceDecoder, sourceSlotCount);
        }
    }
}
