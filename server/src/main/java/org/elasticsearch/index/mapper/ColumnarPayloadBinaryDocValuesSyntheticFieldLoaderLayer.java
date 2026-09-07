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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Loads {@code _source} for a field whose doc values are stored by the ColumNAR codec, which writes them as a
 * {@link ColumnarBinaryDocValuesField} payload. Like {@link ArrayOrderBinaryDocValuesSyntheticFieldLoaderLayer} it preserves array order,
 * duplicates and inline {@code null} positions; unlike it, the slot count travels in the blob, so there is no companion field to advance
 * on and every shape a document takes — values, nulls, an empty array — arrives as one payload.
 */
public final class ColumnarPayloadBinaryDocValuesSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {
    private final String name;
    private final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();

    private BinaryDocValues values;

    // Per-document decoded state.
    private boolean hasField;
    private BytesRef payload;
    private int slotCount;
    private int nonNullCount;

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
        nonNullCount = slotCount - decoder.nullSlotCount();
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
        if (nonNullCount == 0) {
            // An empty array, or one holding nothing but nulls, still has to render as an array. Two is simply
            // "more than one" to CompositeSyntheticFieldLoader, which is all it takes to get the brackets.
            return 2;
        }
        // One slot renders as a scalar, more than one as an array; CompositeSyntheticFieldLoader reads nothing
        // into the count beyond that.
        return slotCount;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (hasField == false) {
            return;
        }
        decoder.reset(payload);
        for (int slot = 0; slot < slotCount; slot++) {
            final BytesRef value = decoder.next();
            if (value == null) {
                b.nullValue();
            } else {
                b.utf8Value(value.bytes, value.offset, value.length);
            }
        }
    }
}
