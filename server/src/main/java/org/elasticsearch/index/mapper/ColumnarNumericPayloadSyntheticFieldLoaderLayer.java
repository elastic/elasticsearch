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
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Loads {@code _source} for a numeric field whose doc values are a {@link ColumnarNumericDocValuesField} payload.
 * The payload keeps the document's array in order, nulls and empty arrays included, so there is no
 * {@code .offsets} companion to read. Once the ColumNAR codec has stored the field, though, it holds only the
 * non-null values: null slots and empty arrays are dropped until the column learns to keep them.
 */
public class ColumnarNumericPayloadSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {
    private final String name;
    private final SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer.NumericValueWriter writer;
    private final NumericBinaryPayload.Decoder decoder = new NumericBinaryPayload.Decoder();

    private BinaryDocValues values;

    // Per-document decoded state.
    private boolean hasField;
    private BytesRef payload;
    private int slotCount;

    public ColumnarNumericPayloadSyntheticFieldLoaderLayer(
        String name,
        SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer.NumericValueWriter writer
    ) {
        this.name = Objects.requireNonNull(name);
        this.writer = Objects.requireNonNull(writer);
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
        // The payload stays valid until the next advance, and nothing touches it between here and write().
        payload = values.binaryValue();
        slotCount = decoder.reset(payload);
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
        if (slotCount == 0 || (slotCount == 1 && decoder.nullSlotCount() == 1)) {
            // An empty array, or one holding a lone null, still has to render as an array. Two is simply "more than one"
            // to CompositeSyntheticFieldLoader, which is all it takes to get the brackets.
            return 2;
        }
        // One slot renders as a scalar, more than one as an array.
        return slotCount;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (hasField == false) {
            return;
        }
        decoder.reset(payload);
        for (int slot = 0; slot < slotCount; slot++) {
            if (decoder.nextIsNull()) {
                b.nullValue();
            } else {
                writer.writeLongValue(b, decoder.nextValue());
            }
        }
    }
}
