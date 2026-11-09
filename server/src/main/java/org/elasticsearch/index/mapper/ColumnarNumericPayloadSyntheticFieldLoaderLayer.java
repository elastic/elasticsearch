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
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Loads {@code _source} for a numeric field whose doc values are a {@link ColumnarNumericDocValuesField} payload.
 * The payload keeps the document's non-null values in the order they were written, so a document without nulls needs
 * nothing else. One with nulls also has an {@code .offsets} companion, whose slots point at the payload's values in
 * order, or are null (see {@link FieldArrayContext#recordOffsetInDocOrder}).
 */
public class ColumnarNumericPayloadSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {
    private final String name;
    @Nullable
    private final String offsetsFieldName;
    private final SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer.NumericValueWriter writer;
    private final long[][] decoded = { new long[8] };
    private final ByteArrayStreamInput scratch = new ByteArrayStreamInput();

    private BinaryDocValues values;
    private SortedDocValues offsets;

    // Per-document decoded state.
    private int count;
    /** The document's slots, each an index into its values or {@link FieldArrayContext#NULL_ORD}; {@code null} without nulls. */
    private int[] offsetToOrd;

    public ColumnarNumericPayloadSyntheticFieldLoaderLayer(
        String name,
        @Nullable String offsetsFieldName,
        SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer.NumericValueWriter writer
    ) {
        this.name = Objects.requireNonNull(name);
        this.offsetsFieldName = offsetsFieldName;
        this.writer = Objects.requireNonNull(writer);
    }

    @Override
    public String fieldName() {
        return name;
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        values = DocValues.getBinary(leafReader, name);
        offsets = offsetsFieldName == null ? DocValues.emptySorted() : DocValues.getSorted(leafReader, offsetsFieldName);
        return this::advanceToDoc;
    }

    private boolean advanceToDoc(int docId) throws IOException {
        count = values.advanceExact(docId) ? NumericBinaryPayload.decode(values.binaryValue(), decoded) : 0;
        if (offsets.advanceExact(docId)) {
            final BytesRef encoded = offsets.lookupOrd(offsets.ordValue());
            scratch.reset(encoded.bytes, encoded.offset, encoded.length);
            offsetToOrd = FieldArrayContext.parseOffsetArray(scratch);
            assert slotsPointAtValuesInOrder() : "offsets of a document's values in order must number them in order";
        } else {
            offsetToOrd = null;
        }
        return hasValue();
    }

    private boolean slotsPointAtValuesInOrder() {
        int next = 0;
        for (int ord : offsetToOrd) {
            if (ord != FieldArrayContext.NULL_ORD && ord != next++) {
                return false;
            }
        }
        return next == count;
    }

    @Override
    public boolean hasValue() {
        return count > 0 || offsetToOrd != null;
    }

    @Override
    public long valueCount() {
        if (offsetToOrd != null) {
            // The offsets were recorded for an array, which has to render as one even when it holds a single slot.
            return offsetToOrd.length + 1;
        }
        return count;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (offsetToOrd == null) {
            for (int i = 0; i < count; i++) {
                writer.writeLongValue(b, decoded[0][i]);
            }
            return;
        }
        for (int ord : offsetToOrd) {
            if (ord == FieldArrayContext.NULL_ORD) {
                b.nullValue();
            } else {
                writer.writeLongValue(b, decoded[0][ord]);
            }
        }
    }
}
