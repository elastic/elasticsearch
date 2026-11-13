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
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Loads {@code _source} for a numeric field whose doc values are a {@link ColumnarNumericDocValuesField} payload.
 * The payload keeps the document's non-null values in the order they were written, which is all {@code _source} needs:
 * a ColumNAR column doesn't keep nulls.
 */
public class ColumnarNumericPayloadSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {
    private final String name;
    private final SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer.NumericValueWriter writer;
    private final long[][] decoded = { new long[8] };

    private BinaryDocValues values;
    /** The number of values the current document holds, read into {@link #decoded}. */
    private int count;

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
        count = readValues(docId);
        return hasValue();
    }

    /** Reads the document's values into {@link #decoded}, returning how many it holds. */
    private int readValues(int docId) throws IOException {
        if (values.advanceExact(docId) == false) {
            return 0;
        }
        if (values instanceof ColumnarNumericBinaryDocValues columnar) {
            // Read straight out of the column rather than building a payload only to decode it.
            final int valueCount = columnar.docValueCount();
            decoded[0] = ArrayUtil.grow(decoded[0], valueCount);
            for (int i = 0; i < valueCount; i++) {
                decoded[0][i] = columnar.valueAt(i);
            }
            return valueCount;
        }
        // Payloads that haven't been through the codec.
        return NumericBinaryPayload.decode(values.binaryValue(), decoded);
    }

    @Override
    public boolean hasValue() {
        return count > 0;
    }

    @Override
    public long valueCount() {
        return count;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        for (int i = 0; i < count; i++) {
            writer.writeLongValue(b, decoded[0][i]);
        }
    }
}
