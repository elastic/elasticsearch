/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueArrayOrderInlineNullBinaryDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;

import java.io.IOException;

import static org.elasticsearch.index.mapper.blockloader.Warnings.registerSingleValueWarning;

/**
 * Single-value length reader for the {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ArrayOrderInlineNull
 * ArrayOrderInlineNull} format. Unlike the SeparateCount reader, the {@code .counts} field here includes null slots and a document with
 * no non-null values writes no binary blob, so arity is decided by the NON-NULL slot count (nulls are dropped): exactly one non-null
 * value yields its length, two or more emit a single-value warning and null, and zero yields null.
 */
public abstract class MultiValuedBinaryArrayOrderInlineNullLengthReader extends BlockDocValuesReader {

    private final Warnings warnings;
    private final TrackingNumericDocValues counts;
    private final TrackingBinaryDocValues values;
    private final MultiValueArrayOrderInlineNullBinaryDocValuesReader reader = new MultiValueArrayOrderInlineNullBinaryDocValuesReader();
    private final BytesRef scratch = new BytesRef();

    MultiValuedBinaryArrayOrderInlineNullLengthReader(Warnings warnings, TrackingNumericDocValues counts, TrackingBinaryDocValues values) {
        super(null);
        this.warnings = warnings;
        this.counts = counts;
        this.values = values;
    }

    abstract int length(BytesRef bytesRef);

    public abstract String toString();

    @Override
    public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
        throws IOException {
        int count = docs.count() - offset;
        if (count == 1) {
            return blockForSingleDoc(factory, docs.get(offset));
        }

        try (BlockLoader.IntBuilder builder = factory.ints(count)) {
            for (int i = offset; i < docs.count(); i++) {
                int doc = docs.get(i);
                appendLength(doc, builder);
            }
            return builder.build();
        }
    }

    @Override
    public int docId() {
        return counts.docValues().docID();
    }

    private void appendLength(int docId, BlockLoader.IntBuilder builder) throws IOException {
        Integer length = lengthOrNull(docId);
        if (length == null) {
            builder.appendNull();
        } else {
            builder.appendInt(length);
        }
    }

    private BlockLoader.Block blockForSingleDoc(BlockLoader.BlockFactory factory, int docId) throws IOException {
        Integer length = lengthOrNull(docId);
        if (length == null) {
            return factory.constantNulls(1);
        }
        return factory.constantInt(length, 1);
    }

    /**
     * Returns the length of the single non-null value for {@code docId}, or {@code null} when the document has zero non-null values
     * (missing / lone null / all-null / empty array) or more than one non-null value (a single-value warning is then registered).
     */
    private Integer lengthOrNull(int docId) throws IOException {
        if (counts.docValues().advanceExact(docId) == false) {
            return null;
        }
        if (values.docValues().advanceExact(docId) == false) {
            // all-null array / lone null / empty array: a count is present but no binary blob is written
            return null;
        }
        int slotCount = Math.toIntExact(counts.docValues().longValue());
        int nonNull = reader.nonNullCount(values.docValues().binaryValue(), slotCount, scratch);
        if (nonNull == 1) {
            return length(scratch);
        }
        if (nonNull > 1) {
            registerSingleValueWarning(warnings);
        }
        return null;
    }

    @Override
    public final void close() {
        Releasables.close(counts, values);
    }
}
