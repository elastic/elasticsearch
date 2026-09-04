/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.BinaryAndCounts;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;

import java.io.IOException;
import java.util.function.BiFunction;

/**
 * Loads byte length from BytesRef.
 */
public final class ByteLengthFromBytesRefDocValuesBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;
    private final Warnings warnings;
    private final BinaryDocValuesFormat binaryFormat;

    public ByteLengthFromBytesRefDocValuesBlockLoader(Warnings warnings, String fieldName) {
        this(warnings, fieldName, BinaryDocValuesFormat.SEPARATE_COUNT);
    }

    public ByteLengthFromBytesRefDocValuesBlockLoader(Warnings warnings, String fieldName, BinaryDocValuesFormat binaryFormat) {
        this.warnings = warnings;
        this.fieldName = fieldName;
        this.binaryFormat = binaryFormat;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.ints(expectedCount);
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        return switch (binaryFormat) {
            case COLUMNAR_PAYLOAD -> {
                // The count travels in the blob, so there is no companion column to load or advance on.
                TrackingBinaryDocValues binary = TrackingBinaryDocValues.get(breaker, context, fieldName);
                yield binary == null ? ConstantNull.COLUMN_READER : new MultiValuedBinaryColumnarPayload(warnings, binary);
            }
            case ARRAY_ORDER_INLINE_NULL -> withCounts(
                breaker,
                context,
                (binary, counts) -> new MultiValuedBinaryArrayOrderInlineNull(warnings, counts, binary)
            );
            case SEPARATE_COUNT -> withCounts(
                breaker,
                context,
                (binary, counts) -> new MultiValuedBinaryWithSeparateCounts(warnings, counts, binary)
            );
        };
    }

    /**
     * Resolves the binary column and its {@code .counts} companion, which both companion-carrying framings need, and
     * hands them to {@code reader}. A field with no counts column is single-valued, so its blob is a bare value.
     */
    private ColumnAtATimeReader withCounts(
        CircuitBreaker breaker,
        LeafReaderContext context,
        BiFunction<TrackingBinaryDocValues, TrackingNumericDocValues, ColumnAtATimeReader> reader
    ) throws IOException {
        BinaryAndCounts bc = BinaryAndCounts.get(breaker, context, fieldName, true);
        if (bc == null) {
            return ConstantNull.COLUMN_READER;
        }
        if (bc.counts() == null) {
            return new SingleValued(bc.binary());
        }
        return reader.apply(bc.binary(), bc.counts());
    }

    private static final class SingleValued extends BlockDocValuesReader {
        private final TrackingBinaryDocValues docValues;

        SingleValued(TrackingBinaryDocValues docValues) {
            super(null);
            this.docValues = docValues;
        }

        @Override
        public int docId() {
            return docValues.docValues().docID();
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docValues.docValues() instanceof BlockLoader.OptionalLengthReader direct) {
                BlockLoader.Block block = direct.tryReadLength(factory, docs, offset, nullsFiltered);
                if (block != null) {
                    return block;
                }
            }
            try (BlockLoader.IntBuilder builder = factory.ints(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        public void read(int doc, IntBuilder builder) throws IOException {
            if (false == docValues.docValues().advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            BytesRef bytes = docValues.docValues().binaryValue();
            builder.appendInt(bytes.length);
        }

        @Override
        public void close() {
            docValues.close();
        }

        @Override
        public String toString() {
            return "ByteLengthFromBytesRef.SingleValued";
        }
    }

    private static final class MultiValuedBinaryWithSeparateCounts extends MultiValuedBinaryWithSeparateCountsLengthReader {

        MultiValuedBinaryWithSeparateCounts(Warnings warnings, TrackingNumericDocValues counts, TrackingBinaryDocValues values) {
            super(warnings, counts, values);
        }

        @Override
        int length(BytesRef bytesRef) {
            return bytesRef.length;
        }

        @Override
        public String toString() {
            return "ByteLengthFromBytesRef.MultiValuedBinaryWithSeparateCounts";
        }
    }

    private static final class MultiValuedBinaryColumnarPayload extends MultiValuedBinaryColumnarPayloadLengthReader {

        MultiValuedBinaryColumnarPayload(Warnings warnings, TrackingBinaryDocValues values) {
            super(warnings, values);
        }

        @Override
        int length(BytesRef bytesRef) {
            return bytesRef.length;
        }

        @Override
        public String toString() {
            return "ByteLengthFromBytesRef.MultiValuedBinaryColumnarPayload";
        }
    }

    private static final class MultiValuedBinaryArrayOrderInlineNull extends MultiValuedBinaryArrayOrderInlineNullLengthReader {

        MultiValuedBinaryArrayOrderInlineNull(Warnings warnings, TrackingNumericDocValues counts, TrackingBinaryDocValues values) {
            super(warnings, counts, values);
        }

        @Override
        int length(BytesRef bytesRef) {
            return bytesRef.length;
        }

        @Override
        public String toString() {
            return "ByteLengthFromBytesRef.MultiValuedBinaryArrayOrderInlineNull";
        }
    }
}
