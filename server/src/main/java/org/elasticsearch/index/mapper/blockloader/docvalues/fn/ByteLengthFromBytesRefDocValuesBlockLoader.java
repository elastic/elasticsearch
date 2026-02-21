/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.AbstractLongsFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;

import java.io.IOException;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * Loads byte length from BytesRef.
 */
public final class ByteLengthFromBytesRefDocValuesBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;
    private final Warnings warnings;

    public ByteLengthFromBytesRefDocValuesBlockLoader(Warnings warnings, String fieldName) {
        this.warnings = warnings;
        this.fieldName = fieldName;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.ints(expectedCount);
    }

    @Override
    public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        breaker.addEstimateBytesAndMaybeBreak(BytesRefsFromBinaryBlockLoader.ESTIMATED_SIZE, "load blocks");
        long release = BytesRefsFromBinaryBlockLoader.ESTIMATED_SIZE;
        try {

            BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
            if (values == null) {
                return ConstantNull.READER;
            }

            String countsFieldName = fieldName + COUNT_FIELD_SUFFIX;
            DocValuesSkipper countsSkipper = context.reader().getDocValuesSkipper(countsFieldName);
            assert countsSkipper != null : "no skipper for counts field [" + countsFieldName + "]";
            if (countsSkipper.maxValue() == 1) {
                release = 0;
                return new SingleValued(breaker, values);
            }

            breaker.addEstimateBytesAndMaybeBreak(AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE, "load blocks");
            release += AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE;
            NumericDocValues counts = context.reader().getNumericDocValues(countsFieldName);
            release = 0;
            return new MultiValuedBinaryWithSeparateCounts(breaker, warnings, counts, values);
        } finally {
            breaker.addWithoutBreaking(-release);
        }
    }

    private static final class SingleValued extends BlockDocValuesReader {
        private final BinaryDocValues docValues;

        SingleValued(CircuitBreaker breaker, BinaryDocValues docValues) {
            super(breaker);
            this.docValues = docValues;
        }

        @Override
        public int docId() {
            return docValues.docID();
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (IntBuilder) builder);
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docValues instanceof BlockLoader.OptionalLengthReader direct) {
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
            if (false == docValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            BytesRef bytes = docValues.binaryValue();
            builder.appendInt(bytes.length);
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-BytesRefsFromBinaryBlockLoader.ESTIMATED_SIZE);
        }

        @Override
        public String toString() {
            return "ByteLengthFromBytesRef.SingleValued";
        }
    }

    private static final class MultiValuedBinaryWithSeparateCounts extends MultiValuedBinaryWithSeparateCountsLengthReader {

        MultiValuedBinaryWithSeparateCounts(CircuitBreaker breaker, Warnings warnings, NumericDocValues counts, BinaryDocValues values) {
            super(breaker, warnings, counts, values);
        }

        @Override
        int length(BytesRef bytesRef) {
            return bytesRef.length;
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(
                -(AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE + BytesRefsFromBinaryBlockLoader.ESTIMATED_SIZE)
            );
        }

        @Override
        public String toString() {
            return "ByteLengthFromBytesRef.MultiValuedBinaryWithSeparateCounts";
        }
    }
}
