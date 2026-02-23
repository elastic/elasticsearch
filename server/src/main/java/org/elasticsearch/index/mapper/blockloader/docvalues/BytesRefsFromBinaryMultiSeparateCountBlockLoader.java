/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;

import java.io.IOException;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * Block loader for multi-value binary fields which store count in a separate parallel numeric doc value column.
 */
public class BytesRefsFromBinaryMultiSeparateCountBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;

    public BytesRefsFromBinaryMultiSeparateCountBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
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
            if (countsSkipper.minValue() == 1 && countsSkipper.maxValue() == 1) {
                release = 0;
                return new BytesRefsFromBinaryBlockLoader.BytesRefsFromBinary(
                    breaker,
                    BytesRefsFromBinaryBlockLoader.ESTIMATED_SIZE,
                    values
                );
            }

            breaker.addWithoutBreaking(AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE);
            release += AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE;
            NumericDocValues counts = context.reader().getNumericDocValues(countsFieldName);
            release = 0;
            return new BytesRefsFromBinarySeparateCount(breaker, values, counts);
        } finally {
            breaker.addWithoutBreaking(-release);
        }
    }

    static class BytesRefsFromBinarySeparateCount extends BytesRefsFromCustomBinaryBlockLoader.AbstractBytesRefsFromBinary {
        private final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();
        private final NumericDocValues counts;

        BytesRefsFromBinarySeparateCount(CircuitBreaker breaker, BinaryDocValues docValues, NumericDocValues counts) {
            super(breaker, docValues);
            this.counts = counts;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            return super.read(factory, docs, offset, nullsFiltered);
        }

        @Override
        public void read(int doc, BytesRefBuilder builder) throws IOException {
            if (false == docValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }

            boolean advanced = counts.advanceExact(doc);
            assert advanced;

            reader.read(docValues.binaryValue(), counts.longValue(), builder);
        }

        @Override
        public String toString() {
            return "BytesRefsFromBinarySeparateCount";
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(
                -(AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE + BytesRefsFromBinaryBlockLoader.ESTIMATED_SIZE)
            );
        }
    }
}
