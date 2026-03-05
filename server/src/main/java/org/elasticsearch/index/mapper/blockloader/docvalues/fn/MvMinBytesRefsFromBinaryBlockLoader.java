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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.AbstractLongsFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromCustomBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueSeparateCountBinaryDocValuesReader;

import java.io.IOException;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * Loads the MIN {@code keyword} in each doc from high-cardinality binary doc values.
 */
public class MvMinBytesRefsFromBinaryBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;

    public MvMinBytesRefsFromBinaryBlockLoader(String fieldName) {
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
            if (countsSkipper.maxValue() == 1) {
                release = 0;
                return new BytesRefsFromBinaryBlockLoader.BytesRefsFromBinary(
                    breaker,
                    BytesRefsFromBinaryBlockLoader.ESTIMATED_SIZE,
                    values
                );
            }

            breaker.addEstimateBytesAndMaybeBreak(AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE, "load blocks");
            release += AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE;
            NumericDocValues counts = context.reader().getNumericDocValues(countsFieldName);
            release = 0;
            return new MinFromBinarySeparateCount(breaker, values, counts);
        } finally {
            breaker.addWithoutBreaking(-release);
        }
    }

    @Override
    public String toString() {
        return "MvMinBytesRefsFromBinary[" + fieldName + "]";
    }

    private static class MinFromBinarySeparateCount extends BytesRefsFromCustomBinaryBlockLoader.AbstractBytesRefsFromBinary {
        private final NumericDocValues counts;
        private final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

        MinFromBinarySeparateCount(CircuitBreaker breaker, BinaryDocValues docValues, NumericDocValues counts) {
            super(breaker, docValues);
            this.counts = counts;
        }

        @Override
        public void read(int doc, BytesRefBuilder builder) throws IOException {
            if (false == counts.advanceExact(doc)) {
                builder.appendNull();
                return;
            }

            boolean advanced = docValues.advanceExact(doc);
            assert advanced;

            int count = (int) counts.longValue();
            reader.readMin(docValues.binaryValue(), count, builder);
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(
                -(AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE + BytesRefsFromBinaryBlockLoader.ESTIMATED_SIZE)
            );
        }

        @Override
        public String toString() {
            return "MvMinBytesRefsFromBinary.SeparateCount";
        }
    }
}
