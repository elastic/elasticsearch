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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromCustomBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueSeparateCountBinaryDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.BinaryAndCounts;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;

import java.io.IOException;

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
        BinaryAndCounts bc = BinaryAndCounts.get(breaker, context, fieldName, true);
        if (bc == null) {
            return ConstantNull.READER;
        }
        if (bc.counts() == null) {
            return new BytesRefsFromBinaryBlockLoader.BytesRefsFromBinary(bc.binary());
        }
        return new MinFromBinarySeparateCount(bc.binary(), bc.counts());
    }

    @Override
    public String toString() {
        return "MvMinBytesRefsFromBinary[" + fieldName + "]";
    }

    private static class MinFromBinarySeparateCount extends BytesRefsFromCustomBinaryBlockLoader.AbstractBytesRefsFromBinary {
        private final TrackingNumericDocValues counts;
        private final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

        MinFromBinarySeparateCount(TrackingBinaryDocValues docValues, TrackingNumericDocValues counts) {
            super(docValues);
            this.counts = counts;
        }

        @Override
        public void read(int doc, BytesRefBuilder builder) throws IOException {
            if (false == counts.docValues().advanceExact(doc)) {
                builder.appendNull();
                return;
            }

            boolean advanced = docValues.docValues().advanceExact(doc);
            assert advanced;

            int count = (int) counts.docValues().longValue();
            reader.readMin(docValues.docValues().binaryValue(), count, builder);
        }

        @Override
        public void close() {
            Releasables.close(super::close, counts);
        }

        @Override
        public String toString() {
            return "MvMinBytesRefsFromBinary.SeparateCount";
        }
    }
}
