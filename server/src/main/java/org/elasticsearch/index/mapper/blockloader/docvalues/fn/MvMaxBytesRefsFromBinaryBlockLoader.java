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
import java.util.Objects;

public class MvMaxBytesRefsFromBinaryBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final String fieldName;

    public MvMaxBytesRefsFromBinaryBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        BinaryAndCounts bc = BinaryAndCounts.get(breaker, context, fieldName, true);
        if (bc == null) {
            return ConstantNull.COLUMN_READER;
        }
        if (bc.counts() == null) {
            return new BytesRefsFromBinaryBlockLoader.BytesRefsFromBinary(bc.binary());
        }
        return new MvMaxBytesRefsFromBinarySeparateCount(bc.binary(), bc.counts());
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    private static class MvMaxBytesRefsFromBinarySeparateCount extends BytesRefsFromCustomBinaryBlockLoader.AbstractBytesRefsFromBinary {
        private final TrackingNumericDocValues counts;
        private final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

        MvMaxBytesRefsFromBinarySeparateCount(TrackingBinaryDocValues values, TrackingNumericDocValues counts) {
            super(values);
            this.counts = Objects.requireNonNull(counts);
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
            reader.readMax(docValues.docValues().binaryValue(), count, builder);
        }

        @Override
        public void close() {
            Releasables.close(super::close, counts);
        }

        @Override
        public String toString() {
            return "MvMaxBytesRefsFromBinary.SeparateCount";
        }
    }
}
