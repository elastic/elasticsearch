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
import org.elasticsearch.index.mapper.blockloader.docvalues.AbstractBytesRefsFromBinaryReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader.ArrayOrderSource;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueArrayOrderInlineNullBinaryDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueSeparateCountBinaryDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.BinaryAndCounts;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;

import java.io.IOException;
import java.util.Objects;

public class MvMaxBytesRefsFromBinaryBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final String fieldName;
    private final ArrayOrderSource arrayOrderSource;

    public MvMaxBytesRefsFromBinaryBlockLoader(String fieldName) {
        this(fieldName, ArrayOrderSource.NONE);
    }

    public MvMaxBytesRefsFromBinaryBlockLoader(String fieldName, ArrayOrderSource arrayOrderSource) {
        this.fieldName = fieldName;
        this.arrayOrderSource = arrayOrderSource;
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
        if (arrayOrderSource == ArrayOrderSource.INLINE) {
            return new MvMaxBytesRefsFromArrayOrderInlineNull(bc.binary(), bc.counts());
        }
        return new MvMaxBytesRefsFromBinarySeparateCount(bc.binary(), bc.counts());
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    private static class MvMaxBytesRefsFromBinarySeparateCount extends AbstractBytesRefsFromBinaryReader {
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

    private static class MvMaxBytesRefsFromArrayOrderInlineNull extends AbstractBytesRefsFromBinaryReader {
        private final TrackingNumericDocValues counts;
        private final MultiValueArrayOrderInlineNullBinaryDocValuesReader reader =
            new MultiValueArrayOrderInlineNullBinaryDocValuesReader();

        MvMaxBytesRefsFromArrayOrderInlineNull(TrackingBinaryDocValues values, TrackingNumericDocValues counts) {
            super(values);
            this.counts = Objects.requireNonNull(counts);
        }

        @Override
        public int docId() {
            return counts.docValues().docID();
        }

        @Override
        public void read(int doc, BytesRefBuilder builder) throws IOException {
            // Counts are always written, so a lack of value here indicates there is no value to record
            if (false == counts.docValues().advanceExact(doc)) {
                builder.appendNull();
                return;
            }

            // all-null array / lone null / empty array: a count is present but no binary blob is written
            if (false == docValues.docValues().advanceExact(doc)) {
                builder.appendNull();
                return;
            }

            int count = (int) counts.docValues().longValue();
            reader.readMax(docValues.docValues().binaryValue(), count, builder);
        }

        @Override
        public void close() {
            Releasables.close(super::close, counts);
        }

        @Override
        public String toString() {
            return "MvMaxBytesRefsFromBinary.ArrayOrderInlineNull";
        }
    }
}
