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
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.MultiValueSeparateCountBinaryDocValuesReader;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

public class MvMaxBytesRefsFromBinaryBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final String fieldName;

    public MvMaxBytesRefsFromBinaryBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
        if (values != null) {
            String countsFieldName = fieldName + COUNT_FIELD_SUFFIX;
            NumericDocValues counts = context.reader().getNumericDocValues(countsFieldName);
            assert counts != null
                : "no counts field ["
                    + countsFieldName
                    + "], this block loader only works with ["
                    + MultiValuedBinaryDocValuesField.SeparateCount.class.getSimpleName()
                    + "]";
            DocValuesSkipper countsSkipper = context.reader().getDocValuesSkipper(countsFieldName);
            assert countsSkipper != null : "no skipper for counts field [" + countsFieldName + "]";
            if (countsSkipper.minValue() == 1 && countsSkipper.maxValue() == 1) {
                return new BytesRefsFromBinaryBlockLoader.BytesRefsFromBinary(values);
            }
            return new MvMaxBinary(values, counts);
        }
        return ConstantNull.READER;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    private static class MvMaxBinary extends BlockDocValuesReader {
        private final BinaryDocValues values;
        private final NumericDocValues counts;
        private final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();

        MvMaxBinary(BinaryDocValues values, NumericDocValues counts) {
            this.values = Objects.requireNonNull(values);
            this.counts = Objects.requireNonNull(counts);
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (var builder = factory.bytesRefs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int docId = docs.get(i);
                    read(docId, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BytesRefBuilder) builder);
        }

        private void read(int docId, BytesRefBuilder builder) throws IOException {
            if (false == counts.advanceExact(docId)) {
                builder.appendNull();
                return;
            }
            boolean advanced = values.advanceExact(docId);
            assert advanced;
            reader.readMax(values.binaryValue(), counts.longValue(), builder);
        }

        @Override
        public int docId() {
            return counts.docID();
        }

        @Override
        public String toString() {
            return "MvMaxBytesRefs.Binary";
        }
    }
}
