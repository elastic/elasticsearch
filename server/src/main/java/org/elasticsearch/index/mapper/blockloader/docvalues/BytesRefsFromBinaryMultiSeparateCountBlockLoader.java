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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

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
    public AllReader reader(LeafReaderContext context) throws IOException {
        BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
        NumericDocValues counts = context.reader().getNumericDocValues(fieldName + ".counts");
        return createReader(values, counts);
    }

    public static AllReader createReader(@Nullable BinaryDocValues docValues, @Nullable NumericDocValues numericDocValues) {
        if (docValues == null) {
            return new ConstantNullsReader();
        }
        if (numericDocValues == null) {
            return new BytesRefsFromCustomBinaryBlockLoader.BytesRefsFromCustomBinary(docValues);
        }
        return new BytesRefsFromBinarySeparateCount(docValues, numericDocValues);
    }

    static class BytesRefsFromBinarySeparateCount extends BytesRefsFromCustomBinaryBlockLoader.AbstractBytesRefsFromBinary {
        private final MultiValueSeparateCountBinaryDocValuesReader reader = new MultiValueSeparateCountBinaryDocValuesReader();
        private final NumericDocValues counts;

        BytesRefsFromBinarySeparateCount(BinaryDocValues docValues, NumericDocValues counts) {
            super(docValues);
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

            assert counts.advanceExact(doc);
            int count = Math.toIntExact(counts.longValue());

            BytesRef bytes = docValues.binaryValue();
            reader.read(bytes, count, builder);
        }

        @Override
        public String toString() {
            return "BytesRefsFromBinarySeparateCount";
        }
    }
}
