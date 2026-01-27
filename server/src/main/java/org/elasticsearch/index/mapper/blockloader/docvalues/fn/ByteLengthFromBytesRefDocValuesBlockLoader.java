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
import org.elasticsearch.index.codec.tsdb.es819.DirectLengthReader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * Loads byte length from BytesRef.
 */
public class ByteLengthFromBytesRefDocValuesBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    protected final String fieldName;

    public ByteLengthFromBytesRefDocValuesBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public final Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

//    @Override
//    public final AllReader reader(LeafReaderContext context) throws IOException {
//        BinaryDocValues docValues = context.reader().getBinaryDocValues(fieldName);
//        if (docValues == null) {
//            return ConstantNull.READER;
//        }
//        return new BytesRefsFromCustomBinary(docValues);
//    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
        if (values == null) {
            return ConstantNull.READER;
        }

        String countsFieldName = fieldName + COUNT_FIELD_SUFFIX;
        DocValuesSkipper countsSkipper = context.reader().getDocValuesSkipper(countsFieldName);
        assert countsSkipper != null : "no skipper for counts field [" + countsFieldName + "]";
        if (countsSkipper.minValue() == 1 && countsSkipper.maxValue() == 1) {
            return new ByteLengthFromSingleCountBlockLoader(values);
        }

        NumericDocValues counts = context.reader().getNumericDocValues(countsFieldName);
        throw new UnsupportedEncodingException();
    }

    static class ByteLengthFromSingleCountBlockLoader extends BlockDocValuesReader {
        protected final BinaryDocValues docValues;

        ByteLengthFromSingleCountBlockLoader(BinaryDocValues docValues) {
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
            if (docValues instanceof DirectLengthReader direct) {
                try (BlockLoader.IntBuilder builder = factory.intsFromDocValues(docs.count() - offset)) {
                    for (int i = offset; i < docs.count(); i++) {
                        int doc = docs.get(i);
                        if (false == docValues.advanceExact(doc)) {
                            builder.appendNull();
                        } else {
                            builder.appendInt(direct.getLength());
                        }
                    }
                    return builder.build();
                }
            }

            try (BlockLoader.IntBuilder builder = factory.intsFromDocValues(docs.count() - offset)) {
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
        public String toString() {
            return "BlockDocValuesReader.ByteLengthFromSingleCountBlockLoader";
        }
    }
}
