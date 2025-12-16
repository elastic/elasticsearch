/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;

public class BooleansBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;

    public BooleansBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public BooleanBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.booleans(expectedCount);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        SortedNumericDocValues docValues = context.reader().getSortedNumericDocValues(fieldName);
        if (docValues != null) {
            NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return new SingletonBooleans(singleton);
            }
            return new Booleans(docValues);
        }
        NumericDocValues singleton = context.reader().getNumericDocValues(fieldName);
        if (singleton != null) {
            return new SingletonBooleans(singleton);
        }
        return new ConstantNullsReader();
    }

    private static class SingletonBooleans extends BlockDocValuesReader {
        private final NumericDocValues numericDocValues;

        SingletonBooleans(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.BooleanBuilder builder = factory.booleansFromDocValues(docs.count() - offset)) {
                int lastDoc = -1;
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (doc < lastDoc) {
                        throw new IllegalStateException("docs within same block must be in order");
                    }
                    if (numericDocValues.advanceExact(doc)) {
                        builder.appendBoolean(numericDocValues.longValue() != 0);
                    } else {
                        builder.appendNull();
                    }
                    lastDoc = doc;
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            BooleanBuilder blockBuilder = (BooleanBuilder) builder;
            if (numericDocValues.advanceExact(docId)) {
                blockBuilder.appendBoolean(numericDocValues.longValue() != 0);
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docId() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.SingletonBooleans";
        }
    }

    private static class Booleans extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;

        Booleans(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.BooleanBuilder builder = factory.booleansFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BooleanBuilder) builder);
        }

        private void read(int doc, BooleanBuilder builder) throws IOException {
            if (false == numericDocValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = numericDocValues.docValueCount();
            if (count == 1) {
                builder.appendBoolean(numericDocValues.nextValue() != 0);
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendBoolean(numericDocValues.nextValue() != 0);
            }
            builder.endPositionEntry();
        }

        @Override
        public int docId() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.Booleans";
        }
    }
}
