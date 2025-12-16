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

public class DoublesBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;
    private final BlockDocValuesReader.ToDouble toDouble;

    public DoublesBlockLoader(String fieldName, BlockDocValuesReader.ToDouble toDouble) {
        this.fieldName = fieldName;
        this.toDouble = toDouble;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.doubles(expectedCount);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        SortedNumericDocValues docValues = context.reader().getSortedNumericDocValues(fieldName);
        if (docValues != null) {
            NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return new SingletonDoubles(singleton, toDouble);
            }
            return new Doubles(docValues, toDouble);
        }
        NumericDocValues singleton = context.reader().getNumericDocValues(fieldName);
        if (singleton != null) {
            return new SingletonDoubles(singleton, toDouble);
        }
        return new ConstantNullsReader();
    }

    public static class SingletonDoubles extends BlockDocValuesReader implements BlockDocValuesReader.NumericDocValuesAccessor {
        private final NumericDocValues docValues;
        private final ToDouble toDouble;

        SingletonDoubles(NumericDocValues docValues, ToDouble toDouble) {
            this.docValues = docValues;
            this.toDouble = toDouble;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docValues instanceof BlockLoader.OptionalColumnAtATimeReader direct) {
                BlockLoader.Block result = direct.tryRead(factory, docs, offset, nullsFiltered, toDouble, false);
                if (result != null) {
                    return result;
                }
            }
            try (BlockLoader.DoubleBuilder builder = factory.doublesFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (docValues.advanceExact(doc)) {
                        builder.appendDouble(toDouble.convert(docValues.longValue()));
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            DoubleBuilder blockBuilder = (DoubleBuilder) builder;
            if (docValues.advanceExact(docId)) {
                blockBuilder.appendDouble(toDouble.convert(docValues.longValue()));
            } else {
                blockBuilder.appendNull();
            }
        }

        @Override
        public int docId() {
            return docValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.SingletonDoubles";
        }

        @Override
        public NumericDocValues numericDocValues() {
            return docValues;
        }
    }

    public static class Doubles extends BlockDocValuesReader {
        private final SortedNumericDocValues docValues;
        private final ToDouble toDouble;

        Doubles(SortedNumericDocValues docValues, ToDouble toDouble) {
            this.docValues = docValues;
            this.toDouble = toDouble;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.DoubleBuilder builder = factory.doublesFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (DoubleBuilder) builder);
        }

        private void read(int doc, DoubleBuilder builder) throws IOException {
            if (false == docValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = docValues.docValueCount();
            if (count == 1) {
                builder.appendDouble(toDouble.convert(docValues.nextValue()));
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendDouble(toDouble.convert(docValues.nextValue()));
            }
            builder.endPositionEntry();
        }

        @Override
        public int docId() {
            return docValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.Doubles";
        }
    }
}
