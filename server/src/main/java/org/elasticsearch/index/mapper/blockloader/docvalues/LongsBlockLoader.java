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

public class LongsBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;

    public LongsBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.longs(expectedCount);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        SortedNumericDocValues docValues = context.reader().getSortedNumericDocValues(fieldName);
        if (docValues != null) {
            NumericDocValues singleton = DocValues.unwrapSingleton(docValues);
            if (singleton != null) {
                return new SingletonLongs(singleton);
            }
            return new Longs(docValues);
        }
        NumericDocValues singleton = context.reader().getNumericDocValues(fieldName);
        if (singleton != null) {
            return new SingletonLongs(singleton);
        }
        return new ConstantNullsReader();
    }

    public static class SingletonLongs extends BlockDocValuesReader implements BlockDocValuesReader.NumericDocValuesAccessor {
        final NumericDocValues numericDocValues;

        SingletonLongs(NumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (numericDocValues instanceof BlockLoader.OptionalColumnAtATimeReader direct) {
                BlockLoader.Block result = direct.tryRead(factory, docs, offset, nullsFiltered, null, false);
                if (result != null) {
                    return result;
                }
            }
            try (BlockLoader.LongBuilder builder = factory.longsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (numericDocValues.advanceExact(doc)) {
                        builder.appendLong(numericDocValues.longValue());
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            BlockLoader.LongBuilder blockBuilder = (BlockLoader.LongBuilder) builder;
            if (numericDocValues.advanceExact(docId)) {
                blockBuilder.appendLong(numericDocValues.longValue());
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
            return "BlockDocValuesReader.SingletonLongs";
        }

        @Override
        public NumericDocValues numericDocValues() {
            return numericDocValues;
        }
    }

    public static class Longs extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;

        Longs(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.LongBuilder builder = factory.longsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (LongBuilder) builder);
        }

        private void read(int doc, LongBuilder builder) throws IOException {
            if (false == numericDocValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            int count = numericDocValues.docValueCount();
            if (count == 1) {
                builder.appendLong(numericDocValues.nextValue());
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                builder.appendLong(numericDocValues.nextValue());
            }
            builder.endPositionEntry();
        }

        @Override
        public int docId() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.Longs";
        }
    }
}
