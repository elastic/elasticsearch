/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.AbstractDoublesFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;

/**
 * Loads the MIN {@code double} in each doc.
 */
public class MvMinDoublesFromDocValuesBlockLoader extends AbstractDoublesFromDocValuesBlockLoader {
    public MvMinDoublesFromDocValuesBlockLoader(String fieldName, BlockDocValuesReader.ToDouble toDouble) {
        super(fieldName, toDouble);
    }

    @Override
    protected AllReader singletonReader(NumericDocValues docValues, BlockDocValuesReader.ToDouble toDouble) {
        return new Singleton(docValues, toDouble);
    }

    @Override
    protected AllReader sortedReader(SortedNumericDocValues docValues, BlockDocValuesReader.ToDouble toDouble) {
        return new MvMaxSorted(docValues, toDouble);
    }

    @Override
    public String toString() {
        return "DoublesFromDocValues[" + fieldName + "]";
    }

    private static class MvMaxSorted extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;
        private final ToDouble toDouble;

        MvMaxSorted(SortedNumericDocValues numericDocValues, ToDouble toDouble) {
            this.numericDocValues = numericDocValues;
            this.toDouble = toDouble;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (DoubleBuilder builder = factory.doublesFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (DoubleBuilder) builder);
        }

        private void read(int doc, DoubleBuilder builder) throws IOException {
            if (false == numericDocValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            builder.appendDouble(toDouble.convert(numericDocValues.nextValue()));
        }

        @Override
        public int docId() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "MvMinDoublesFromDocValues.Sorted";
        }
    }
}
