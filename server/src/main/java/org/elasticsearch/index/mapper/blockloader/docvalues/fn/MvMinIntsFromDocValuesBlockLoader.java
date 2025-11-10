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
import org.elasticsearch.index.mapper.blockloader.docvalues.AbstractIntsFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;

/**
 * Loads the MIN {@code int} in each doc.
 */
public class MvMinIntsFromDocValuesBlockLoader extends AbstractIntsFromDocValuesBlockLoader {
    public MvMinIntsFromDocValuesBlockLoader(String fieldName) {
        super(fieldName);
    }

    @Override
    protected AllReader singletonReader(NumericDocValues docValues) {
        return new Singleton(docValues);
    }

    @Override
    protected AllReader sortedReader(SortedNumericDocValues docValues) {
        return new MvMinSorted(docValues);
    }

    @Override
    public String toString() {
        return "IntsFromDocValues[" + fieldName + "]";
    }

    private static class MvMinSorted extends BlockDocValuesReader {
        private final SortedNumericDocValues numericDocValues;

        MvMinSorted(SortedNumericDocValues numericDocValues) {
            this.numericDocValues = numericDocValues;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (IntBuilder builder = factory.intsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (IntBuilder) builder);
        }

        private void read(int doc, IntBuilder builder) throws IOException {
            if (false == numericDocValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            builder.appendInt(Math.toIntExact(numericDocValues.nextValue()));
        }

        @Override
        public int docId() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "MvMinIntsFromDocValues.Sorted";
        }
    }
}
