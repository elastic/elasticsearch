/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.elasticsearch.index.mapper.blockloader.docvalues.AbstractLongsFromDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;

/**
 * Loads the MIN {@code long} in each doc.
 */
public class MvMinLongsFromDocValuesBlockLoader extends AbstractLongsFromDocValuesBlockLoader {
    public MvMinLongsFromDocValuesBlockLoader(String fieldName) {
        super(fieldName);
    }

    @Override
    protected AllReader singletonReader(TrackingNumericDocValues docValues) {
        return new Singleton(docValues);
    }

    @Override
    protected AllReader sortedReader(TrackingSortedNumericDocValues docValues) {
        return new MvMinSorted(docValues);
    }

    @Override
    public String toString() {
        return "LongsFromDocValues[" + fieldName + "]";
    }

    private static class MvMinSorted extends BlockDocValuesReader {
        private final TrackingSortedNumericDocValues numericDocValues;

        MvMinSorted(TrackingSortedNumericDocValues numericDocValues) {
            super(null);
            this.numericDocValues = numericDocValues;
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (LongBuilder builder = factory.longsFromDocValues(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (LongBuilder) builder);
        }

        private void read(int doc, LongBuilder builder) throws IOException {
            if (false == numericDocValues.docValues().advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            builder.appendLong(numericDocValues.docValues().nextValue());
        }

        @Override
        public int docId() {
            return numericDocValues.docValues().docID();
        }

        @Override
        public String toString() {
            return "MvMinLongsFromDocValues.Sorted";
        }

        @Override
        public void close() {
            numericDocValues.close();
        }
    }
}
