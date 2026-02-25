/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;

public class KeyedFlattenedDocValuesBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String keyedFieldName;
    private final String key;
    private final boolean usesBinaryDocValues;

    public KeyedFlattenedDocValuesBlockLoader(String keyedFieldName, String key, boolean usesBinaryDocValues) {
        this.keyedFieldName = keyedFieldName;
        this.key = key;
        this.usesBinaryDocValues = usesBinaryDocValues;
    }

    @Override
    public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        if (usesBinaryDocValues) {
            MultiValuedSortedBinaryDocValues dv = MultiValuedSortedBinaryDocValues.from(context.reader(), keyedFieldName);
            SortedBinaryDocValues filtered = BinaryKeyedFlattenedLeafFieldData.getKeyFilteredSortedBinaryDocValues(dv, key);
            return new BinaryKeyedBlockDocValuesReader(breaker, filtered);
        } else {
            SortedSetDocValues dv = DocValues.getSortedSet(context.reader(), keyedFieldName);
            if (dv.getValueCount() == 0) {
                return ConstantNull.READER;
            }
            SortedSetDocValues filtered = KeyedFlattenedLeafFieldData.getKeyFilteredSortedSetDocValues(dv, key);
            return new SortedSetKeyedBlockDocValuesReader(breaker, filtered);
        }
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return null;
    }

    private abstract static class KeyedBlockDocValuesReader extends BlockDocValuesReader {
        KeyedBlockDocValuesReader(CircuitBreaker breaker) {
            super(breaker);
        }

        protected abstract void read(int docId, BlockLoader.BytesRefBuilder builder) throws IOException;

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    read(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BlockLoader.BytesRefBuilder) builder);
        }

        @Override
        public void close() {}
    }

    private static final class BinaryKeyedBlockDocValuesReader extends KeyedBlockDocValuesReader {
        private final SortedBinaryDocValues filteredDocValues;
        private int curDocId = -1;

        BinaryKeyedBlockDocValuesReader(CircuitBreaker circuitBreaker, SortedBinaryDocValues filteredDocValues) {
            super(circuitBreaker);

            this.filteredDocValues = filteredDocValues;
        }

        @Override
        protected int docId() {
            return curDocId;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }

        @Override
        protected void read(int docId, BytesRefBuilder builder) throws IOException {
            curDocId = docId;
            if (filteredDocValues.advanceExact(docId) == false) {
                builder.appendNull();
                return;
            }

            int count = filteredDocValues.docValueCount();
            if (count == 1) {
                builder.appendBytesRef(filteredDocValues.nextValue());
                return;
            }

            builder.beginPositionEntry();
            for (int i = 0; i < count; i++) {
                builder.appendBytesRef(filteredDocValues.nextValue());
            }
            builder.endPositionEntry();
        }
    }

    private static final class SortedSetKeyedBlockDocValuesReader extends KeyedBlockDocValuesReader {
        private final SortedSetDocValues filteredDocValues;

        SortedSetKeyedBlockDocValuesReader(CircuitBreaker circuitBreaker, SortedSetDocValues filteredDocValues) {
            super(circuitBreaker);
            this.filteredDocValues = filteredDocValues;
        }

        @Override
        protected int docId() {
            return filteredDocValues.docID();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }

        @Override
        protected void read(int docId, BytesRefBuilder builder) throws IOException {
            if (filteredDocValues.advanceExact(docId) == false) {
                builder.appendNull();
                return;
            }

            int count = filteredDocValues.docValueCount();
            if (count == 1) {
                builder.appendBytesRef(filteredDocValues.lookupOrd(filteredDocValues.nextOrd()));
                return;
            }

            builder.beginPositionEntry();
            for (int i = 0; i < count; i++) {
                builder.appendBytesRef(filteredDocValues.lookupOrd(filteredDocValues.nextOrd()));
            }
            builder.endPositionEntry();
        }
    }
}
