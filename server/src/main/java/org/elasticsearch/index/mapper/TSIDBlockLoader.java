/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.codec.tsdb.es819.BlockAwareSortedDocValues;
import org.elasticsearch.index.codec.tsdb.es819.SingletonDocValuesBlockLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

public final class TSIDBlockLoader implements BlockLoader {

    private static final String FIELD_NAME = TimeSeriesIdFieldMapper.NAME;

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) throws IOException {
        var singleton = context.reader().getSortedDocValues(FIELD_NAME);
        return new TSIDs((BlockAwareSortedDocValues) singleton);
    }

    @Override
    public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
        var singleton = context.reader().getSortedDocValues(FIELD_NAME);
        return new BlockDocValuesReader.SingletonOrdinals(singleton);
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return StoredFieldsSpec.NO_REQUIREMENTS;
    }

    @Override
    public boolean supportsOrdinals() {
        return true;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
        return DocValues.getSortedSet(context.reader(), FIELD_NAME);
    }

    public static final class TSIDs implements ColumnAtATimeReader {
        private final Thread creationThread;
        private final SortedDocValues sorted;
        private final SingletonDocValuesBlockLoader blockLoader;

        TSIDs(BlockAwareSortedDocValues sorted) {
            this.creationThread = Thread.currentThread();
            this.sorted = sorted;
            this.blockLoader = sorted.getSingletonBlockLoader();
        }

        @Override
        public Block read(BlockFactory factory, Docs docs, int offset) throws IOException {
            try (TSIDOrdinalsBuilder builder = factory.tsidOrdinalsBuilder(sorted, docs.count() - offset)) {
                blockLoader.loadBlock(builder, docs, offset);
                return builder.build();
            }
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return creationThread == Thread.currentThread() && blockLoader.docID() <= startingDocID;
        }

        @Override
        public String toString() {
            return "TSIDBlockLoader.TSIDs";
        }
    }
}
