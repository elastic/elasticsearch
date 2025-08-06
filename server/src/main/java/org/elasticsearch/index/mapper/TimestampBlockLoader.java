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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.codec.tsdb.es819.BlockAwareNumericDocValues;
import org.elasticsearch.index.codec.tsdb.es819.SingletonLongDocValuesBlockLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

public final class TimestampBlockLoader implements BlockLoader {

    private static final String FIELD_NAME = "@timestamp";

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.longs(expectedCount);
    }

    @Override
    public ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) throws IOException {
        var singleton = getNumericDocValues(context);
        return new Timestamps((BlockAwareNumericDocValues) singleton);
    }

    private static NumericDocValues getNumericDocValues(LeafReaderContext context) throws IOException {
        var singleton = context.reader().getNumericDocValues(FIELD_NAME);
        if (singleton == null) {
            var docValues = context.reader().getSortedNumericDocValues(FIELD_NAME);
            singleton = DocValues.unwrapSingleton(docValues);
        }
        return singleton;
    }

    @Override
    public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
        return new BlockDocValuesReader.SingletonLongs(getNumericDocValues(context));
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return StoredFieldsSpec.NO_REQUIREMENTS;
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException();
    }

    public static final class Timestamps implements ColumnAtATimeReader {
        private final Thread creationThread;
        private final SingletonLongDocValuesBlockLoader blockLoader;

        Timestamps(BlockAwareNumericDocValues blockAware) {
            this.creationThread = Thread.currentThread();
            this.blockLoader = blockAware.getSingletonBlockLoader();
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset) throws IOException {
            try (BlockLoader.SingletonLongBuilder builder = factory.singletonLongs(docs.count() - offset)) {
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
            return "TimestampBlockLoader.Timestamps";
        }
    }
}
