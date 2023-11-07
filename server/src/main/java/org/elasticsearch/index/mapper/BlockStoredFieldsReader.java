/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BlockLoader.BytesRefBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Loads values from {@link LeafReader#storedFields}. This whole process is very slow
 * and cast-tastic, so it doesn't really try to avoid megamorphic invocations. It's
 * just going to be slow.
 *
 * Note that this extends {@link BlockDocValuesReader} because it pretends to load
 * doc values because, for now, ESQL only knows how to load things in a doc values
 * order.
 */
public abstract class BlockStoredFieldsReader extends BlockDocValuesReader {
    public static BlockLoader bytesRefsFromBytesRefs(String field) {
        StoredFieldLoader loader = StoredFieldLoader.create(false, Set.of(field));
        return context -> new Bytes(loader.getLoader(context, null), field) {
            @Override
            protected BytesRef toBytesRef(Object v) {
                return (BytesRef) v;
            }
        };
    }

    public static BlockLoader bytesRefsFromStrings(String field) {
        StoredFieldLoader loader = StoredFieldLoader.create(false, Set.of(field));
        return context -> new Bytes(loader.getLoader(context, null), field) {
            private final BytesRef scratch = new BytesRef();

            @Override
            protected BytesRef toBytesRef(Object v) {
                return toBytesRef(scratch, (String) v);
            }
        };
    }

    public static BlockLoader id() {
        StoredFieldLoader loader = StoredFieldLoader.create(false, Set.of(IdFieldMapper.NAME));
        return context -> new Id(loader.getLoader(context, null));
    }

    private final LeafStoredFieldLoader loader;
    private int docID = -1;

    protected BlockStoredFieldsReader(LeafStoredFieldLoader loader) {
        this.loader = loader;
    }

    @Override
    public final BlockLoader.Block readValues(BlockLoader.BuilderFactory factory, BlockLoader.Docs docs) throws IOException {
        try (BlockLoader.Builder builder = builder(factory, docs.count())) {
            for (int i = 0; i < docs.count(); i++) {
                readValuesFromSingleDoc(docs.get(i), builder);
            }
            return builder.build();
        }
    }

    @Override
    public final void readValuesFromSingleDoc(int docId, BlockLoader.Builder builder) throws IOException {
        if (docId < this.docID) {
            throw new IllegalStateException("docs within same block must be in order");
        }
        this.docID = docId;
        loader.advanceTo(docId);
        read(loader, builder);
    }

    protected abstract void read(LeafStoredFieldLoader loader, BlockLoader.Builder builder) throws IOException;

    @Override
    public final int docID() {
        return docID;
    }

    private abstract static class Bytes extends BlockStoredFieldsReader {
        private final String field;

        Bytes(LeafStoredFieldLoader loader, String field) {
            super(loader);
            this.field = field;
        }

        @Override
        public BytesRefBuilder builder(BlockLoader.BuilderFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        protected abstract BytesRef toBytesRef(Object v);

        @Override
        protected void read(LeafStoredFieldLoader loader, BlockLoader.Builder builder) throws IOException {
            List<Object> values = loader.storedFields().get(field);
            if (values == null) {
                builder.appendNull();
                return;
            }
            if (values.size() == 1) {
                ((BytesRefBuilder) builder).appendBytesRef(toBytesRef(values.get(0)));
                return;
            }
            builder.beginPositionEntry();
            for (Object v : values) {
                ((BytesRefBuilder) builder).appendBytesRef(toBytesRef(v));
            }
            builder.endPositionEntry();
        }

        @Override
        public String toString() {
            return "BlockStoredFieldsReader.Bytes";
        }
    }

    private static class Id extends BlockStoredFieldsReader {
        private final BytesRef scratch = new BytesRef();

        Id(LeafStoredFieldLoader loader) {
            super(loader);
        }

        @Override
        public BlockLoader.BytesRefBuilder builder(BlockLoader.BuilderFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        protected void read(LeafStoredFieldLoader loader, BlockLoader.Builder builder) throws IOException {
            ((BytesRefBuilder) builder).appendBytesRef(toBytesRef(scratch, loader.id()));
        }

        @Override
        public String toString() {
            return "BlockStoredFieldsReader.Id";
        }
    }
}
