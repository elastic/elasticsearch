/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Loads values from {@code _source}. This whole process is very slow and cast-tastic,
 * so it doesn't really try to avoid megamorphic invocations. It's just going to be
 * slow.
 *
 * Note that this extends {@link BlockDocValuesReader} because it pretends to load
 * doc values because, for now, ESQL only knows how to load things in a doc values
 * order.
 */
public abstract class BlockSourceReader extends BlockDocValuesReader {
    /**
     * Read {@code boolean}s from {@code _source}.
     */
    public static BlockLoader booleans(ValueFetcher fetcher) {
        StoredFieldLoader loader = StoredFieldLoader.create(true, Set.of());
        return context -> new BlockSourceReader(fetcher, loader.getLoader(context, null)) {
            @Override
            public BlockLoader.Builder builder(BlockLoader.BuilderFactory factory, int expectedCount) {
                return factory.booleans(expectedCount);
            }

            @Override
            protected void append(BlockLoader.Builder builder, Object v) {
                ((BlockLoader.BooleanBuilder) builder).appendBoolean((Boolean) v);
            }

            @Override
            public String toString() {
                return "SourceBooleans";
            }
        };
    }

    /**
     * Read {@link BytesRef}s from {@code _source}.
     */
    public static BlockLoader bytesRefs(ValueFetcher fetcher) {
        StoredFieldLoader loader = StoredFieldLoader.create(true, Set.of());
        return context -> new BlockSourceReader(fetcher, loader.getLoader(context, null)) {
            BytesRef scratch = new BytesRef();

            @Override
            public BlockLoader.Builder builder(BlockLoader.BuilderFactory factory, int expectedCount) {
                return factory.bytesRefs(expectedCount);
            }

            @Override
            protected void append(BlockLoader.Builder builder, Object v) {
                ((BlockLoader.BytesRefBuilder) builder).appendBytesRef(toBytesRef(scratch, (String) v));
            }

            @Override
            public String toString() {
                return "SourceBytes";
            }
        };
    }

    /**
     * Read {@code double}s from {@code _source}.
     */
    public static BlockLoader doubles(ValueFetcher fetcher) {
        StoredFieldLoader loader = StoredFieldLoader.create(true, Set.of());
        return context -> new BlockSourceReader(fetcher, loader.getLoader(context, null)) {
            @Override
            public BlockLoader.Builder builder(BlockLoader.BuilderFactory factory, int expectedCount) {
                return factory.doubles(expectedCount);
            }

            @Override
            protected void append(BlockLoader.Builder builder, Object v) {
                ((BlockLoader.DoubleBuilder) builder).appendDouble(((Number) v).doubleValue());
            }

            @Override
            public String toString() {
                return "SourceDoubles";
            }
        };
    }

    /**
     * Read {@code int}s from {@code _source}.
     */
    public static BlockLoader ints(ValueFetcher fetcher) {
        StoredFieldLoader loader = StoredFieldLoader.create(true, Set.of());
        return context -> new BlockSourceReader(fetcher, loader.getLoader(context, null)) {
            @Override
            public BlockLoader.Builder builder(BlockLoader.BuilderFactory factory, int expectedCount) {
                return factory.ints(expectedCount);
            }

            @Override
            protected void append(BlockLoader.Builder builder, Object v) {
                ((BlockLoader.IntBuilder) builder).appendInt(((Number) v).intValue());
            }

            @Override
            public String toString() {
                return "SourceInts";
            }
        };
    }

    /**
     * Read {@code long}s from {@code _source}.
     */
    public static BlockLoader longs(ValueFetcher fetcher) {
        StoredFieldLoader loader = StoredFieldLoader.create(true, Set.of());
        return context -> new BlockSourceReader(fetcher, loader.getLoader(context, null)) {
            @Override
            public BlockLoader.Builder builder(BlockLoader.BuilderFactory factory, int expectedCount) {
                return factory.longs(expectedCount);
            }

            @Override
            protected void append(BlockLoader.Builder builder, Object v) {
                ((BlockLoader.LongBuilder) builder).appendLong(((Number) v).longValue());
            }

            @Override
            public String toString() {
                return "SourceLongs";
            }
        };
    }

    private final ValueFetcher fetcher;
    private final LeafStoredFieldLoader loader;
    private final List<Object> ignoredValues = new ArrayList<>();
    private int docID = -1;

    BlockSourceReader(ValueFetcher fetcher, LeafStoredFieldLoader loader) {
        this.fetcher = fetcher;
        this.loader = loader;
    }

    @Override
    public BlockLoader.Block readValues(BlockLoader.BuilderFactory factory, BlockLoader.Docs docs) throws IOException {
        try (BlockLoader.Builder builder = builder(factory, docs.count())) {
            for (int i = 0; i < docs.count(); i++) {
                int doc = docs.get(i);
                if (doc < this.docID) {
                    throw new IllegalStateException("docs within same block must be in order");
                }
                readValuesFromSingleDoc(doc, builder);
            }
            return builder.build();
        }
    }

    @Override
    public void readValuesFromSingleDoc(int doc, BlockLoader.Builder builder) throws IOException {
        this.docID = doc;
        loader.advanceTo(doc);
        List<Object> values = fetcher.fetchValues(Source.fromBytes(loader.source()), doc, ignoredValues);
        ignoredValues.clear();  // TODO do something with these?
        if (values == null) {
            builder.appendNull();
            return;
        }
        if (values.size() == 1) {
            append(builder, values.get(0));
            return;
        }
        builder.beginPositionEntry();
        for (Object v : values) {
            append(builder, v);
        }
        builder.endPositionEntry();
    }

    protected abstract void append(BlockLoader.Builder builder, Object v);

    @Override
    public int docID() {
        return docID;
    }
}
