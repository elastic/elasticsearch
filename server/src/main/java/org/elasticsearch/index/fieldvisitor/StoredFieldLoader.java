/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generates a {@link LeafStoredFieldLoader} for a given lucene segment to load stored fields.
 */
public abstract class StoredFieldLoader {

    /**
     * Return a {@link LeafStoredFieldLoader} for the given segment and document set
     *
     * The loader will use an internal lucene merge reader if the document set is of
     * sufficient size and is contiguous.  Callers may pass {@code null} if the set
     * is not known up front or if the merge reader optimisation will not apply.
     */
    public abstract LeafStoredFieldLoader getLoader(LeafReaderContext ctx, int[] docs) throws IOException;

    /**
     * @return a list of fields that will be loaded for each document
     */
    public abstract List<String> fieldsToLoad();

    /**
     * Creates a new StoredFieldLoader using a StoredFieldsSpec
     */
    public static StoredFieldLoader fromSpec(StoredFieldsSpec spec) {
        if (spec.noRequirements()) {
            return StoredFieldLoader.empty();
        }
        return create(spec.requiresSource(), spec.requiredStoredFields());
    }

    public static StoredFieldLoader create(boolean loadSource, Set<String> fields) {
        return create(loadSource, fields, false);
    }

    /**
     * Creates a new StoredFieldLoader
     *
     * @param loadSource           indicates whether this loader should load the {@code _source} field.
     * @param fields               a set of additional fields that the loader should load.
     * @param forceSequentialReader if {@code true}, forces the use of a sequential leaf reader;
     *                              otherwise, uses the heuristic defined in {@link StoredFieldLoader#reader(LeafReaderContext, int[])}.
     */
    public static StoredFieldLoader create(boolean loadSource, Set<String> fields, boolean forceSequentialReader) {
        if (loadSource == false && fields.isEmpty()) {
            return StoredFieldLoader.empty();
        }
        List<String> fieldsToLoad = fieldsToLoad(loadSource, fields);
        return new StoredFieldLoader() {
            @Override
            public LeafStoredFieldLoader getLoader(LeafReaderContext ctx, int[] docs) throws IOException {
                return new ReaderStoredFieldLoader(forceSequentialReader ? sequentialReader(ctx) : reader(ctx, docs), loadSource, fields);
            }

            @Override
            public List<String> fieldsToLoad() {
                return fieldsToLoad;
            }
        };
    }

    /**
     * Creates a new StoredFieldLoader using a StoredFieldsSpec that is optimized
     * for loading documents in order.
     */
    public static StoredFieldLoader fromSpecSequential(StoredFieldsSpec spec) {
        if (spec.noRequirements()) {
            return StoredFieldLoader.empty();
        }
        List<String> fieldsToLoad = fieldsToLoad(spec.requiresSource(), spec.requiredStoredFields());
        return new StoredFieldLoader() {
            @Override
            public LeafStoredFieldLoader getLoader(LeafReaderContext ctx, int[] docs) throws IOException {
                return new ReaderStoredFieldLoader(sequentialReader(ctx), spec.requiresSource(), spec.requiredStoredFields());
            }

            @Override
            public List<String> fieldsToLoad() {
                return fieldsToLoad;
            }
        };
    }

    /**
     * Creates a StoredFieldLoader tuned for sequential reads of _source
     */
    public static StoredFieldLoader sequentialSource() {
        return new StoredFieldLoader() {
            @Override
            public LeafStoredFieldLoader getLoader(LeafReaderContext ctx, int[] docs) throws IOException {
                return new ReaderStoredFieldLoader(sequentialReader(ctx), true, Set.of());
            }

            @Override
            public List<String> fieldsToLoad() {
                return List.of();
            }
        };
    }

    /**
     * Creates a no-op StoredFieldLoader that will not load any fields from disk
     */
    public static StoredFieldLoader empty() {
        return new StoredFieldLoader() {
            @Override
            public LeafStoredFieldLoader getLoader(LeafReaderContext ctx, int[] docs) {
                return new EmptyStoredFieldLoader();
            }

            @Override
            public List<String> fieldsToLoad() {
                return List.of();
            }
        };
    }

    private static CheckedBiConsumer<Integer, FieldsVisitor, IOException> reader(LeafReaderContext ctx, int[] docs) throws IOException {
        LeafReader leafReader = ctx.reader();
        if (docs != null && docs.length > 10 && hasSequentialDocs(docs)) {
            return sequentialReader(ctx);
        }
        StoredFields storedFields = leafReader.storedFields();
        return storedFields::document;
    }

    private static CheckedBiConsumer<Integer, FieldsVisitor, IOException> sequentialReader(LeafReaderContext ctx) throws IOException {
        LeafReader leafReader = ctx.reader();
        if (leafReader instanceof SequentialStoredFieldsLeafReader lf) {
            return lf.getSequentialStoredFieldsReader()::document;
        }
        return leafReader.storedFields()::document;
    }

    private static List<String> fieldsToLoad(boolean loadSource, Set<String> fields) {
        Set<String> fieldsToLoad = new HashSet<>();
        fieldsToLoad.add("_id");
        fieldsToLoad.add("_routing");
        if (loadSource) {
            fieldsToLoad.add("_source");
        }
        fieldsToLoad.addAll(fields);
        return fieldsToLoad.stream().sorted().toList();
    }

    private static boolean hasSequentialDocs(int[] docs) {
        return docs.length > 0 && docs[docs.length - 1] - docs[0] == docs.length - 1;
    }

    private static class EmptyStoredFieldLoader implements LeafStoredFieldLoader {

        @Override
        public void advanceTo(int doc) throws IOException {}

        @Override
        public BytesReference source() {
            return null;
        }

        @Override
        public String id() {
            return null;
        }

        @Override
        public String routing() {
            return null;
        }

        @Override
        public Map<String, List<Object>> storedFields() {
            return Collections.emptyMap();
        }
    }

    private static class ReaderStoredFieldLoader implements LeafStoredFieldLoader {

        private final CheckedBiConsumer<Integer, FieldsVisitor, IOException> reader;
        private final CustomFieldsVisitor visitor;
        private int doc = -1;

        ReaderStoredFieldLoader(CheckedBiConsumer<Integer, FieldsVisitor, IOException> reader, boolean loadSource, Set<String> fields) {
            this.reader = reader;
            this.visitor = new CustomFieldsVisitor(fields, loadSource);
        }

        @Override
        public void advanceTo(int doc) throws IOException {
            if (doc != this.doc) {
                visitor.reset();
                reader.accept(doc, visitor);
                this.doc = doc;
            }
        }

        @Override
        public BytesReference source() {
            return visitor.source();
        }

        @Override
        public String id() {
            return visitor.id();
        }

        @Override
        public String routing() {
            return visitor.routing();
        }

        @Override
        public Map<String, List<Object>> storedFields() {
            return visitor.fields();
        }
    }

}
