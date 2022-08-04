/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Loads source {@code _source} during a GET or {@code _search}.
 */
public interface SourceLoader {
    /**
     * Does this {@link SourceLoader} reorder field values?
     */
    boolean reordersFieldValues();

    /**
     * Build the loader for some segment.
     */
    Leaf leaf(LeafReader reader, int[] docIdsInLeaf) throws IOException;

    /**
     * Stream containing all non-{@code _source} stored fields required
     * to build the {@code _source}.
     */
    Stream<String> requiredStoredFields();  // TODO return Set?

    /**
     * Loads {@code _source} from some segment.
     */
    interface Leaf {
        /**
         * Load the {@code _source} for a document.
         * @param fieldsVisitor field visitor populated with {@code _source} if it
         *                      has been saved
         * @param docId the doc to load
         */
        BytesReference source(FieldsVisitor fieldsVisitor, int docId) throws IOException;

        Leaf EMPTY_OBJECT = (fieldsVisitor, docId) -> {
            // TODO accept a requested xcontent type
            try (XContentBuilder b = new XContentBuilder(JsonXContent.jsonXContent, new ByteArrayOutputStream())) {
                return BytesReference.bytes(b.startObject().endObject());
            }
        };
    }

    /**
     * Load {@code _source} from a stored field.
     */
    SourceLoader FROM_STORED_SOURCE = new SourceLoader() {
        @Override
        public boolean reordersFieldValues() {
            return false;
        }

        @Override
        public Leaf leaf(LeafReader reader, int[] docIdsInLeaf) {
            return (fieldsVisitor, docId) -> fieldsVisitor.source();
        }

        @Override
        public Stream<String> requiredStoredFields() {
            return Stream.empty();
        }
    };

    /**
     * Load {@code _source} from doc vales.
     */
    class Synthetic implements SourceLoader {
        private final SyntheticFieldLoader loader;
        private final Map<String, SyntheticFieldLoader.StoredFieldLoader> storedFieldLoaders;

        public Synthetic(Mapping mapping) {
            loader = mapping.getRoot().syntheticFieldLoader();
            storedFieldLoaders = Map.copyOf(loader.storedFieldLoaders().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }

        @Override
        public boolean reordersFieldValues() {
            return true;
        }

        @Override
        public Stream<String> requiredStoredFields() {
            return storedFieldLoaders.keySet().stream();
        }

        @Override
        public Leaf leaf(LeafReader reader, int[] docIdsInLeaf) throws IOException {
            SyntheticFieldLoader.DocValuesLoader leaf = loader.docValuesLoader(reader, docIdsInLeaf);
            return (fieldsVisitor, docId) -> {
                if (fieldsVisitor != null) {
                    for (Map.Entry<String, List<Object>> e : fieldsVisitor.fields().entrySet()) {
                        SyntheticFieldLoader.StoredFieldLoader loader = storedFieldLoaders.get(e.getKey());
                        if (loader != null) {
                            loader.load(e.getValue());
                        }
                    }
                }
                // TODO accept a requested xcontent type
                try (XContentBuilder b = new XContentBuilder(JsonXContent.jsonXContent, new ByteArrayOutputStream())) {
                    if (leaf.advanceToDoc(docId)) {
                        loader.write(b);
                    } else {
                        b.startObject().endObject();
                    }
                    return BytesReference.bytes(b);
                }
            };
        }
    }

    /**
     * Load a field for {@link Synthetic}.
     */
    interface SyntheticFieldLoader {
        /**
         * Load no values.
         */
        SyntheticFieldLoader NOTHING = new SyntheticFieldLoader() {
            @Override
            public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
                return Stream.of();
            }

            @Override
            public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
                return null;
            }

            @Override
            public void write(XContentBuilder b) throws IOException {}
        };

        Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders();

        /**
         * Build a loader for this field in the provided segment.
         */
        DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException;

        /**
         * Write values for this document.
         */
        void write(XContentBuilder b) throws IOException;

        interface StoredFieldLoader {
            void load(List<Object> values);
        }

        /**
         * Loads values for a field in a particular leaf.
         */
        interface DocValuesLoader {
            /**
             * Position the loader at a document.
             */
            boolean advanceToDoc(int docId) throws IOException;
        }
    }

}
