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
    Stream<String> requiredStoredFields();

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
        private final Map<String, SyntheticFieldLoader.StoredFieldLoader> storedFieldLeaves;

        public Synthetic(Mapping mapping) {
            loader = mapping.getRoot().syntheticFieldLoader();
            storedFieldLeaves = Map.ofEntries(loader.storedFieldsLeaves().toArray(Map.Entry[]::new));
        }

        @Override
        public boolean reordersFieldValues() {
            return true;
        }

        @Override
        public Stream<String> requiredStoredFields() {
            return loader.requiredStoredFields();
        }

        @Override
        public Leaf leaf(LeafReader reader, int[] docIdsInLeaf) throws IOException {
            SyntheticFieldLoader.Leaf leaf = loader.leaf(reader, docIdsInLeaf);
            if (leaf.empty()) {
                return Leaf.EMPTY_OBJECT;
            }
            return (fieldsVisitor, docId) -> {
                for (Map.Entry<String, List<Object>> e : fieldsVisitor.fields().entrySet()) {
                    SyntheticFieldLoader.StoredFieldLoader storedFieldLeaf = storedFieldLeaves.get(e.getKey());
                    if (storedFieldLeaf != null) {
                        storedFieldLeaf.loadedStoredField(e.getValue());
                    }
                }
                // TODO accept a requested xcontent type
                try (XContentBuilder b = new XContentBuilder(JsonXContent.jsonXContent, new ByteArrayOutputStream())) {
                    if (leaf.advanceToDoc(fieldsVisitor, docId)) {
                        leaf.write(b);
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
        SyntheticFieldLoader.Leaf NOTHING_LEAF = new Leaf() {
            @Override
            public boolean empty() {
                return true;
            }

            @Override
            public boolean advanceToDoc(FieldsVisitor fieldsVisitor, int docId) {
                return false;
            }

            @Override
            public void write(XContentBuilder b) {}
        };

        /**
         * Load no values.
         */
        SyntheticFieldLoader NOTHING = new SyntheticFieldLoader() {
            @Override
            public Stream<String> requiredStoredFields() {
                return Stream.empty();
            }

            @Override
            public Leaf leaf(LeafReader reader, int[] docIdsInLeaf) {
                return NOTHING_LEAF;
            }
        };

        /**
         * Build a loader for this field in the provided segment.
         */
        Leaf leaf(LeafReader reader, int[] docIdsInLeaf) throws IOException;

        default RenameMe writer() {
            return null;
        }

        /**
         * Stream containing all non-{@code _source} stored fields required
         * to build the {@code _source}.
         */
        Stream<String> requiredStoredFields();

        default Stream<Map.Entry<String, StoredFieldLoader>> storedFieldsLeaves() {
            return Stream.empty();
        }

        /**
         * Loads values for a field in a particular leaf.
         */
        interface Leaf extends RenameMe {
            /**
             * Is this entirely empty?
             */
            boolean empty();

            /**
             * Position the loader at a document.
             */
            boolean advanceToDoc(FieldsVisitor fieldsVisitor, int docId) throws IOException;
        }

        interface RenameMe {
            /**
             * Write values for this document.
             */
            void write(XContentBuilder b) throws IOException;
        }

        interface StoredFieldLoader extends RenameMe {
            void loadedStoredField(List<Object> values);
        }
    }

}
