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
    };

    /**
     * Load {@code _source} from doc vales.
     */
    class Synthetic implements SourceLoader {
        private final SyntheticFieldLoader loader;

        public Synthetic(Mapping mapping) {
            loader = mapping.getRoot().syntheticFieldLoader();
        }

        @Override
        public boolean reordersFieldValues() {
            return true;
        }

        @Override
        public Leaf leaf(LeafReader reader, int[] docIdsInLeaf) throws IOException {
            SyntheticFieldLoader.Leaf leaf = loader.leaf(reader, docIdsInLeaf);
            if (leaf.empty()) {
                return Leaf.EMPTY_OBJECT;
            }
            return (fieldsVisitor, docId) -> {
                // TODO accept a requested xcontent type
                try (XContentBuilder b = new XContentBuilder(JsonXContent.jsonXContent, new ByteArrayOutputStream())) {
                    if (leaf.advanceToDoc(docId)) {
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
            public boolean advanceToDoc(int docId) throws IOException {
                return false;
            }

            @Override
            public void write(XContentBuilder b) throws IOException {}
        };

        /**
         * Load no values.
         */
        SyntheticFieldLoader NOTHING = (r, docIds) -> NOTHING_LEAF;

        /**
         * Build a loader for this field in the provided segment.
         */
        Leaf leaf(LeafReader reader, int[] docIdsInLeaf) throws IOException;

        /**
         * Loads values for a field in a particular leaf.
         */
        interface Leaf {
            /**
             * Is this entirely empty?
             */
            boolean empty();

            /**
             * Position the loader at a document.
             */
            boolean advanceToDoc(int docId) throws IOException;

            /**
             * Write values for this document.
             */
            void write(XContentBuilder b) throws IOException;
        }
    }

}
