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
     * Build the loader for some segment.
     */
    Leaf leaf(LeafReader reader) throws IOException;

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
    }

    /**
     * Load {@code _source} from a stored field.
     */
    SourceLoader FROM_STORED_SOURCE = new SourceLoader() {
        @Override
        public Leaf leaf(LeafReader reader) {
            return new Leaf() {
                @Override
                public BytesReference source(FieldsVisitor fieldsVisitor, int docId) {
                    return fieldsVisitor.source();
                }
            };
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
        public Leaf leaf(LeafReader reader) throws IOException {
            SyntheticFieldLoader.Leaf leaf = loader.leaf(reader);
            return new Leaf() {
                @Override
                public BytesReference source(FieldsVisitor fieldsVisitor, int docId) throws IOException {
                    // TODO accept a requested xcontent type
                    try (XContentBuilder b = new XContentBuilder(JsonXContent.jsonXContent, new ByteArrayOutputStream())) {
                        leaf.advanceToDoc(docId);
                        if (leaf.hasValue()) {
                            leaf.load(b);
                        } else {
                            b.startObject().endObject();
                        }
                        return BytesReference.bytes(b);
                    }
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
        SyntheticFieldLoader NOTHING = r -> new Leaf() {
            @Override
            public void advanceToDoc(int docId) throws IOException {}

            @Override
            public boolean hasValue() {
                return false;
            }

            @Override
            public void load(XContentBuilder b) throws IOException {}
        };

        /**
         * Build a loader for this field in the provided segment.
         */
        Leaf leaf(LeafReader reader) throws IOException;

        /**
         * Loads values for a field in a particular leaf.
         */
        interface Leaf {
            /**
             * Position the loader at a document.
             */
            void advanceToDoc(int docId) throws IOException;

            /**
             * Is there a value for this field in this document?
             */
            boolean hasValue();

            /**
             * Load values for this document.
             */
            void load(XContentBuilder b) throws IOException;
        }
    }

}
