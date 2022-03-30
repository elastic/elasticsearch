/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Function;

public interface SourceLoader {
    interface Leaf {
        BytesReference source(FieldsVisitor fieldsVisitor, int docId) throws IOException;
    }

    Leaf leaf(LeafReaderContext ctx);

    SourceLoader FROM_STORED_SOURCE = new SourceLoader() {
        @Override
        public Leaf leaf(LeafReaderContext ctx) {
            return new Leaf() {
                @Override
                public BytesReference source(FieldsVisitor fieldsVisitor, int docId) {
                    return fieldsVisitor.source();
                }
            };
        }
    };

    class Synthetic implements SourceLoader {
        private final SyntheticFieldLoader loader;

        Synthetic(Function<MappedFieldType, IndexFieldData<?>> fdLookup, RootObjectMapper root) {
            loader = root.syntheticFieldLoader(fdLookup);
        }

        @Override
        public Leaf leaf(LeafReaderContext ctx) {
            SyntheticFieldLoader.Leaf leaf = loader.leaf(ctx);
            return new Leaf() {
                @Override
                public BytesReference source(FieldsVisitor fieldsVisitor, int docId) throws IOException {
                    // NOCOMMIT pick the right xcontent type
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

    interface SyntheticFieldLoader {
        Leaf leaf(LeafReaderContext ctx);

        interface Leaf {
            void advanceToDoc(int docId) throws IOException;
            boolean hasValue();
            void load(XContentBuilder b) throws IOException;
        }
    }
}
