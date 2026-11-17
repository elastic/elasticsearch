/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Set;

/**
 * The {@link SourceLoader} of {@link MappingLookup#newNestedChildSourceLoader}, for ES|QL. Like {@link NestedStoredSourceLoader},
 * which search uses, it gives a nested child document the slice of its root's {@code _source} it was indexed from, and caches
 * the root's source for the root's other children. Unlike it, it reads the root with whatever loader the index's {@code _source}
 * needs, stored or synthetic, rather than only the stored bytes.
 *
 * <p>A synthetic root loader reads doc values, so its documents must be read in increasing doc ID order.
 */
final class NestedChildSourceLoader implements SourceLoader {

    private final SourceLoader rootLoader;
    private final NestedDocuments nestedDocuments;

    NestedChildSourceLoader(SourceLoader rootLoader, NestedDocuments nestedDocuments) {
        this.rootLoader = rootLoader;
        this.nestedDocuments = nestedDocuments;
    }

    @Override
    public boolean reordersFieldValues() {
        return rootLoader.reordersFieldValues();
    }

    @Override
    public Set<String> requiredStoredFields() {
        return rootLoader.requiredStoredFields();
    }

    @Override
    public Leaf leaf(LeafReaderContext ctx, int[] docIdsInLeaf) throws IOException {
        LeafNestedDocuments leafNestedDocuments = nestedDocuments.getLeafNestedDocuments(ctx);
        // The caller's stored fields may be on a child, which has no _source, so the root is read with its own
        LeafStoredFieldLoader leafRootLoader = StoredFieldLoader.create(true, rootLoader.requiredStoredFields()).getLoader(ctx, null);
        return new ChildLeaf(leafNestedDocuments, leafRootLoader, rootLoader.leaf(ctx, null));
    }

    private static class ChildLeaf implements Leaf {
        private final LeafNestedDocuments leafNestedDocuments;
        private final LeafStoredFieldLoader leafRootLoader;
        private final Leaf rootLeaf;

        private int lastRootDoc = -1;
        private Source lastRootSource = null;

        ChildLeaf(LeafNestedDocuments leafNestedDocuments, LeafStoredFieldLoader leafRootLoader, Leaf rootLeaf) {
            this.leafNestedDocuments = leafNestedDocuments;
            this.leafRootLoader = leafRootLoader;
            this.rootLeaf = rootLeaf;
        }

        @Override
        public Source source(LeafStoredFieldLoader storedFields, int docId) throws IOException {
            SearchHit.NestedIdentity nestedIdentity = leafNestedDocuments.advance(docId);
            if (nestedIdentity == null) {
                if (docId != lastRootDoc) {
                    lastRootSource = rootLeaf.source(storedFields, docId);
                    lastRootDoc = docId;
                }
                return lastRootSource;
            }
            int rootDoc = leafNestedDocuments.rootDoc();
            if (rootDoc != lastRootDoc) {
                leafRootLoader.advanceTo(rootDoc);
                lastRootSource = rootLeaf.source(leafRootLoader, rootDoc);
                lastRootDoc = rootDoc;
            }
            return nestedIdentity.extractSource(lastRootSource);
        }

        /** The root's source, which is kept for its other children however small the slice of this one. */
        @Override
        public long sourceBytesHeld(Source source) {
            return rootLeaf.sourceBytesHeld(lastRootSource);
        }

        @Override
        public void write(LeafStoredFieldLoader storedFields, int docId, XContentBuilder b) throws IOException {
            Source source = source(storedFields, docId);
            b.rawValue(source.internalSourceRef().streamInput(), source.sourceContentType());
        }
    }
}
