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
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Set;

/**
 * A {@link SourceLoader} that correctly handles nested documents by extracting the appropriate
 * portion of the root document's source for a given nested Lucene document ID.
 *
 * <p>Nested documents in Elasticsearch store their source within the parent (root) document's
 * {@code _source} stored field. Nested Lucene child documents carry no {@code _source} of their
 * own, so a plain stored-source loader returns empty when asked for a nested document's source.
 * This class detects that case, loads the root document's source instead, and delegates to
 * {@link SearchHit.NestedIdentity#extractSource} to return just the relevant nested-object slice.
 *
 * <p>Root documents are passed through to the root loader. The root source is cached per root document ID
 * so that multiple children of the same parent, or a root document visited after its children, do
 * not trigger redundant reads.
 *
 * <p>The root loader reads stored source, optionally with vectors patched back in; it is not applicable
 * to synthetic or columnar source, whose loaders need the doc IDs of a leaf up front. It is constructed
 * exclusively from the stored-source branch of
 * {@link MappingLookup#newSourceLoader(SourceFilter, SourceFieldMetrics, NestedDocuments)}.
 */
final class NestedStoredSourceLoader implements SourceLoader {

    private final SourceLoader rootLoader;
    private final NestedDocuments nestedDocuments;

    NestedStoredSourceLoader(SourceLoader rootLoader, NestedDocuments nestedDocuments) {
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

        // Separate stored-field loader for root documents: nested child docs carry no _source,
        // so when the caller's storedFields are positioned on a child, we need to reload from root.
        StoredFieldLoader rootStoredFieldLoader = StoredFieldLoader.create(true, rootLoader.requiredStoredFields());
        LeafStoredFieldLoader leafRootLoader = rootStoredFieldLoader.getLoader(ctx, null);

        return new NestedLeaf(leafNestedDocuments, leafRootLoader, rootLoader.leaf(ctx, null));
    }

    private static class NestedLeaf implements Leaf {

        private final LeafNestedDocuments leafNestedDocuments;
        private final LeafStoredFieldLoader leafRootLoader;
        private final Leaf rootLeaf;

        private int lastRootDoc = -1;
        private Source lastRootSource = null;

        NestedLeaf(LeafNestedDocuments leafNestedDocuments, LeafStoredFieldLoader leafRootLoader, Leaf rootLeaf) {
            this.leafNestedDocuments = leafNestedDocuments;
            this.leafRootLoader = leafRootLoader;
            this.rootLeaf = rootLeaf;
        }

        @Override
        public Source source(LeafStoredFieldLoader storedFields, int docId) throws IOException {
            SearchHit.NestedIdentity nestedIdentity = leafNestedDocuments.advance(docId);
            if (nestedIdentity == null) {
                // Root document: load and cache so that later children of this root skip the reload.
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

        @Override
        public void write(LeafStoredFieldLoader storedFields, int docId, XContentBuilder b) throws IOException {
            Source source = source(storedFields, docId);
            b.rawValue(source.internalSourceRef().streamInput(), source.sourceContentType());
        }
    }
}
