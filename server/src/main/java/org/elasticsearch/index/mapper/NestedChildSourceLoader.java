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
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@link SourceLoader} of {@link MappingLookup#newNestedChildSourceLoader}, for ES|QL. Like {@link NestedStoredSourceLoader},
 * which search uses, it gives a nested child document the slice of its root's {@code _source} it was indexed from, and caches
 * the root's source for the root's other children. Unlike it, it reads the root with whatever loader the index's {@code _source}
 * needs, stored or synthetic, rather than only the stored bytes. It requires the root's source to hold every object of the
 * nested field, which a mapping-level {@code _source} filter can break; see {@link MappingLookup#newNestedChildSourceLoader}.
 *
 * <p>A synthetic root loader reads doc values, so its documents must be read in increasing doc ID order.
 */
final class NestedChildSourceLoader implements SourceLoader {

    private final SourceLoader rootLoader;
    private final NestedDocuments nestedDocuments;
    private final NestedLookup nestedLookup;

    NestedChildSourceLoader(SourceLoader rootLoader, NestedDocuments nestedDocuments, NestedLookup nestedLookup) {
        this.rootLoader = rootLoader;
        this.nestedDocuments = nestedDocuments;
        this.nestedLookup = nestedLookup;
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
        return new ChildLeaf(ctx, nestedLookup, leafNestedDocuments, leafRootLoader, rootLoader.leaf(ctx, null));
    }

    private static class ChildLeaf implements Leaf {
        private final LeafReaderContext ctx;
        private final NestedLookup nestedLookup;
        private final LeafNestedDocuments leafNestedDocuments;
        private final LeafStoredFieldLoader leafRootLoader;
        private final Leaf rootLeaf;
        /** The children of each top-level nested field, read forward from one child to the next. */
        private final Map<String, DocIdSetIterator> siblings = new HashMap<>();

        private int lastRootDoc = -1;
        private Source lastRootSource = null;
        /** The bytes held for {@link #lastRootSource}, or -1 until asked, since measuring a map-backed source serializes it. */
        private long lastRootBytesHeld = -1;
        private int lastChildDoc = -1;
        private SearchHit.NestedIdentity lastChildIdentity = null;
        /** The objects of one top-level nested field in the last root's source, which every child of that field slices. */
        private int collectionRootDoc = -1;
        private String collectionPath = null;
        private List<Map<?, ?>> collectionObjects = null;

        ChildLeaf(
            LeafReaderContext ctx,
            NestedLookup nestedLookup,
            LeafNestedDocuments leafNestedDocuments,
            LeafStoredFieldLoader leafRootLoader,
            Leaf rootLeaf
        ) {
            this.ctx = ctx;
            this.nestedLookup = nestedLookup;
            this.leafNestedDocuments = leafNestedDocuments;
            this.leafRootLoader = leafRootLoader;
            this.rootLeaf = rootLeaf;
        }

        @Override
        public Source source(LeafStoredFieldLoader storedFields, int docId) throws IOException {
            SearchHit.NestedIdentity nestedIdentity = nextSibling(docId);
            int rootDoc = lastRootDoc;
            if (nestedIdentity == null) {
                nestedIdentity = leafNestedDocuments.advance(docId);
                if (nestedIdentity == null) {
                    if (docId != lastRootDoc) {
                        lastRootSource = rootLeaf.source(storedFields, docId);
                        lastRootDoc = docId;
                        onRootChanged();
                    }
                    lastChildIdentity = null;
                    return lastRootSource;
                }
                rootDoc = leafNestedDocuments.rootDoc();
            }
            lastChildDoc = docId;
            lastChildIdentity = nestedIdentity;
            if (rootDoc != lastRootDoc) {
                leafRootLoader.advanceTo(rootDoc);
                lastRootSource = rootLeaf.source(leafRootLoader, rootDoc);
                lastRootDoc = rootDoc;
                onRootChanged();
            }
            return extractSource(nestedIdentity);
        }

        private void onRootChanged() {
            lastRootBytesHeld = -1;
            collectionRootDoc = -1;
            collectionPath = null;
            collectionObjects = null;
        }

        /**
         * Like {@link SearchHit.NestedIdentity#extractSource}, which walks the whole field to find one object, so reading all
         * of a root's children is quadratic in their number. This walks the field once per root.
         */
        private Source extractSource(SearchHit.NestedIdentity nestedIdentity) {
            if (nestedIdentity.getChild() != null) {
                return nestedIdentity.extractSource(lastRootSource);
            }
            String path = nestedIdentity.getField().string();
            if (collectionRootDoc != lastRootDoc || path.equals(collectionPath) == false) {
                collectionObjects = XContentMapValues.extractNestedSources(path, lastRootSource.source());
                collectionRootDoc = lastRootDoc;
                collectionPath = path;
            }
            if (collectionObjects == null || collectionObjects.isEmpty()) {
                return Source.empty(lastRootSource.sourceContentType());
            }
            if (nestedIdentity.getOffset() >= collectionObjects.size()) {
                throw new IllegalStateException("Error retrieving path " + path);
            }
            Map<String, Object> child = new HashMap<>();
            child.put(path, collectionObjects.get(nestedIdentity.getOffset()));
            return Source.fromMap(child, lastRootSource.sourceContentType());
        }

        /**
         * The identity of {@code docId} when it is a later child of the last child's root in the same top-level nested field, or
         * {@code null}. {@link LeafNestedDocuments#advance} counts a child's offset from its root's first child, which makes
         * reading every child quadratic in their number; this counts only the children since the last one.
         */
        private SearchHit.NestedIdentity nextSibling(int docId) throws IOException {
            if (lastChildIdentity == null || lastChildIdentity.getChild() != null || docId <= lastChildDoc || docId >= lastRootDoc) {
                return null;
            }
            String path = lastChildIdentity.getField().string();
            DocIdSetIterator children = siblings(path);
            if (children == null || children.docID() > lastChildDoc) {
                return null;
            }
            int offset = lastChildIdentity.getOffset();
            for (int child = children.advance(lastChildDoc + 1); child <= docId; child = children.nextDoc()) {
                offset++;
                if (child == docId) {
                    return new SearchHit.NestedIdentity(path, offset, null);
                }
            }
            return null;
        }

        private DocIdSetIterator siblings(String path) throws IOException {
            if (siblings.containsKey(path) == false) {
                NestedObjectMapper mapper = nestedLookup.getNestedMappers().get(path);
                DocIdSetIterator iterator = null;
                if (mapper != null) {
                    IndexSearcher searcher = new IndexSearcher(ReaderUtil.getTopLevelContext(ctx));
                    Scorer scorer = searcher.createWeight(searcher.rewrite(mapper.nestedTypeFilter()), ScoreMode.COMPLETE_NO_SCORES, 1)
                        .scorer(ctx);
                    iterator = scorer == null ? null : scorer.iterator();
                }
                siblings.put(path, iterator);
            }
            return siblings.get(path);
        }

        /** The root's source, which is kept for its other children however small the slice of this one. */
        @Override
        public long sourceBytesHeld(Source source) {
            if (lastRootBytesHeld < 0) {
                lastRootBytesHeld = rootLeaf.sourceBytesHeld(lastRootSource);
            }
            return lastRootBytesHeld;
        }

        @Override
        public void write(LeafStoredFieldLoader storedFields, int docId, XContentBuilder b) throws IOException {
            Source source = source(storedFields, docId);
            b.rawValue(source.internalSourceRef().streamInput(), source.sourceContentType());
        }
    }
}
