/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.index.mapper.SourceLoader;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * A {@link SourceProvider} that loads _source from a concurrent search.
 *
 * NOTE: This is written under the assumption that individual segments are accessed by a single
 * thread, even if separate segments may be searched concurrently. If we ever implement
 * within-segment concurrency this will have to work entirely differently.
 * **/
class ConcurrentSegmentSourceProvider implements SourceProvider {
    private final Function<SourceFilter, SourceLoader> sourceLoaderProvider;
    private final SourceLoader sourceLoader;
    private final StoredFieldLoader storedFieldLoader;
    private final Map<Object, Leaf> leaves = ConcurrentCollections.newConcurrentMap();
    private final boolean isStoredSource;

    ConcurrentSegmentSourceProvider(MappingLookup lookup, SourceFilter filter, SourceFieldMetrics metrics) {
        this.sourceLoaderProvider = sourceFilter -> lookup.newSourceLoader(sourceFilter, metrics);
        this.sourceLoader = sourceLoaderProvider.apply(filter);
        // we force a sequential reader here since it is used during query execution where documents are scanned sequentially
        this.isStoredSource = lookup.isSourceSynthetic() == false;
        this.storedFieldLoader = StoredFieldLoader.create(isStoredSource, sourceLoader.requiredStoredFields(), true);
    }

    private ConcurrentSegmentSourceProvider(ConcurrentSegmentSourceProvider source, SourceFilter filter) {
        assert source.isStoredSource == false;
        this.sourceLoaderProvider = source.sourceLoaderProvider;
        this.isStoredSource = source.isStoredSource;
        this.sourceLoader = source.sourceLoaderProvider.apply(filter);
        // Also re-initialize stored field loader:
        this.storedFieldLoader = StoredFieldLoader.create(isStoredSource, sourceLoader.requiredStoredFields(), true);
    }

    @Override
    public Source getSource(LeafReaderContext ctx, int doc) throws IOException {
        final Object id = ctx.id();
        var leaf = leaves.get(id);
        if (leaf == null) {
            leaf = new Leaf(sourceLoader.leaf(ctx.reader(), null), storedFieldLoader.getLoader(ctx, null));
            var existing = leaves.put(id, leaf);
            assert existing == null : "unexpected source provider [" + existing + "]";
        } else if (isStoredSource == false && doc < leaf.doc) {
            // When queries reference the same runtime field in multiple clauses, each clause re-reads the values from the source in
            // increasing docId order. So the last docId accessed by the first clause is higher than the first docId read by the second
            // clause. This is okay for stored source, as stored fields do not restrict the order that docIds that can be accessed.
            // But with synthetic source, field values may come from doc values, which require than docIds only be read in increasing order.
            // To handle this, we detect lower docIds and create a new doc value reader for each clause.
            leaf = new Leaf(sourceLoader.leaf(ctx.reader(), null), storedFieldLoader.getLoader(ctx, null));
            leaves.put(id, leaf);
        }
        return leaf.getSource(ctx, doc);
    }

    @Override
    public SourceProvider optimizedSourceProvider(SourceFilter sourceFilter) {
        if (isStoredSource) {
            return this;
        } else {
            return new ConcurrentSegmentSourceProvider(this, sourceFilter);
        }
    }

    private static class Leaf implements SourceProvider {
        private final SourceLoader.Leaf sourceLoader;
        private final LeafStoredFieldLoader storedFieldLoader;
        int doc = -1;
        Source source = null;

        private Leaf(SourceLoader.Leaf sourceLoader, LeafStoredFieldLoader storedFieldLoader) {
            this.sourceLoader = sourceLoader;
            this.storedFieldLoader = storedFieldLoader;
        }

        @Override
        public Source getSource(LeafReaderContext ctx, int doc) throws IOException {
            if (this.doc == doc) {
                return source;
            }
            this.doc = doc;
            storedFieldLoader.advanceTo(doc);
            return source = sourceLoader.source(storedFieldLoader, doc);
        }
    }
}
