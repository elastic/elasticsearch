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
import org.elasticsearch.index.mapper.SourceLoader;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link SourceProvider} that loads _source from a concurrent search.
 *
 * NOTE: This is written under the assumption that individual segments are accessed by a single
 * thread, even if separate segments may be searched concurrently. If we ever implement
 * within-segment concurrency this will have to work entirely differently.
 * **/
class ConcurrentSegmentSourceProvider implements SourceProvider {
    private final SourceLoader sourceLoader;
    private final StoredFieldLoader storedFieldLoader;
    private final Map<Object, Leaf> leaves = ConcurrentCollections.newConcurrentMap();

    ConcurrentSegmentSourceProvider(SourceLoader loader, boolean loadSource) {
        this.sourceLoader = loader;
        this.storedFieldLoader = StoredFieldLoader.create(loadSource, sourceLoader.requiredStoredFields());
    }

    @Override
    public Source getSource(LeafReaderContext ctx, int doc) throws IOException {
        final Object id = ctx.id();
        var leaf = leaves.get(id);
        if (leaf == null) {
            leaf = new Leaf(sourceLoader.leaf(ctx.reader(), null), storedFieldLoader.getLoader(ctx, null));
            var existing = leaves.put(id, leaf);
            assert existing == null : "unexpected source provider [" + existing + "]";
        }
        return leaf.getSource(ctx, doc);
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
