/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.SourceLoader;

import java.io.IOException;
import java.util.Map;

// NB This is written under the assumption that individual segments are accessed by a single
// thread, even if separate segments may be searched concurrently.  If we ever implement
// within-segment concurrency this will have to work entirely differently.
class SyntheticSourceProvider implements SourceProvider {

    private final SourceLoader sourceLoader;
    private final Map<Object, SyntheticSourceLeafLoader> leaves = ConcurrentCollections.newConcurrentMap();

    SyntheticSourceProvider(SourceLoader sourceLoader) {
        this.sourceLoader = sourceLoader;
    }

    @Override
    public Source getSource(LeafReaderContext ctx, int doc) throws IOException {
        final Object id = ctx.id();
        var provider = leaves.get(id);
        if (provider == null) {
            provider = new SyntheticSourceLeafLoader(ctx);
            var existing = leaves.put(id, provider);
            assert existing == null : "unexpected source provider [" + existing + "]";
        }
        return provider.getSource(doc);
    }

    private class SyntheticSourceLeafLoader {
        private final LeafStoredFieldLoader leafLoader;
        private final SourceLoader.Leaf leaf;

        SyntheticSourceLeafLoader(LeafReaderContext ctx) throws IOException {
            this.leafLoader = (sourceLoader.requiredStoredFields().isEmpty())
                ? StoredFieldLoader.empty().getLoader(ctx, null)
                : StoredFieldLoader.create(false, sourceLoader.requiredStoredFields()).getLoader(ctx, null);
            this.leaf = sourceLoader.leaf(ctx.reader(), null);
        }

        Source getSource(int doc) throws IOException {
            leafLoader.advanceTo(doc);
            return leaf.source(leafLoader, doc);
        }
    }
}
