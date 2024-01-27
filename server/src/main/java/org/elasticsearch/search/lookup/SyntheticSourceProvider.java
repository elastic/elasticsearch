/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceLoader;

import java.io.IOException;

// NB This is written under the assumption that individual segments are accessed by a single
// thread, even if separate segments may be searched concurrently.  If we ever implement
// within-segment concurrency this will have to work entirely differently.
class SyntheticSourceProvider implements SourceProvider {

    private final SourceLoader sourceLoader;
    private volatile SyntheticSourceLeafLoader[] leafLoaders;

    SyntheticSourceProvider(Mapping mapping) {
        sourceLoader = new SourceLoader.Synthetic(mapping);
    }

    @Override
    public Source getSource(LeafReaderContext ctx, int doc) throws IOException {
        maybeInit(ctx);
        if (leafLoaders[ctx.ord] == null) {
            // individual segments are currently only accessed on one thread so there's no need
            // for locking here.
            leafLoaders[ctx.ord] = new SyntheticSourceLeafLoader(ctx);
        }
        return leafLoaders[ctx.ord].getSource(doc);
    }

    private void maybeInit(LeafReaderContext ctx) {
        if (leafLoaders == null) {
            synchronized (this) {
                if (leafLoaders == null) {
                    leafLoaders = new SyntheticSourceLeafLoader[findParentContext(ctx).leaves().size()];
                }
            }
        }
    }

    private IndexReaderContext findParentContext(LeafReaderContext ctx) {
        if (ctx.parent != null) {
            return ctx.parent;
        }
        assert ctx.isTopLevel;
        return ctx;
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
