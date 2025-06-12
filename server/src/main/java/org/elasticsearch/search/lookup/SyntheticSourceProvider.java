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
        } else if (doc < provider.lastSeenDocId) {
            // When queries reference the same runtime field in multiple clauses, each clause re-reads the values from the source in
            // increasing docId order. So the last docId accessed by the first clause is higher than the first docId read by the second
            // clause. This is okay for stored source, as stored fields do not restrict the order that docIds that can be accessed.
            // But with synthetic source, field values may come from doc values, which require than docIds only be read in increasing order.
            // To handle this, we detect lower docIds and create a new doc value reader for each clause.
            provider = new SyntheticSourceLeafLoader(ctx);
            leaves.put(id, provider);
        }
        return provider.getSource(doc);
    }

    private class SyntheticSourceLeafLoader {
        private final LeafStoredFieldLoader leafLoader;
        private final SourceLoader.Leaf leaf;
        int lastSeenDocId = -1;

        SyntheticSourceLeafLoader(LeafReaderContext ctx) throws IOException {
            this.leafLoader = (sourceLoader.requiredStoredFields().isEmpty())
                ? StoredFieldLoader.empty().getLoader(ctx, null)
                : StoredFieldLoader.create(false, sourceLoader.requiredStoredFields()).getLoader(ctx, null);
            this.leaf = sourceLoader.leaf(ctx.reader(), null);
        }

        Source getSource(int doc) throws IOException {
            this.lastSeenDocId = doc;
            leafLoader.advanceTo(doc);
            return leaf.source(leafLoader, doc);
        }
    }
}
