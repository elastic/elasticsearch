/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;

import java.io.IOException;

class StoredFieldSourceProvider implements SourceProvider {

    private final StoredFieldLoader storedFieldLoader;
    private volatile LeafStoredFieldSourceProvider[] leaves;

    StoredFieldSourceProvider(StoredFieldLoader storedFieldLoader) {
        this.storedFieldLoader = storedFieldLoader;
    }

    @Override
    public Source getSource(LeafReaderContext ctx, int doc) throws IOException {
        LeafStoredFieldSourceProvider[] leaves = getLeavesUnderLock(ctx.parent);
        if (leaves[ctx.ord] == null) {
            // individual segments are only accessed on one thread so there's no need
            // for locking here
            leaves[ctx.ord] = new LeafStoredFieldSourceProvider(storedFieldLoader.getLoader(ctx, null));
        }
        return leaves[ctx.ord].getSource(doc);
    }

    private LeafStoredFieldSourceProvider[] getLeavesUnderLock(CompositeReaderContext parentCtx) {
        if (leaves == null) {
            synchronized (this) {
                if (leaves == null) {
                    leaves = new LeafStoredFieldSourceProvider[parentCtx.leaves().size()];
                }
            }
        }
        return leaves;
    }

    private static class LeafStoredFieldSourceProvider {

        final LeafStoredFieldLoader leafStoredFieldLoader;
        int doc = -1;
        Source source;

        private LeafStoredFieldSourceProvider(LeafStoredFieldLoader leafStoredFieldLoader) {
            this.leafStoredFieldLoader = leafStoredFieldLoader;
        }

        Source getSource(int doc) throws IOException {
            if (this.doc == doc) {
                return source;
            }
            this.doc = doc;
            leafStoredFieldLoader.advanceTo(doc);
            return source = Source.fromBytes(leafStoredFieldLoader.source());
        }
    }
}
