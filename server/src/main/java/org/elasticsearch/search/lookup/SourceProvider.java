/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;

import java.io.IOException;

/**
 * Provides access to the Source of a document
 */
public interface SourceProvider {

    /**
     * Get the Source for the given doc within the given context
     */
    Source getSource(LeafReaderContext ctx, int doc) throws IOException;

    /**
     * A SourceProvider that loads source from stored fields
     */
    static SourceProvider fromStoredFields() {
        StoredFieldLoader storedFieldLoader = StoredFieldLoader.sequentialSource();
        return new SourceProvider() {

            // TODO we can make this segment thread safe by keeping an array of LeafStoredFieldLoader/doc/Source
            // records, indexed by the ordinal of the LeafReaderContext

            LeafReaderContext ctx;
            int doc;
            LeafStoredFieldLoader leafStoredFieldLoader;
            Source source;

            @Override
            public Source getSource(LeafReaderContext ctx, int doc) throws IOException {
                if (this.ctx == ctx) {
                    if (this.doc == doc) {
                        return source;
                    }
                } else {
                    leafStoredFieldLoader = storedFieldLoader.getLoader(ctx, null);
                }
                this.ctx = ctx;
                this.doc = doc;
                leafStoredFieldLoader.advanceTo(doc);
                return source = Source.fromBytes(leafStoredFieldLoader.source());
            }
        };
    }
}
