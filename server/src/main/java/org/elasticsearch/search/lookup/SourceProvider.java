/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
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
     *
     * The returned SourceProvider is thread-safe across segments, in that it may be
     * safely used by a searcher that searches different segments on different threads,
     * but it is not safe to use this to access documents from the same segment across
     * multiple threads.
     */
    static SourceProvider fromStoredFields() {
        StoredFieldLoader storedFieldLoader = StoredFieldLoader.sequentialSource();
        return new StoredFieldSourceProvider(storedFieldLoader);
    }

}
