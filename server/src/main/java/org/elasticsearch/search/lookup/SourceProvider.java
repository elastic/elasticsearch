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
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.SourceFieldMetrics;

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
     * A SourceProvider that delegate loading source to the provided {@link MappingLookup}.
     *
     * The returned SourceProvider is thread-safe across segments, in that it may be
     * safely used by a searcher that searches different segments on different threads,
     * but it is not safe to use this to access documents from the same segment across
     * multiple threads.
     */
    static SourceProvider fromLookup(MappingLookup lookup, SourceFilter filter, SourceFieldMetrics metrics) {
        return new ConcurrentSegmentSourceProvider(lookup, filter, metrics);
    }

    /**
     * Optionally returns a new {@link SourceProvider} that is more optimized to load source with the provided source filter in mind.
     * <p>
     * Currently this is only the case if source mode is synthetic, and only a subset of fields is requested,
     * then only loading source for requested fields is much more efficient.
     *
     * @param sourceFilter The part of the source the caller is actually interested in.
     * @return a new instance if source can be loaded in a more optimal way, otherwise returns this instance.
     */
    default SourceProvider optimizedSourceProvider(SourceFilter sourceFilter) {
        return this;
    }
}
