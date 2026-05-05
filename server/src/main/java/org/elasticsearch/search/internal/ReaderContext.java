/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.transport.TransportRequest;

/**
 * Holds a reference to a point in time {@link Engine.Searcher} that will be used to construct {@link SearchContext}.
 * This class also implements {@link RefCounted} since in some situations like
 * in {@link org.elasticsearch.search.SearchService} a SearchContext can be closed concurrently due to independent events
 * ie. when an index gets removed. To prevent accessing closed IndexReader / IndexSearcher instances the SearchContext
 * can be guarded by a reference count and fail if it's been closed by an external event.
 */
public sealed interface ReaderContext extends Releasable permits SingleSessionReaderContext, PitReaderContext, ScrollReaderContext {
    void validate(TransportRequest request);

    void addOnClose(Releasable releasable);

    ShardSearchContextId id();

    IndexService indexService();

    IndexShard indexShard();

    long keepAlive();

    Engine.Searcher acquireSearcher(String source);

    /**
     * Returns a releasable to indicate that the caller has stopped using this reader.
     * The time to live of the reader after usage can be extended using the provided
     * <code>keepAliveInMillis</code>.
     */
    Releasable markAsUsed(long keepAliveInMillis);

    boolean isExpired();

    default boolean isRelocating() {
        // Only relevant for PIT contexts.
        return false;
    }

    // 6 methods below are (unfortunately) only implemented by ScrollReaderContext.

    ShardSearchRequest getShardSearchRequest(ShardSearchRequest other);

    ScrollContext scrollContext();

    AggregatedDfs getAggregatedDfs(AggregatedDfs other);

    void setAggregatedDfs(AggregatedDfs aggregatedDfs);

    RescoreDocIds getRescoreDocIds(RescoreDocIds other);

    void setRescoreDocIds(RescoreDocIds rescoreDocIds);

    /**
     * Returns {@code true} for readers that are intended to use in a single query. For readers that are intended
     * to use in multiple queries (i.e., scroll or readers), we should not release them after the fetch phase
     * or the query phase with empty results.
     */
    boolean singleSession();

    /**
     * Returns the object or <code>null</code> if the given key does not have a
     * value in the context
     */
    <T> T getFromContext(String key);

    /**
     * Puts the object into the context
     */
    void putInContext(String key, Object value);

    long getStartTimeInNano();
}
