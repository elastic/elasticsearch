/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.index.engine.LiveVersionMapArchive;

/**
 * What the stateless {@link IndexEngine} and its tests need from a live version map archive beyond core's
 * {@link LiveVersionMapArchive}. Implemented by both selectable archive variants.
 */
public interface StatelessLiveVersionMapArchiveApi extends LiveVersionMapArchive {

    /**
     * Called once the unpromotable shards have been refreshed up to the given commit generation, so that archived maps up to it can be
     * released.
     */
    void afterUnpromotablesRefreshed(long generation);

    /**
     * Returns the generation that we need to receive an unpromotable refresh for in order to consider the archive safe, or -1 if the
     * archive holds no unsafe map.
     */
    long getMinSafeGeneration();

    /**
     * The number of {@link org.elasticsearch.index.engine.LiveVersionMap.VersionLookup} objects held and their accounted bytes, as
     * computed by the last {@link #getRamBytesUsed()} call. Reading this does not walk the archive. It is refreshed as a side effect of
     * the indexing memory and disk controllers' periodic polling of the engine's index buffer bytes, so it is at most one polling
     * interval stale.
     */
    CachedStats cachedStats();

    /**
     * @param mapsHeld the number of lookup objects held; what costs memory and what reads walk, but not comparable across archive
     *                 variants (the per-generation one merges maps, the by-reference one keeps one per hand-off)
     * @param ramBytesUsed the same value {@link #getRamBytesUsed()} returned
     */
    record CachedStats(long mapsHeld, long ramBytesUsed) {
        public static final CachedStats ZERO = new CachedStats(0, 0);
    }
}
