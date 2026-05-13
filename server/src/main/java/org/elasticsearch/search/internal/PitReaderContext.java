/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.SearchService;

public final class PitReaderContext extends ReaderContext {
    private static final long CONTEXT_RELOCATION_GRACE_TIME_MS = 1000;

    // Additional metadata that is needed to handle relocation of PIT contexts
    // opened during resharding.
    private final IndexReshardingMetadata reshardingMetadata;
    private final SplitShardCountSummary shardCountSummary;

    // Volatile since it is written by generic thread pool thread (in `afterIndexShardClosed`)
    // and read from search threads.
    private volatile long relocatedTimestampMs = -1;

    public PitReaderContext(
        ShardSearchContextId id,
        IndexService indexService,
        IndexShard indexShard,
        Engine.SearcherSupplier searcherSupplier,
        long keepAliveInMillis,
        IndexReshardingMetadata reshardingMetadata,
        SplitShardCountSummary shardCountSummary
    ) {
        super(id, indexService, indexShard, searcherSupplier, keepAliveInMillis, false);
        this.reshardingMetadata = reshardingMetadata;
        this.shardCountSummary = shardCountSummary;
    }

    @Override
    public boolean isExpired() {
        if (isRelocating()) {
            if (hasOutstandingRefs()) {
                return false; // there are outstanding users so can't expire
            }

            // Otherwise we don't want to wait for keepalive since we are relocated.
            // However, we don't want to close immediately to prevent running searches from failing.
            // Refcounting via #markAsUsed protects against this while search phases are running,
            // but we also need to protect against closing too soon during phase transitions.
            // The grace period is long enough to allow for search phase transitions of running searches before they complete.
            return nowInMillis() - relocatedTimestampMs > CONTEXT_RELOCATION_GRACE_TIME_MS;
        }

        return super.isExpired();
    }

    @Override
    public boolean isRelocating() {
        return relocatedTimestampMs > 0;
    }

    /**
     * Indicate that this context is in the process of relocating.
     * We check this to prevent new search requests from using this context,
     * while running searches can still use it. Also, this marks the context for cleanup
     * in one of the next {@link  SearchService} Reaper runs.
     */
    public void relocate() {
        relocatedTimestampMs = nowInMillis();
    }

    public IndexReshardingMetadata reshardingMetadata() {
        return reshardingMetadata;
    }

    public SplitShardCountSummary shardCountSummary() {
        return shardCountSummary;
    }
}
