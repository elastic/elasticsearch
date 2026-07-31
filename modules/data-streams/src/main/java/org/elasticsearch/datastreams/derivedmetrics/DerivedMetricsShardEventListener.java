/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

/**
 * Flushes what has been observed for a shard before that shard leaves the node.
 *
 * <p>Without this, a shard that relocates or closes mid-interval leaves its observations sitting in the buffer until the interval closes
 * on its own. That is usually harmless — the flush still happens, and the partial still lands — but it is an avoidable window in which a
 * node restart would lose data that was already collected. Relocation hand-off drains the shard's operation permits before this is
 * called, so by this point the shard's observations are complete.
 *
 * <p>The buffer is keyed by data stream rather than by shard, so this flushes the stream the shard belonged to. Flushing more than the
 * departing shard's share is harmless: it produces one more partial, and partials are summed at query time by design.
 */
public class DerivedMetricsShardEventListener implements IndexEventListener {

    private final DerivedMetricsService service;

    public DerivedMetricsShardEventListener(DerivedMetricsService service) {
        this.service = service;
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        service.flushEverything("shard [" + shardId + "] is closing");
    }
}
