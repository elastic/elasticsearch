/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.index.shard.ShardId;

import java.util.List;

/**
 * Contains an iterator of shards for a given {@link ShardId shard id} and additional routing-related metadata needed to
 * execute searches on these shards.
 */
public final class SearchShardRouting extends ShardIterator {
    private final SplitShardCountSummary reshardSplitShardCountSummary;

    public SearchShardRouting(ShardId shardId, List<ShardRouting> shards, SplitShardCountSummary reshardSplitShardCountSummary) {
        super(shardId, shards);
        this.reshardSplitShardCountSummary = reshardSplitShardCountSummary;
    }

    public SplitShardCountSummary reshardSplitShardCountSummary() {
        return reshardSplitShardCountSummary;
    }

    public static SearchShardRouting fromShardIterator(ShardIterator shardIterator, SplitShardCountSummary reshardSplitShardCountSummary) {
        return new SearchShardRouting(shardIterator.shardId(), shardIterator.getShardRoutings(), reshardSplitShardCountSummary);
    }
}
