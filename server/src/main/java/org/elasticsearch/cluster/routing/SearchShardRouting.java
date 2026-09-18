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
    private final SplitShardCountSummary splitShardCountSummary;

    public SearchShardRouting(ShardId shardId, List<ShardRouting> shards, SplitShardCountSummary splitShardCountSummary) {
        super(shardId, shards);
        this.splitShardCountSummary = splitShardCountSummary;
    }

    public SplitShardCountSummary splitShardCountSummary() {
        return splitShardCountSummary;
    }

    public static SearchShardRouting fromShardIterator(ShardIterator shardIterator, SplitShardCountSummary splitShardCountSummary) {
        return new SearchShardRouting(shardIterator.shardId(), shardIterator.getShardRoutings(), splitShardCountSummary);
    }
}
