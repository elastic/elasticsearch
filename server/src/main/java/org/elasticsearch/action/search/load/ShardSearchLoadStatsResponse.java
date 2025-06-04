/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search.load;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.ChunkedBroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Response to a shard stats request.
 */
public class ShardSearchLoadStatsResponse extends ChunkedBroadcastResponse {

    private final ShardSearchLoadStats[] shards;

    /**
     * Constructor to create a ShardStatsResponse object from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs
     */
    ShardSearchLoadStatsResponse(StreamInput in) throws IOException {
        super(in);
        shards = in.readArray(ShardSearchLoadStats::new, ShardSearchLoadStats[]::new);
    }

    /**
     * Constructor to create a ShardSearchLoadStatsResponse object with the given parameters.
     *
     * @param shards          the array of shard stats
     * @param totalShards     the total number of shards
     * @param successfulShards the number of successful shards
     * @param failedShards    the number of failed shards
     * @param shardFailures   the list of shard failures
     */
    ShardSearchLoadStatsResponse(
        ShardSearchLoadStats[] shards,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = aggregateSearchLoadByShard(Objects.requireNonNull(shards));
    }

    /**
     * Returns a copy of the array of shard stats.
     *
     * @return the array of shard stats
     */
    public ShardSearchLoadStats[] getShards() {
        return Arrays.copyOf(shards, shards.length);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(shards);
    }

    @Override
    protected Iterator<ToXContent> customXContentChunks(ToXContent.Params params) {
        return Collections.emptyIterator();
    }

    private ShardSearchLoadStats[] aggregateSearchLoadByShard(ShardSearchLoadStats[] shards) {
        Map<ShardKey, Double> aggregated = new HashMap<>();

        for (var stat : shards) {
            var key = new ShardKey(stat.getIndexName(), stat.getShardId());
            var load = Optional.ofNullable(stat.getSearchLoad()).orElse(0.0);
            aggregated.merge(key, load, Double::sum);
        }

        return aggregated.entrySet()
            .stream()
            .map(e -> new ShardSearchLoadStats(e.getKey().indexName(), e.getKey().shardId(), e.getValue()))
            .toArray(ShardSearchLoadStats[]::new);
    }

    private record ShardKey(String indexName, int shardId) {}
}
