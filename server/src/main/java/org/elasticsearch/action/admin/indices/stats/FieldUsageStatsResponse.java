/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.ChunkedBroadcastResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FieldUsageStatsResponse extends ChunkedBroadcastResponse {
    private final Map<String, List<FieldUsageShardResponse>> stats;

    FieldUsageStatsResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        Map<String, List<FieldUsageShardResponse>> stats
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.stats = stats;
    }

    FieldUsageStatsResponse(StreamInput in) throws IOException {
        super(in);
        stats = in.readMap(i -> i.readCollectionAsList(FieldUsageShardResponse::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(stats, StreamOutput::writeCollection);
    }

    public Map<String, List<FieldUsageShardResponse>> getStats() {
        return stats;
    }

    @Override
    protected Iterator<ToXContent> customXContentChunks(ToXContent.Params params) {
        return Iterators.flatMap(
            stats.entrySet().stream().sorted(Map.Entry.comparingByKey()).iterator(),
            entry -> Iterators.concat(
                ChunkedToXContentHelper.singleChunk((builder, p) -> builder.startObject(entry.getKey()).startArray("shards")),
                entry.getValue().iterator(),
                ChunkedToXContentHelper.singleChunk((builder, p) -> builder.endArray().endObject())
            )
        );
    }
}
