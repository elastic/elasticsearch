/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

final class AnalyzeDiskUsageShardResponse extends BroadcastShardResponse {
    final IndexDiskUsageStats stats;

    AnalyzeDiskUsageShardResponse(StreamInput in) throws IOException {
        super(in);
        stats = new IndexDiskUsageStats(in);
    }

    AnalyzeDiskUsageShardResponse(ShardId shardId, IndexDiskUsageStats stats) {
        super(shardId);
        this.stats = Objects.requireNonNull(stats, "stats must be non null");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        stats.writeTo(out);
    }
}
