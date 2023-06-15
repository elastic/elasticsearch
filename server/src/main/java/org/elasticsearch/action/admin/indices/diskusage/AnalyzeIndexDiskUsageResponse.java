/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class AnalyzeIndexDiskUsageResponse extends BroadcastResponse {
    private final Map<String, IndexDiskUsageStats> stats;

    AnalyzeIndexDiskUsageResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        Map<String, IndexDiskUsageStats> stats
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.stats = stats;
    }

    AnalyzeIndexDiskUsageResponse(StreamInput in) throws IOException {
        super(in);
        stats = in.readMap(IndexDiskUsageStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(stats, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    Map<String, IndexDiskUsageStats> getStats() {
        return stats;
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        final List<Map.Entry<String, IndexDiskUsageStats>> entries = stats.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();
        for (Map.Entry<String, IndexDiskUsageStats> entry : entries) {
            builder.startObject(entry.getKey());
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
    }
}
