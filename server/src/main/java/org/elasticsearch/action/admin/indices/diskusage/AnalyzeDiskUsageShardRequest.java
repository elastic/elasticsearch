/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

final class AnalyzeDiskUsageShardRequest extends BroadcastShardRequest {
    final boolean flush;

    AnalyzeDiskUsageShardRequest(ShardId shardId, AnalyzeIndexDiskUsageRequest request) {
        super(shardId, request);
        this.flush = request.flush;
    }

    AnalyzeDiskUsageShardRequest(StreamInput in) throws IOException {
        super(in);
        this.flush = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(flush);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return AnalyzeDiskUsageShardRequest.this.getDescription();
            }
        };
    }

    @Override
    public String getDescription() {
        return "Analyze disk usage shard [" + shardId() + "], flush [" + flush + "]";
    }
}
