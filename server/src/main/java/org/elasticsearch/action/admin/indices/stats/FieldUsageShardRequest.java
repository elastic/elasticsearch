/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class FieldUsageShardRequest extends BroadcastShardRequest {

    private final String[] fields;

    FieldUsageShardRequest(ShardId shardId, FieldUsageStatsRequest request) {
        super(shardId, request);
        this.fields = request.fields();
    }


    FieldUsageShardRequest(StreamInput in) throws IOException {
        super(in);
        this.fields = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(fields);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return FieldUsageShardRequest.this.getDescription();
            }
        };
    }

    @Override
    public String getDescription() {
        return "get field usage for shard: [" + shardId() + "], fields: " + Arrays.toString(fields);
    }

    public String[] fields() {
        return fields;
    }
}
