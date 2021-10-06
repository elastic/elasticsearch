/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class FieldUsageStatsRequest extends BroadcastRequest<FieldUsageStatsRequest> {

    private String[] fields = Strings.EMPTY_ARRAY;

    public FieldUsageStatsRequest(String... indices) {
        super(indices);
    }

    public FieldUsageStatsRequest(String[] indices, IndicesOptions indicesOptions) {
        super(indices, indicesOptions);
    }

    public FieldUsageStatsRequest(StreamInput in) throws IOException {
        super(in);
        fields = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(fields);
    }

    public FieldUsageStatsRequest fields(String... fields) {
        this.fields = fields;
        return this;
    }

    public String[] fields() {
        return this.fields;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, FieldUsageStatsAction.NAME, type, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return FieldUsageStatsRequest.this.getDescription();
            }
        };
    }

    @Override
    public String getDescription() {
        return "get field usage for indices [" + String.join(",", indices) + "], fields " + Arrays.toString(fields);
    }

}
