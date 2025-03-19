/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class EsqlGetQueryResponse extends ActionResponse implements ToXContentObject {
    // This is rather limited at the moment, as we don't extract information such as CPU and memory usage, owning user, etc. for the task.
    public record DetailedQuery(
        TaskId id,
        long startTimeMillis,
        long runningTimeNanos,
        String query,
        String coordinatingNode,
        List<String> dataNodes
    ) implements ToXContentObject {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("id", id.getId());
            builder.field("node", id.getNodeId());
            builder.field("start_time_millis", startTimeMillis);
            builder.field("running_time_nanos", runningTimeNanos);
            builder.field("query", query);
            builder.field("coordinating_node", coordinatingNode);
            builder.field("data_nodes", dataNodes);
            builder.endObject();
            return builder;
        }
    }

    private final DetailedQuery query;

    public EsqlGetQueryResponse(DetailedQuery query) {
        this.query = query;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new AssertionError("should not reach here");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return query.toXContent(builder, params);
    }
}
