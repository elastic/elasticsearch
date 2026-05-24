/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;

import java.io.IOException;
import java.util.List;

public class EsqlListQueriesResponse extends ActionResponse implements ToXContentObject {
    private final List<Query> queries;

    public record Query(AsyncExecutionId id, long startTimeMillis, long runningTimeNanos, String query) implements ToXContentFragment {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(id.getEncoded());
            builder.field("start_time_millis", startTimeMillis);
            builder.field("running_time_nanos", runningTimeNanos);
            builder.field("query", query);
            builder.endObject();
            return builder;
        }
    }

    public EsqlListQueriesResponse(List<Query> queries) {
        this.queries = queries;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new AssertionError("should not reach here");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("queries");
        for (Query query : queries) {
            query.toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
