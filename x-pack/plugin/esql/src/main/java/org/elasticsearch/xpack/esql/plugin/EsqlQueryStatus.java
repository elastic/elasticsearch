/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;

import java.io.IOException;

public record EsqlQueryStatus(AsyncExecutionId id) implements Task.Status {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Task.Status.class,
        "EsqlDocIdStatus",
        EsqlQueryStatus::new
    );

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    private EsqlQueryStatus(StreamInput stream) throws IOException {
        this(AsyncExecutionId.readFrom(stream));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        id.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("request_id", id.getEncoded()).endObject();
    }
}
