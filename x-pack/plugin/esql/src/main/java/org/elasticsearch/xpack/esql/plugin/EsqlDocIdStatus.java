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
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;

import java.io.IOException;

/**
 * Keeps track of the doc ID, which is itself used for {@link AsyncExecutionId}.
 *
 * The reason this contains just the doc ID and not the entire {@link AsyncExecutionId} is that during the creation of
 * {@link EsqlQueryAction}, we don't have access to the node ID yet, thus we can't create a {@link TaskId} yet.
 */
public record EsqlDocIdStatus(String id) implements Task.Status {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Task.Status.class,
        "EsqlDocIdStatus",
        EsqlDocIdStatus::new
    );

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    private EsqlDocIdStatus(StreamInput stream) throws IOException {
        this(stream.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.rawValue(id);
    }
}
