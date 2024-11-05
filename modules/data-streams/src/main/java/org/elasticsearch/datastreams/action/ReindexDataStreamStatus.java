/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ReindexDataStreamStatus implements Task.Status {
    private boolean success = false;
    private Exception exception = null;

    public ReindexDataStreamStatus() {
        super();
    }

    public ReindexDataStreamStatus(boolean success, Exception exception) {
        this();
        this.success = success;
        this.exception = exception;
    }

    public ReindexDataStreamStatus(StreamInput in) {

    }

    public static final String NAME = "ReindexDataStreamStatus";

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("complete", success || exception != null);
        if (exception != null) {
            builder.field("exception", exception.getMessage());
        }
        builder.endObject();
        return builder;
    }
}
