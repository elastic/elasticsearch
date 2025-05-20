/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record TransformSchedulerStats(int registeredTransformCount, String peekTransformName) implements ToXContent, Writeable {

    public static final String REGISTERED_TRANSFORM_COUNT_FIELD_NAME = "registered_transform_count";
    public static final String PEEK_TRANSFORM_FIELD_NAME = "peek_transform";

    public TransformSchedulerStats(StreamInput in) throws IOException {
        this(in.readVInt(), in.readOptionalString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.registeredTransformCount);
        out.writeOptionalString(this.peekTransformName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REGISTERED_TRANSFORM_COUNT_FIELD_NAME, this.registeredTransformCount);
        builder.field(PEEK_TRANSFORM_FIELD_NAME, this.peekTransformName);
        return builder.endObject();
    }
}
