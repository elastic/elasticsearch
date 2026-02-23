/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Status of a {@link SideChannel}.
 *
 * @param type Type of the {@link SideChannel}. Think {@code min_competitive} or {@code limiter}.
 * @param status Status as reported by the {@link SideChannel}.
 */
public record SideChannelStatus(String type, int id, SideChannel.Status status) implements Writeable, ToXContentObject {
    public static SideChannelStatus readFrom(StreamInput in) throws IOException {
        return new SideChannelStatus(in.readString(), in.readInt(), in.readOptionalNamedWriteable(SideChannel.Status.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeInt(id);
        out.writeOptionalNamedWriteable(status != null && status.supportsVersion(out.getTransportVersion()) ? status : null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", type);
        builder.field("id", id);
        if (status != null) {
            builder.field("status", status);
        }
        return builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
