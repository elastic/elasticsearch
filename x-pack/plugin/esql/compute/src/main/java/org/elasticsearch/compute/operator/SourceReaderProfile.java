/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Per-factory format-reader profile, shipped back from data nodes as part of
 * {@link DriverCompletionInfo#sourceReaderProfiles()}.
 */
public record SourceReaderProfile(String source, long readNanos, long readCpuNanos) implements Writeable, ToXContentObject {

    public static SourceReaderProfile readFrom(StreamInput in) throws IOException {
        return new SourceReaderProfile(in.readString(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(source);
        out.writeVLong(readNanos);
        out.writeVLong(readCpuNanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("source", source);
        builder.field("read_nanos", readNanos);
        builder.field("read_cpu_nanos", readCpuNanos);
        return builder.endObject();
    }
}
