/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * The location in the query text of the command that produced a decision point, so a reader can map a decision back to the query.
 */
public record SourceAnchor(String text, int line, int column) implements Writeable, ToXContentObject {

    public SourceAnchor {
        Objects.requireNonNull(text, "text");
    }

    public SourceAnchor(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt(), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(text);
        out.writeVInt(line);
        out.writeVInt(column);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("text", text);
        b.field("line", line);
        b.field("column", column);
        return b.endObject();
    }
}
