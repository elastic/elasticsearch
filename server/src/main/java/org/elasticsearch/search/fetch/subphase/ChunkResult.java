/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Represents a single scored chunk from a semantic text field, including its text content,
 * byte offsets within the original field value, and relevance score.
 */
public record ChunkResult(String text, int startOffset, int endOffset, float score) implements ToXContentObject, Writeable {

    public ChunkResult {
        if (endOffset < startOffset) {
            throw new IllegalArgumentException("endOffset [" + endOffset + "] must be >= startOffset [" + startOffset + "]");
        }
    }

    public ChunkResult(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt(), in.readVInt(), in.readFloat());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(text);
        out.writeVInt(startOffset);
        out.writeVInt(endOffset);
        out.writeFloat(score);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("text", text);
        builder.field("start_offset", startOffset);
        builder.field("end_offset", endOffset);
        builder.field("score", score);
        builder.endObject();
        return builder;
    }
}
