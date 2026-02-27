/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.CONTENT_FIELD;

public record ContentString(String content) implements Content, NamedWriteable {
    public static final String NAME = "content_string";

    public static ContentString of(XContentParser parser) throws IOException {
        var content = parser.text();
        return new ContentString(content);
    }

    public ContentString(StreamInput in) throws IOException {
        this(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(content);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public String toString() {
        return content;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(CONTENT_FIELD, content);
    }

    @Override
    public boolean containsMultimodalContent() {
        return false;
    }
}
