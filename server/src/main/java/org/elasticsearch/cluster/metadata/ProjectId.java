/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public record ProjectId(String id) implements Writeable, ToXContent {

    public static final Reader<ProjectId> READER = ProjectId::new;

    public ProjectId(StreamInput in) throws IOException {
        this(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(id);
    }

    public static ProjectId fromXContent(XContentParser parser) throws IOException {
        return new ProjectId(parser.text());
    }

    public static ProjectId ofNullable(@Nullable String id, @Nullable ProjectId fallback) {
        return id == null ? fallback : new ProjectId(id);
    }

    @Override
    public String toString() {
        return this.id;
    }
}
