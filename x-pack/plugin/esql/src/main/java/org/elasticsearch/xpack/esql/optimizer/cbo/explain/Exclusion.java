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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * An alternative that was never enumerated for a decision, so a reader can tell "not considered" apart from "considered and lost".
 */
public record Exclusion(String name, ExclusionCode code, @Nullable String reason) implements Writeable, ToXContentObject {

    public Exclusion {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(code, "code");
    }

    public Exclusion(StreamInput in) throws IOException {
        this(in.readString(), in.readEnum(ExclusionCode.class), in.readOptionalString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeEnum(code);
        out.writeOptionalString(reason);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("name", name);
        b.field("code", code.displayName());
        ExplainXContent.optionalField(b, "reason", reason);
        return b.endObject();
    }
}
