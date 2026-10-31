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
 * Another decision point whose choice changed in an alternative's counterfactual plan, which explains why an alternative
 * that looks cheap locally can still lose on total plan cost.
 */
public record DecisionRef(String id, String chosen) implements Writeable, ToXContentObject {

    public DecisionRef {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(chosen, "chosen");
    }

    public DecisionRef(StreamInput in) throws IOException {
        this(in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(chosen);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("id", id);
        b.field("chosen", chosen);
        return b.endObject();
    }
}
