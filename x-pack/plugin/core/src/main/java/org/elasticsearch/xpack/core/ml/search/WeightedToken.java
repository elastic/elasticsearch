/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record WeightedToken(String token, float weight) implements Writeable, ToXContentFragment {

    public WeightedToken(StreamInput in) throws IOException {
        this(in.readString(), in.readFloat());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(token);
        out.writeFloat(weight);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(token, weight);
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
