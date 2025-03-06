/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

// TODO: docs
public record InferenceContext(String productUseCase) implements Writeable, ToXContent {

    public InferenceContext(StreamInput in) throws IOException {
        this(in.readString());
    }

    public static InferenceContext empty() {
        return new InferenceContext("");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(productUseCase);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("product_use_case", productUseCase);

        builder.endObject();

        return builder;
    }
}
