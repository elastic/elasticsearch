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
import java.util.Objects;

/**
 * Record for storing context alongside an inference request, typically used for metadata.
 * This is mainly used to pass along inference context on the transport layer without relying on
 * {@link org.elasticsearch.common.util.concurrent.ThreadContext}, which depending on the internal
 * {@link org.elasticsearch.client.internal.Client} throws away parts of the context, when passed along the transport layer.
 *
 * @param productUseCase - for now mainly used by Elastic Inference Service
 */
public record InferenceContext(String productUseCase) implements Writeable, ToXContent {

    public static final InferenceContext EMPTY_INSTANCE = new InferenceContext("");

    public InferenceContext {
        Objects.requireNonNull(productUseCase);
    }

    public InferenceContext(StreamInput in) throws IOException {
        this(in.readString());
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceContext that = (InferenceContext) o;
        return Objects.equals(productUseCase, that.productUseCase);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(productUseCase);
    }
}
