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
 * <p>
 * Components are never null; an empty string means "not set".
 *
 * @param productUseCase - for now mainly used by Elastic Inference Service
 * @param productSolution - the originating Elastic solution, such as security or observability
 * @param productFeature - the stable inference feature identifier
 * @param interactionId - an identifier used to attribute related inference requests
 */
public record InferenceContext(String productUseCase, String productSolution, String productFeature, String interactionId)
    implements
        Writeable,
        ToXContent {

    public static final InferenceContext EMPTY_INSTANCE = new InferenceContext("");

    public InferenceContext {
        Objects.requireNonNull(productUseCase);
        Objects.requireNonNull(productSolution);
        Objects.requireNonNull(productFeature);
        Objects.requireNonNull(interactionId);
    }

    public InferenceContext(String productUseCase) {
        this(productUseCase, "", "", "");
    }

    public InferenceContext(StreamInput in) throws IOException {
        this(in.readString(), in.readString(), in.readString(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(productUseCase);
        out.writeString(productSolution);
        out.writeString(productFeature);
        out.writeString(interactionId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("product_use_case", productUseCase);
        builder.field("product_solution", productSolution);
        builder.field("product_feature", productFeature);
        builder.field("interaction_id", interactionId);

        builder.endObject();

        return builder;
    }
}
