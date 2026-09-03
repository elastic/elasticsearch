/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.TransportVersion;
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
 * @param interactionId - an identifier used to attribute related inference requests
 * @param productSolution - the originating Elastic solution, such as security or observability
 * @param productFeature - the stable inference feature identifier
 */
public record InferenceContext(String productUseCase, String interactionId, String productSolution, String productFeature)
    implements
        Writeable,
        ToXContent {

    public static final TransportVersion INFERENCE_ATTRIBUTION_HEADERS_ADDED = TransportVersion.fromName(
        "inference_attribution_headers_added"
    );

    public static final InferenceContext EMPTY_INSTANCE = new InferenceContext("");

    public InferenceContext {
        Objects.requireNonNull(productUseCase);
        Objects.requireNonNull(interactionId);
        Objects.requireNonNull(productSolution);
        Objects.requireNonNull(productFeature);
    }

    public InferenceContext(String productUseCase) {
        this(productUseCase, "", "", "");
    }

    public InferenceContext(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.getTransportVersion().supports(INFERENCE_ATTRIBUTION_HEADERS_ADDED) ? in.readString() : "",
            in.getTransportVersion().supports(INFERENCE_ATTRIBUTION_HEADERS_ADDED) ? in.readString() : "",
            in.getTransportVersion().supports(INFERENCE_ATTRIBUTION_HEADERS_ADDED) ? in.readString() : ""
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(productUseCase);
        if (out.getTransportVersion().supports(INFERENCE_ATTRIBUTION_HEADERS_ADDED)) {
            out.writeString(interactionId);
            out.writeString(productSolution);
            out.writeString(productFeature);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("product_use_case", productUseCase);
        builder.field("interaction_id", interactionId);
        builder.field("product_solution", productSolution);
        builder.field("product_feature", productFeature);

        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceContext that = (InferenceContext) o;
        return Objects.equals(productUseCase, that.productUseCase)
            && Objects.equals(interactionId, that.interactionId)
            && Objects.equals(productSolution, that.productSolution)
            && Objects.equals(productFeature, that.productFeature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productUseCase, interactionId, productSolution, productFeature);
    }
}
