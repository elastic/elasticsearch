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
import org.elasticsearch.inference.telemetry.InferenceProductContext;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_INFERENCE_INTERACTION_ID_HTTP_HEADER;
import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_FEATURE_HTTP_HEADER;
import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_SOLUTION_HTTP_HEADER;
import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER;

/**
 * Record for storing context alongside an inference request, typically used for metadata.
 * This is mainly used to pass along inference context on the transport layer without relying on
 * {@link org.elasticsearch.common.util.concurrent.ThreadContext}, which depending on the internal
 * {@link org.elasticsearch.client.internal.Client} throws away parts of the context, when passed along the transport layer.
 * <p>
 * Components are never null; an empty string means "not set". The component order matches
 * {@link InferenceProductContext} so the two can be mapped without reordering.
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

    /**
     * Reads the attribution headers off an incoming REST request. A header that is absent or has no value yields an empty string.
     * Only the first value of each header is used.
     */
    public static InferenceContext fromHeaders(Map<String, List<String>> headers) {
        return new InferenceContext(
            firstHeaderValue(headers, X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER),
            firstHeaderValue(headers, X_ELASTIC_PRODUCT_SOLUTION_HTTP_HEADER),
            firstHeaderValue(headers, X_ELASTIC_PRODUCT_FEATURE_HTTP_HEADER),
            firstHeaderValue(headers, X_ELASTIC_INFERENCE_INTERACTION_ID_HTTP_HEADER)
        );
    }

    private static String firstHeaderValue(Map<String, List<String>> headers, String headerName) {
        if (headers == null) {
            return "";
        }

        var values = headers.get(headerName);

        return values == null || values.isEmpty() ? "" : values.get(0);
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

    /**
     * The attribution that is set, keyed by the thread context header carrying it. Delegates to {@link InferenceProductContext}
     * so the field to header mapping lives in exactly one place.
     */
    public Map<String, String> attributionHeaders() {
        return new InferenceProductContext(productUseCase, null, productSolution, productFeature, interactionId).headers();
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
