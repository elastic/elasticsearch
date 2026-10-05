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
import org.elasticsearch.inference.InferenceRequestMetadata;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.inference.InferenceRequestMetadata.Field.INTERACTION_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_FEATURE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_SOLUTION;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_USE_CASE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.SPACE_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.TRACE_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.USER_ID;

/**
 * Transport and XContent adapter for {@link InferenceRequestMetadata}.
 * This is mainly used to pass along inference context on the transport layer without relying on
 * {@link org.elasticsearch.common.util.concurrent.ThreadContext}, which depending on the internal
 * {@link org.elasticsearch.client.internal.Client} throws away parts of the context, when passed along the transport layer.
 */
public final class InferenceContext implements Writeable, ToXContent {

    public static final InferenceContext EMPTY_INSTANCE = new InferenceContext("");

    private final InferenceRequestMetadata metadata;

    public InferenceContext(InferenceRequestMetadata metadata) {
        this.metadata = Objects.requireNonNull(metadata);
    }

    public InferenceContext(String productUseCase) {
        this(InferenceRequestMetadata.builder().put(PRODUCT_USE_CASE, Objects.requireNonNull(productUseCase)).build());
    }

    public InferenceContext(StreamInput in) throws IOException {
        this(readMetadata(in));
    }

    public InferenceRequestMetadata metadata() {
        return metadata;
    }

    /**
     * These POST inference actions run on the local node, so this layout is not a mixed-cluster contract.
     * The outer {@code inference_context} gate does not make a later field addition compatible with a peer
     * that already understands that version. A caller that sends this request to another node has to version
     * the layout first. Components are listed by name; enum order is not the wire layout.
     */
    private static InferenceRequestMetadata readMetadata(StreamInput in) throws IOException {
        var productUseCase = in.readString();
        var productSolution = in.readString();
        var productFeature = in.readString();
        var interactionId = in.readString();
        var traceId = in.readString();
        var userId = in.readString();
        var spaceId = in.readString();
        return InferenceRequestMetadata.builder()
            .put(PRODUCT_USE_CASE, productUseCase)
            .put(PRODUCT_SOLUTION, productSolution)
            .put(PRODUCT_FEATURE, productFeature)
            .put(INTERACTION_ID, interactionId)
            .put(TRACE_ID, traceId)
            .put(USER_ID, userId)
            .put(SPACE_ID, spaceId)
            .build();
    }

    /**
     * These POST inference actions run on the local node, so this layout is not a mixed-cluster contract.
     * The outer {@code inference_context} gate does not make a later field addition compatible with a peer
     * that already understands that version. A caller that sends this request to another node has to version
     * the layout first. Components are listed by name; enum order is not the wire layout. Absent values are
     * written as empty strings.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(valueOrEmpty(PRODUCT_USE_CASE));
        out.writeString(valueOrEmpty(PRODUCT_SOLUTION));
        out.writeString(valueOrEmpty(PRODUCT_FEATURE));
        out.writeString(valueOrEmpty(INTERACTION_ID));
        out.writeString(valueOrEmpty(TRACE_ID));
        out.writeString(valueOrEmpty(USER_ID));
        out.writeString(valueOrEmpty(SPACE_ID));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (var field : InferenceRequestMetadata.Field.values()) {
            builder.field(field.xContentName(), valueOrEmpty(field));
        }
        builder.endObject();
        return builder;
    }

    private String valueOrEmpty(InferenceRequestMetadata.Field field) {
        var value = metadata.get(field);
        return value == null ? "" : value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return metadata.equals(((InferenceContext) o).metadata);
    }

    @Override
    public int hashCode() {
        return metadata.hashCode();
    }

    @Override
    public String toString() {
        return metadata.toString();
    }
}
