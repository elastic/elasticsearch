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
import org.elasticsearch.inference.InferenceRequestMetadata.Field;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InferenceRequestMetadata.Field.INTERACTION_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_FEATURE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_SOLUTION;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.PRODUCT_USE_CASE;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.SPACE_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.TRACE_ID;
import static org.elasticsearch.inference.InferenceRequestMetadata.Field.USER_ID;

/**
 * Request-payload carrier for the {@link InferenceRequestMetadata} fields the inference plugin propagates.
 * It travels on the request object because {@code stashWithOrigin} clears ordinary
 * {@link org.elasticsearch.common.util.concurrent.ThreadContext} headers before the inference action runs.
 * Product origin is not carried here; core preserves it across the stash.
 */
public record InferenceContext(InferenceRequestMetadata metadata) implements Writeable, ToXContent {

    /**
     * Stream and XContent layout. These POST inference actions run on the local node, so this layout is not a
     * mixed-cluster contract. The outer {@code inference_context} gate does not make a later field addition
     * compatible with a peer that already understands that version. A caller that sends this request to another
     * node has to version the layout first. The order is listed explicitly; enum order is not the wire layout.
     */
    static final List<Field> WIRE_LAYOUT = List.of(
        PRODUCT_USE_CASE,
        PRODUCT_SOLUTION,
        PRODUCT_FEATURE,
        INTERACTION_ID,
        TRACE_ID,
        USER_ID,
        SPACE_ID
    );

    public static final InferenceContext EMPTY_INSTANCE = new InferenceContext(InferenceRequestMetadata.EMPTY);

    public InferenceContext {
        Objects.requireNonNull(metadata);
        metadata.forEachPresent((field, value) -> {
            if (WIRE_LAYOUT.contains(field) == false) {
                throw new IllegalArgumentException("inference context cannot carry [" + field.xContentName() + "]");
            }
        });
    }

    public InferenceContext(String productUseCase) {
        this(InferenceRequestMetadata.builder().put(PRODUCT_USE_CASE, Objects.requireNonNull(productUseCase)).build());
    }

    public InferenceContext(StreamInput in) throws IOException {
        this(readMetadata(in));
    }

    private static InferenceRequestMetadata readMetadata(StreamInput in) throws IOException {
        var builder = InferenceRequestMetadata.builder();
        for (var field : WIRE_LAYOUT) {
            builder.put(field, in.readString());
        }
        return builder.build();
    }

    /**
     * Absent values are written as empty strings.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        for (var field : WIRE_LAYOUT) {
            out.writeString(valueOrEmpty(field));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (var field : WIRE_LAYOUT) {
            builder.field(field.xContentName(), valueOrEmpty(field));
        }
        builder.endObject();
        return builder;
    }

    private String valueOrEmpty(Field field) {
        var value = metadata.get(field);
        return value == null ? "" : value;
    }

    @Override
    public String toString() {
        return metadata.toString();
    }
}
