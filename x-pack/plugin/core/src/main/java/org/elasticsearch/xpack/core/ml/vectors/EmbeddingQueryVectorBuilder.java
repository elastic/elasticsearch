/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.vectors;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class EmbeddingQueryVectorBuilder implements QueryVectorBuilder {
    public static final String NAME = "embedding";

    public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField FORMAT_FIELD = new ParseField("format");
    public static final ParseField VALUE_FIELD = new ParseField("value");

    private static final TransportVersion EMBEDDING_QUERY_VECTOR_BUILDER = TransportVersion.fromName("embedding_query_vector_builder");

    private final String inferenceId;
    private final InferenceString.DataType type;
    private final InferenceString.DataFormat format;
    private final String value;

    public EmbeddingQueryVectorBuilder(InferenceString.DataType type, String value) {
        this(null, type, null, value);
    }

    public EmbeddingQueryVectorBuilder(
        @Nullable String inferenceId,
        InferenceString.DataType type,
        @Nullable InferenceString.DataFormat format,
        String value
    ) {
        this.inferenceId = inferenceId;
        this.type = Objects.requireNonNull(type);
        this.format = format;
        this.value = Objects.requireNonNull(value);
    }

    public EmbeddingQueryVectorBuilder(StreamInput in) throws IOException {
        this.inferenceId = in.readOptionalString();
        this.type = in.readEnum(InferenceString.DataType.class);
        this.format = in.readOptionalEnum(InferenceString.DataFormat.class);
        this.value = in.readString();
    }

    @Override
    public void buildVector(Client client, ActionListener<float[]> listener) {
        // TODO: Implement
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return EMBEDDING_QUERY_VECTOR_BUILDER;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(inferenceId);
        out.writeEnum(type);
        out.writeOptionalEnum(format);
        out.writeString(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inferenceId != null) {
            builder.field(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
        }
        builder.field(TYPE_FIELD.getPreferredName(), type);
        if (format != null) {
            builder.field(FORMAT_FIELD.getPreferredName(), format);
        }
        builder.field(VALUE_FIELD.getPreferredName(), value);
        builder.endObject();
        return builder;
    }
}
