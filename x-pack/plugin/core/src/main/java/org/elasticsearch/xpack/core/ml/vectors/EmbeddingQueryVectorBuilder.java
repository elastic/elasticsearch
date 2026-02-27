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
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class EmbeddingQueryVectorBuilder implements QueryVectorBuilder {
    public static final String NAME = "embedding";

    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField FORMAT_FIELD = new ParseField("format");
    public static final ParseField VALUE_FIELD = new ParseField("value");

    private static final TransportVersion EMBEDDING_QUERY_VECTOR_BUILDER = TransportVersion.fromName("embedding_query_vector_builder");

    private final InferenceString.DataType type;
    private final InferenceString.DataFormat format;
    private final String value;

    public EmbeddingQueryVectorBuilder(InferenceString.DataType type, String value) {
        // TODO: Get default format
        this(type, null, value);
    }

    public EmbeddingQueryVectorBuilder(InferenceString.DataType type, InferenceString.DataFormat format, String value) {
        this.type = Objects.requireNonNull(type);
        this.format = Objects.requireNonNull(format);
        this.value = Objects.requireNonNull(value);
    }

    public EmbeddingQueryVectorBuilder(StreamInput in) throws IOException {
        this.type = in.readEnum(InferenceString.DataType.class);
        this.format = in.readEnum(InferenceString.DataFormat.class);
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
        // TODO: Implement
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // TODO: Implement
        return null;
    }
}
