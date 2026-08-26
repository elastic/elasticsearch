/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GenericQueryVectorBuilder implements QueryVectorBuilder {
    public static final String NAME = "generic_query_vector_builder";

    private final float[] queryVector;
    private final Exception error;

    public GenericQueryVectorBuilder(float[] queryVector) {
        this.queryVector = Objects.requireNonNull(queryVector);
        this.error = null;
    }

    public GenericQueryVectorBuilder(Exception error) {
        this.queryVector = null;
        this.error = Objects.requireNonNull(error);
    }

    public GenericQueryVectorBuilder(StreamInput in) throws IOException {
        this.queryVector = in.readOptionalFloatArray();
        this.error = in.readOptionalException();
    }

    @Override
    public void buildVector(Client client, ActionListener<float[]> listener) {
        if (error != null) {
            listener.onFailure(error);
        } else {
            listener.onResponse(queryVector);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.current();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalFloatArray(queryVector);
        out.writeOptionalException(error);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (queryVector != null) {
            builder.array("query_vector", queryVector);
        }
        builder.endObject();
        return builder;
    }
}
