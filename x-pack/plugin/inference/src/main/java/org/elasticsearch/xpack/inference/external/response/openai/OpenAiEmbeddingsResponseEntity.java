/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.openai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.results.InferenceResult;

import java.io.IOException;
import java.util.Objects;

// TODO figure out the format for the response
public record OpenAiEmbeddingsResponseEntity(Embedding[] embeddings) implements InferenceResult {
    private static final String NAME = "openai_embeddings_response_entity";
    private static final String EMBEDDINGS_FIELD = "embeddings";

    public OpenAiEmbeddingsResponseEntity {
        Objects.requireNonNull(embeddings);
    }

    public record Embedding(float[] embedding) implements ToXContentObject, Writeable {
        private static final String EMBEDDING_FIELD = "embedding";

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.array(EMBEDDING_FIELD, embedding);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeFloatArray(embedding);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.array(EMBEDDINGS_FIELD, (Object[]) embeddings);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // TODO change this
        return TransportVersions.V_8_500_072;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(embeddings);
    }
}
