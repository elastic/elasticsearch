/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceResults;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class MapEmbeddingsProvider implements EmbeddingsProvider {
    public static final String NAME = "map_embeddings_provider";

    private final Map<String, InferenceResults> embeddings;

    public MapEmbeddingsProvider() {
        this.embeddings = new ConcurrentHashMap<>();
    }

    public MapEmbeddingsProvider(StreamInput in) throws IOException {
        this.embeddings = in.readImmutableMap(i -> i.readNamedWriteable(InferenceResults.class));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(embeddings, StreamOutput::writeNamedWriteable);
    }

    @Override
    public InferenceResults getEmbeddings(String inferenceId) {
        return embeddings.get(inferenceId);
    }

    public void addEmbeddings(String inferenceId, InferenceResults embeddings) {
        this.embeddings.put(inferenceId, embeddings);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MapEmbeddingsProvider that = (MapEmbeddingsProvider) o;
        return Objects.equals(embeddings, that.embeddings);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(embeddings);
    }
}
