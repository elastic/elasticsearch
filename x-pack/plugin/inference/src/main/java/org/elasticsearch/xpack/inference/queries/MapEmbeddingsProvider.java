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
import java.util.HashMap;
import java.util.Map;

public class MapEmbeddingsProvider implements EmbeddingsProvider {
    public static final String NAME = "map_embeddings_provider";

    private final Map<InferenceEndpointKey, InferenceResults> embeddings;

    public MapEmbeddingsProvider() {
        this.embeddings = new HashMap<>();
    }

    public MapEmbeddingsProvider(StreamInput in) throws IOException {
        embeddings = in.readMap(InferenceEndpointKey::new, i -> i.readNamedWriteable(InferenceResults.class));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(embeddings);
    }

    @Override
    public InferenceResults getEmbeddings(InferenceEndpointKey key) {
        return embeddings.get(key);
    }

    public void addEmbeddings(InferenceEndpointKey key, InferenceResults embeddings) {
        this.embeddings.put(key, embeddings);
    }
}
