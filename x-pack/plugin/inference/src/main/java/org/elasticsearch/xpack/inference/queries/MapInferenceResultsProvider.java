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

public class MapInferenceResultsProvider implements InferenceResultsProvider {
    public static final String NAME = "map_inference_results_provider";

    private final Map<String, InferenceResults> inferenceResultsMap;

    public MapInferenceResultsProvider() {
        this.inferenceResultsMap = new ConcurrentHashMap<>();
    }

    public MapInferenceResultsProvider(StreamInput in) throws IOException {
        this.inferenceResultsMap = in.readImmutableMap(i -> i.readNamedWriteable(InferenceResults.class));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(inferenceResultsMap, StreamOutput::writeNamedWriteable);
    }

    @Override
    public InferenceResults getInferenceResults(String inferenceId) {
        return inferenceResultsMap.get(inferenceId);
    }

    public void addInferenceResults(String inferenceId, InferenceResults embeddings) {
        this.inferenceResultsMap.put(inferenceId, embeddings);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MapInferenceResultsProvider that = (MapInferenceResultsProvider) o;
        return Objects.equals(inferenceResultsMap, that.inferenceResultsMap);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inferenceResultsMap);
    }
}
