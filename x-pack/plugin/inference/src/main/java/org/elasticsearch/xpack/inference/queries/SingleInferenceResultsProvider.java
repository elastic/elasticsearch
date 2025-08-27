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
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class SingleInferenceResultsProvider implements InferenceResultsProvider {
    public static final String NAME = "single_inference_results_provider";

    private final InferenceResults inferenceResults;

    public SingleInferenceResultsProvider(InferenceResults inferenceResults) {
        Objects.requireNonNull(inferenceResults);
        this.inferenceResults = inferenceResults;
    }

    public SingleInferenceResultsProvider(StreamInput in) throws IOException {
        this.inferenceResults = in.readNamedWriteable(InferenceResults.class);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(inferenceResults);
    }

    @Override
    public InferenceResults getInferenceResults(String inferenceId) {
        return inferenceResults;
    }

    @Override
    public Collection<InferenceResults> getAllInferenceResults() {
        return List.of(inferenceResults);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SingleInferenceResultsProvider that = (SingleInferenceResultsProvider) o;
        return Objects.equals(inferenceResults, that.inferenceResults);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inferenceResults);
    }
}
