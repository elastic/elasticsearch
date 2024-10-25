/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BatchedChunkedInferenceServiceResults implements InferenceServiceResults {
    public static final String NAME = "batched_chunked_inference_service_results";
    private final TaskType taskType;
    private final List<ChunkedInferenceServiceResults> chunkedInferenceServiceResults;

    public BatchedChunkedInferenceServiceResults(TaskType taskType, List<ChunkedInferenceServiceResults> chunkedInferenceServiceResults) {
        this.taskType = taskType;
        this.chunkedInferenceServiceResults = chunkedInferenceServiceResults;
    }

    public BatchedChunkedInferenceServiceResults(StreamInput in) throws IOException {
        // TODO: Figure out how to do this given that you don't know the type of the chunkedInferenceServiceResults
        this.taskType = in.readEnum(TaskType.class);
        ; // TODO

        switch (taskType) {
            case TEXT_EMBEDDING:
                this.chunkedInferenceServiceResults = in.readCollectionAsList(InferenceChunkedTextEmbeddingByteResults::new);
                break;
            case SPARSE_EMBEDDING:
                this.chunkedInferenceServiceResults = in.readCollectionAsList(InferenceChunkedSparseEmbeddingResults::new);
                break;
            default:
                throw new IllegalArgumentException("Unknown task type: " + taskType);
        }

        // TODO: What about BYTE chunked results? Seems like we don't use them anymore? Ask about this.
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public List<ChunkedInferenceServiceResults> getChunkedInferenceServiceResults() {
        return chunkedInferenceServiceResults;
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return List.of();
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        return List.of();
    }

    @Override
    public Map<String, Object> asMap() {
        return Map.of();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(taskType);
        out.writeCollection(chunkedInferenceServiceResults, StreamOutput::writeWriteable); // TODO: Is this correct?
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return chunkedInferenceServiceResults.stream()
            .map(result -> result.toXContentChunked(params))
            .reduce(Iterators::concat)
            .orElseThrow(() -> new RuntimeException("TODO"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchedChunkedInferenceServiceResults that = (BatchedChunkedInferenceServiceResults) o;
        return Objects.equals(taskType, that.getTaskType())
            && Objects.equals(chunkedInferenceServiceResults, that.getChunkedInferenceServiceResults());
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskType, chunkedInferenceServiceResults);
    }
}
