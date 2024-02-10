/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextExpansionResults;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ChunkedSparseEmbeddingResults implements ChunkedInferenceServiceResults {

    public static final String NAME = "chunked_sparse_embedding_results";

    public static ChunkedSparseEmbeddingResults ofMlResult(ChunkedTextExpansionResults mlInferenceResults) {
        return new ChunkedSparseEmbeddingResults(mlInferenceResults.getChunks());
    }

    private final List<ChunkedTextExpansionResults.ChunkedResult> chunkedResults;

    public ChunkedSparseEmbeddingResults(List<ChunkedTextExpansionResults.ChunkedResult> chunks) {
        this.chunkedResults = chunks;
    }

    public ChunkedSparseEmbeddingResults(StreamInput in) throws IOException {
        this.chunkedResults = in.readCollectionAsList(ChunkedTextExpansionResults.ChunkedResult::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("sparse_embedding_chunk");
        for (ChunkedTextExpansionResults.ChunkedResult chunk : chunkedResults) {
            chunk.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(chunkedResults);
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        throw new UnsupportedOperationException("Chunked results are not returned in the coordindated action");
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        throw new UnsupportedOperationException("Chunked results are not returned in the legacy format");
    }

    @Override
    public Map<String, Object> asMap() {
        throw new UnsupportedOperationException("Chunked results are not returned in the a map format");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkedSparseEmbeddingResults that = (ChunkedSparseEmbeddingResults) o;
        return Objects.equals(chunkedResults, that.chunkedResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunkedResults);
    }
}
