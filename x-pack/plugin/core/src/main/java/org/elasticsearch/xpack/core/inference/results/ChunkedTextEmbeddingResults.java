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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ChunkedTextEmbeddingResults implements ChunkedInferenceServiceResults {

    public static final String NAME = "chunked_text_embedding_service_results";

    public static ChunkedTextEmbeddingResults ofMlResult(
        org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults mlInferenceResults
    ) {
        return new ChunkedTextEmbeddingResults(mlInferenceResults.getChunks());
    }

    private final List<org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk> chunks;

    public ChunkedTextEmbeddingResults(
        List<org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk> chunks
    ) {
        this.chunks = chunks;
    }

    public ChunkedTextEmbeddingResults(StreamInput in) throws IOException {
        this.chunks = in.readCollectionAsList(
            org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk::new
        );
    }

    public List<org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk> getChunks() {
        return chunks;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("text_embedding_chunk");
        for (var embedding : chunks) {
            embedding.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(chunks);
    }

    @Override
    public String getWriteableName() {
        return NAME;
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
        ChunkedTextEmbeddingResults that = (ChunkedTextEmbeddingResults) o;
        return Objects.equals(chunks, that.chunks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunks);
    }
}
