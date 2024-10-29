/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextExpansionResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingUtils.validateInputSizeAgainstEmbeddings;

public class InferenceChunkedSparseEmbeddingResults implements ChunkedInferenceServiceResults {

    public static final String NAME = "chunked_sparse_embedding_results";
    public static final String FIELD_NAME = "sparse_embedding_chunk";

    public static InferenceChunkedSparseEmbeddingResults ofMlResult(MlChunkedTextExpansionResults mlInferenceResults) {
        return new InferenceChunkedSparseEmbeddingResults(mlInferenceResults.getChunks());
    }

    /**
     * Returns a list of {@link InferenceChunkedSparseEmbeddingResults}. The number of entries in the list will match the input list size.
     * Each {@link InferenceChunkedSparseEmbeddingResults} will have a single chunk containing the entire results from the
     * {@link SparseEmbeddingResults}.
     */
    public static List<ChunkedInferenceServiceResults> listOf(List<String> inputs, SparseEmbeddingResults sparseEmbeddingResults) {
        validateInputSizeAgainstEmbeddings(inputs, sparseEmbeddingResults.embeddings().size());

        var results = new ArrayList<ChunkedInferenceServiceResults>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            results.add(ofSingle(inputs.get(i), sparseEmbeddingResults.embeddings().get(i)));
        }

        return results;
    }

    private static InferenceChunkedSparseEmbeddingResults ofSingle(String input, SparseEmbeddingResults.Embedding embedding) {
        var weightedTokens = embedding.tokens()
            .stream()
            .map(weightedToken -> new WeightedToken(weightedToken.token(), weightedToken.weight()))
            .toList();

        return new InferenceChunkedSparseEmbeddingResults(List.of(new MlChunkedTextExpansionResults.ChunkedResult(input, weightedTokens)));
    }

    private final List<MlChunkedTextExpansionResults.ChunkedResult> chunkedResults;

    public InferenceChunkedSparseEmbeddingResults(List<MlChunkedTextExpansionResults.ChunkedResult> chunks) {
        this.chunkedResults = chunks;
    }

    public InferenceChunkedSparseEmbeddingResults(StreamInput in) throws IOException {
        this.chunkedResults = in.readCollectionAsList(MlChunkedTextExpansionResults.ChunkedResult::new);
    }

    public List<MlChunkedTextExpansionResults.ChunkedResult> getChunkedResults() {
        return chunkedResults;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContent.builder(params).array(FIELD_NAME, chunkedResults.iterator());
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
        return Map.of(
            FIELD_NAME,
            chunkedResults.stream().map(MlChunkedTextExpansionResults.ChunkedResult::asMap).collect(Collectors.toList())
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceChunkedSparseEmbeddingResults that = (InferenceChunkedSparseEmbeddingResults) o;
        return Objects.equals(chunkedResults, that.chunkedResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunkedResults);
    }

    @Override
    public Iterator<Chunk> chunksAsMatchedTextAndByteReference(XContent xcontent) {
        return chunkedResults.stream()
            .map(chunk -> new Chunk(chunk.matchedText(), toBytesReference(xcontent, chunk.weightedTokens())))
            .iterator();
    }

    /**
     * Serialises the {@link WeightedToken} list, according to the provided {@link XContent},
     * into a {@link BytesReference}.
     */
    private static BytesReference toBytesReference(XContent xContent, List<WeightedToken> tokens) {
        try {
            XContentBuilder b = XContentBuilder.builder(xContent);
            b.startObject();
            for (var weightedToken : tokens) {
                weightedToken.toXContent(b, ToXContent.EMPTY_PARAMS);
            }
            b.endObject();
            return BytesReference.bytes(b);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
}
