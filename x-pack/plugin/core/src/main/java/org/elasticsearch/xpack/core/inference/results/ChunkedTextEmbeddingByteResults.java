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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingUtils.validateInputSizeAgainstEmbeddings;

public record ChunkedTextEmbeddingByteResults(List<EmbeddingChunk<ByteEmbedding.ByteArrayWrapper>> chunks, boolean isTruncated)
    implements
        ChunkedInferenceServiceResults {

    public static final String NAME = "chunked_text_embedding_service_byte_results";
    public static final String FIELD_NAME = "text_embedding_byte_chunk";

    /**
     * Returns a list of {@link ChunkedTextEmbeddingByteResults}. The number of entries in the list will match the input list size.
     * Each {@link ChunkedTextEmbeddingByteResults} will have a single chunk containing the entire results from the
     * {@link TextEmbeddingByteResults}.
     */
    public static List<ChunkedInferenceServiceResults> of(List<String> inputs, TextEmbeddingByteResults textEmbeddings) {
        validateInputSizeAgainstEmbeddings(inputs, textEmbeddings.embeddings().size());

        var results = new ArrayList<ChunkedInferenceServiceResults>(inputs.size());
        for (int i = 0; i < inputs.size(); i++) {
            results.add(of(inputs.get(i), textEmbeddings.embeddings().get(i).getEmbedding().bytes));
        }

        return results;
    }

    public static ChunkedTextEmbeddingByteResults of(String input, byte[] byteEmbeddings) {
        return new ChunkedTextEmbeddingByteResults(List.of(new EmbeddingChunk<>(input, new ByteEmbedding(byteEmbeddings))), false);
    }

    public ChunkedTextEmbeddingByteResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(in1 -> new EmbeddingChunk<>(in1.readString(), new ByteEmbedding(in1))), in.readBoolean());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(FIELD_NAME);
        for (var embedding : chunks) {
            embedding.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(chunks);
        out.writeBoolean(isTruncated);
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        throw new UnsupportedOperationException("Chunked results are not returned in the coordinated action");
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        throw new UnsupportedOperationException("Chunked results are not returned in the legacy format");
    }

    @Override
    public Map<String, Object> asMap() {
        return Map.of(FIELD_NAME, chunks);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public List<EmbeddingChunk<ByteEmbedding.ByteArrayWrapper>> getChunks() {
        return chunks;
    }
}
