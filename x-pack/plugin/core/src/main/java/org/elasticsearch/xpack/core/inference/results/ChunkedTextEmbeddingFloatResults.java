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
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public record ChunkedTextEmbeddingFloatResults(List<EmbeddingChunk<FloatEmbedding.FloatArrayWrapper>> chunks)
    implements
        ChunkedInferenceServiceResults {

    public static final String NAME = "chunked_text_embedding_service_float_results";
    public static final String FIELD_NAME = "text_embedding_float_chunk";

    public static ChunkedTextEmbeddingFloatResults ofMlResult(
        org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults mlInferenceResults
    ) {
        var chunks = new ArrayList<EmbeddingChunk<FloatEmbedding.FloatArrayWrapper>>();
        for (var mlChunk : mlInferenceResults.getChunks()) {
            chunks.add(new EmbeddingChunk<>(mlChunk.matchedText(), new FloatEmbedding(doubleArrayToFloat(mlChunk.embedding()))));
        }
        return new ChunkedTextEmbeddingFloatResults(chunks);
    }

    private static float[] doubleArrayToFloat(double[] arr) {
        var result = new float[arr.length];
        for (int i = 0; i < arr.length; i++) {
            result[i] = (float) arr[i];
        }
        return result;
    }

    public ChunkedTextEmbeddingFloatResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(in1 -> new EmbeddingChunk<>(in1.readString(), new FloatEmbedding(in1))));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
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

    public List<EmbeddingChunk<FloatEmbedding.FloatArrayWrapper>> getChunks() {
        return chunks;
    }

    @Override
    public Iterator<Chunk> chunksAsMatchedTextAndByteReference(XContent xcontent) {
        return chunks.stream()
            .map(chunk -> new Chunk(chunk.matchedText(), toBytesReference(xcontent, chunk.embedding().getEmbedding().getFloats())))
            .iterator();
    }

    /**
     * Serialises the {@code value} array, according to the provided {@link XContent}, into a {@link BytesReference}.
     */
    private static BytesReference toBytesReference(XContent xContent, float[] value) {
        try {
            XContentBuilder b = XContentBuilder.builder(xContent);
            b.startArray();
            for (float v : value) {
                b.value(v);
            }
            b.endArray();
            return BytesReference.bytes(b);
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }
}
