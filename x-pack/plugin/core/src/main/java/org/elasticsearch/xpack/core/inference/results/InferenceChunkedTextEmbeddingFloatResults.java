/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlChunkedTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.utils.FloatConversionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingUtils.validateInputSizeAgainstEmbeddings;

public record InferenceChunkedTextEmbeddingFloatResults(List<InferenceFloatEmbeddingChunk> chunks)
    implements
        ChunkedInferenceServiceResults {

    public static final String NAME = "chunked_text_embedding_service_float_results";
    public static final String FIELD_NAME = "text_embedding_float_chunk";

    public InferenceChunkedTextEmbeddingFloatResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(InferenceFloatEmbeddingChunk::new));
    }

    /**
     * Returns a list of {@link InferenceChunkedTextEmbeddingFloatResults}.
     * Each {@link InferenceChunkedTextEmbeddingFloatResults} contain a single chunk with the text and the
     * {@link InferenceTextEmbeddingFloatResults}.
     */
    public static List<ChunkedInferenceServiceResults> listOf(List<String> inputs, InferenceTextEmbeddingFloatResults textEmbeddings) {
        validateInputSizeAgainstEmbeddings(inputs, textEmbeddings.embeddings().size());

        var results = new ArrayList<ChunkedInferenceServiceResults>(inputs.size());

        for (int i = 0; i < inputs.size(); i++) {
            results.add(
                new InferenceChunkedTextEmbeddingFloatResults(
                    List.of(new InferenceFloatEmbeddingChunk(inputs.get(i), textEmbeddings.embeddings().get(i).values()))
                )
            );
        }

        return results;
    }

    public static InferenceChunkedTextEmbeddingFloatResults ofMlResults(MlChunkedTextEmbeddingFloatResults mlInferenceResult) {
        return new InferenceChunkedTextEmbeddingFloatResults(
            mlInferenceResult.getChunks()
                .stream()
                .map(chunk -> new InferenceFloatEmbeddingChunk(chunk.matchedText(), FloatConversionUtils.floatArrayOf(chunk.embedding())))
                .toList()
        );
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        // TODO add isTruncated flag
        return ChunkedToXContentHelper.array(FIELD_NAME, chunks.iterator());
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

    public List<InferenceFloatEmbeddingChunk> getChunks() {
        return chunks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceChunkedTextEmbeddingFloatResults that = (InferenceChunkedTextEmbeddingFloatResults) o;
        return Objects.equals(chunks, that.chunks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunks);
    }

    public record InferenceFloatEmbeddingChunk(String matchedText, float[] embedding) implements Writeable, ToXContentObject {

        public InferenceFloatEmbeddingChunk(StreamInput in) throws IOException {
            this(in.readString(), in.readFloatArray());
        }

        public static InferenceFloatEmbeddingChunk of(String matchedText, double[] doubleEmbedding) {
            return new InferenceFloatEmbeddingChunk(matchedText, FloatConversionUtils.floatArrayOf(doubleEmbedding));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(matchedText);
            out.writeFloatArray(embedding);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ChunkedNlpInferenceResults.TEXT, matchedText);

            builder.startArray(ChunkedNlpInferenceResults.INFERENCE);
            for (float value : embedding) {
                builder.value(value);
            }
            builder.endArray();

            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferenceFloatEmbeddingChunk that = (InferenceFloatEmbeddingChunk) o;
            return Objects.equals(matchedText, that.matchedText) && Arrays.equals(embedding, that.embedding);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(matchedText);
            result = 31 * result + Arrays.hashCode(embedding);
            return result;
        }
    }

    @Override
    public Iterator<Chunk> chunksAsMatchedTextAndByteReference(XContent xcontent) {
        return chunks.stream().map(chunk -> new Chunk(chunk.matchedText(), toBytesReference(xcontent, chunk.embedding()))).iterator();
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
