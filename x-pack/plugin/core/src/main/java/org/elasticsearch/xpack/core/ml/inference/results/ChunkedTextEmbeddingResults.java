/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ChunkedTextEmbeddingResults extends ChunkedNlpInferenceResults {

    public record EmbeddingChunk(String matchedText, double[] embedding) implements Writeable, ToXContentObject {

        public EmbeddingChunk(StreamInput in) throws IOException {
            this(in.readString(), in.readDoubleArray());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(matchedText);
            out.writeDoubleArray(embedding);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TEXT, matchedText);
            builder.field(INFERENCE, embedding);
            builder.endObject();
            return builder;
        }

        public Map<String, Object> asMap() {
            var map = new HashMap<String, Object>();
            map.put(TEXT, matchedText);
            map.put(INFERENCE, embedding);
            return map;
        }

        /**
         * It appears the default equals function for a record
         * does not call Arrays.equals() for the embedding array.
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EmbeddingChunk that = (EmbeddingChunk) o;
            return Objects.equals(matchedText, that.matchedText) && Arrays.equals(embedding, that.embedding);
        }

        /**
         * Use Arrays.hashCode() on the embedding array
         */
        @Override
        public int hashCode() {
            return Objects.hash(matchedText, Arrays.hashCode(embedding));
        }
    }

    public static final String NAME = "chunked_text_embedding_result";

    private final String resultsField;
    private final List<EmbeddingChunk> chunks;

    public ChunkedTextEmbeddingResults(String resultsField, List<EmbeddingChunk> embeddings, boolean isTruncated) {
        super(isTruncated);
        this.resultsField = resultsField;
        this.chunks = embeddings;
    }

    public ChunkedTextEmbeddingResults(StreamInput in) throws IOException {
        super(in);
        this.resultsField = in.readString();
        this.chunks = in.readCollectionAsList(EmbeddingChunk::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public Object predictedValue() {
        throw new UnsupportedOperationException("[" + NAME + "] does not support a single predicted value");
    }

    public List<EmbeddingChunk> getChunks() {
        return chunks;
    }

    @Override
    void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(resultsField);
        for (var chunk : chunks) {
            chunk.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(resultsField);
        out.writeCollection(chunks);
    }

    @Override
    void addMapFields(Map<String, Object> map) {
        map.put(resultsField, chunks.stream().map(EmbeddingChunk::asMap).collect(Collectors.toList()));
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        ChunkedTextEmbeddingResults that = (ChunkedTextEmbeddingResults) o;
        return Objects.equals(resultsField, that.resultsField) && Objects.equals(chunks, that.chunks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultsField, chunks);
    }
}
