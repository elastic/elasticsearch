/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public record ChunkedTextEmbeddingFloatResults(List<EmbeddingChunk> chunks) implements ChunkedInferenceServiceResults {

    public static final String NAME = "chunked_text_embedding_service_float_results";
    public static final String FIELD_NAME = "text_embedding_float_chunk";

    public ChunkedTextEmbeddingFloatResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(EmbeddingChunk::new));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // TODO add isTruncated flag
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
        return Map.of(FIELD_NAME, chunks.stream().map(EmbeddingChunk::asMap).collect(Collectors.toList()));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public List<EmbeddingChunk> getChunks() {
        return chunks;
    }

    public record EmbeddingChunk(String matchedText, List<Float> embedding) implements Writeable, ToXContentObject {

        public EmbeddingChunk(StreamInput in) throws IOException {
            this(in.readString(), in.readCollectionAsImmutableList(StreamInput::readFloat));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(matchedText);
            out.writeCollection(embedding, StreamOutput::writeFloat);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ChunkedNlpInferenceResults.TEXT, matchedText);

            builder.startArray(ChunkedNlpInferenceResults.INFERENCE);
            for (Float value : embedding) {
                builder.value(value);
            }
            builder.endArray();

            builder.endObject();
            return builder;
        }

        public Map<String, Object> asMap() {
            var map = new HashMap<String, Object>();
            map.put(ChunkedNlpInferenceResults.TEXT, matchedText);
            map.put(ChunkedNlpInferenceResults.INFERENCE, embedding);
            return map;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

}
