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
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Writes a text embedding result in the follow json format
 * {
 *     "text_embedding": [
 *         {
 *             "embedding": [
 *                 0.1
 *             ]
 *         },
 *         {
 *             "embedding": [
 *                 0.2
 *             ]
 *         }
 *     ]
 * }
 */
public record TextEmbeddingResults(List<Embedding> embeddings) implements InferenceServiceResults {
    public static final String NAME = "text_embedding_service_results";
    public static final String TEXT_EMBEDDING = TaskType.TEXT_EMBEDDING.toString();

    public TextEmbeddingResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(Embedding::new));
    }

    @SuppressWarnings("deprecation")
    TextEmbeddingResults(LegacyTextEmbeddingResults legacyTextEmbeddingResults) {
        this(
            legacyTextEmbeddingResults.embeddings()
                .stream()
                .map(embedding -> new Embedding(embedding.values()))
                .collect(Collectors.toList())
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TEXT_EMBEDDING);
        for (Embedding embedding : embeddings) {
            embedding.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(embeddings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return embeddings.stream()
            .map(embedding -> embedding.values.stream().mapToDouble(value -> value).toArray())
            .map(values -> new org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults(TEXT_EMBEDDING, values, false))
            .toList();
    }

    @Override
    @SuppressWarnings("deprecation")
    public List<? extends InferenceResults> transformToLegacyFormat() {
        var legacyEmbedding = new LegacyTextEmbeddingResults(
            embeddings.stream().map(embedding -> new LegacyTextEmbeddingResults.Embedding(embedding.values)).toList()
        );

        return List.of(legacyEmbedding);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(TEXT_EMBEDDING, embeddings.stream().map(Embedding::asMap).collect(Collectors.toList()));

        return map;
    }

    public record Embedding(List<Float> values) implements Writeable, ToXContentObject {
        public static final String EMBEDDING = "embedding";

        public Embedding(StreamInput in) throws IOException {
            this(in.readCollectionAsImmutableList(StreamInput::readFloat));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(values, StreamOutput::writeFloat);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.startArray(EMBEDDING);
            for (Float value : values) {
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

        public Map<String, Object> asMap() {
            return Map.of(EMBEDDING, values);
        }
    }
}
