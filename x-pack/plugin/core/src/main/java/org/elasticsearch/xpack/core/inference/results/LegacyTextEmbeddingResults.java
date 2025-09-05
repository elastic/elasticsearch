/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Writes a text embedding result in the following json format
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
 *
 * Legacy text embedding results represents what was returned prior to the
 * {@link org.elasticsearch.TransportVersions#V_8_12_0} version.
 * @deprecated use {@link TextEmbeddingFloatResults} instead
 */
@Deprecated
public record LegacyTextEmbeddingResults(List<Embedding> embeddings) implements InferenceResults {
    public static final String NAME = "text_embedding_results";
    public static final String TEXT_EMBEDDING = TaskType.TEXT_EMBEDDING.toString();

    public LegacyTextEmbeddingResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(Embedding::new));
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
    public String getResultsField() {
        return TEXT_EMBEDDING;
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(getResultsField(), embeddings);

        return map;
    }

    @Override
    public Map<String, Object> asMap(String outputField) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(outputField, embeddings);

        return map;
    }

    @Override
    public Object predictedValue() {
        throw new UnsupportedOperationException("[" + NAME + "] does not support a single predicted value");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LegacyTextEmbeddingResults that = (LegacyTextEmbeddingResults) o;
        return Objects.equals(embeddings, that.embeddings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(embeddings);
    }

    public TextEmbeddingFloatResults transformToTextEmbeddingResults() {
        return new TextEmbeddingFloatResults(this);
    }

    public record Embedding(float[] values) implements Writeable, ToXContentObject {
        public static final String EMBEDDING = "embedding";

        public Embedding(StreamInput in) throws IOException {
            this(in.readFloatArray());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeFloatArray(values);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.startArray(EMBEDDING);
            for (float value : values) {
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
            Embedding embedding = (Embedding) o;
            return Arrays.equals(values, embedding.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
