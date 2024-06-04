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
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Writes a text embedding result in the follow json format
 * {
 *     "text_embedding": [
 *         {
 *             "embedding": [
 *                 23
 *             ]
 *         },
 *         {
 *             "embedding": [
 *                 -23
 *             ]
 *         }
 *     ]
 * }
 */
public record TextEmbeddingByteResults(List<Embedding> embeddings) implements InferenceServiceResults, TextEmbedding {
    public static final String NAME = "text_embedding_service_byte_results";
    public static final String TEXT_EMBEDDING_BYTES = "text_embedding_bytes";

    public TextEmbeddingByteResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(Embedding::new));
    }

    @Override
    public int getFirstEmbeddingSize() {
        return TextEmbeddingUtils.getFirstEmbeddingSize(new ArrayList<>(embeddings));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TEXT_EMBEDDING_BYTES);
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
            .map(
                embedding -> new org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults(
                    TEXT_EMBEDDING_BYTES,
                    embedding.toDoubleArray(),
                    false
                )
            )
            .toList();
    }

    @Override
    @SuppressWarnings("deprecation")
    public List<? extends InferenceResults> transformToLegacyFormat() {
        var legacyEmbedding = new LegacyTextEmbeddingResults(
            embeddings.stream().map(embedding -> new LegacyTextEmbeddingResults.Embedding(embedding.toFloatArray())).toList()
        );

        return List.of(legacyEmbedding);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(TEXT_EMBEDDING_BYTES, embeddings);

        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextEmbeddingByteResults that = (TextEmbeddingByteResults) o;
        return Objects.equals(embeddings, that.embeddings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(embeddings);
    }

    public record Embedding(byte[] values) implements Writeable, ToXContentObject, EmbeddingInt {
        public static final String EMBEDDING = "embedding";

        public Embedding(StreamInput in) throws IOException {
            this(in.readByteArray());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByteArray(values);
        }

        public static Embedding of(List<Byte> embeddingValuesList) {
            byte[] embeddingValues = new byte[embeddingValuesList.size()];
            for (int i = 0; i < embeddingValuesList.size(); i++) {
                embeddingValues[i] = embeddingValuesList.get(i);
            }
            return new Embedding(embeddingValues);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.startArray(EMBEDDING);
            for (byte value : values) {
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

        private float[] toFloatArray() {
            float[] floatArray = new float[values.length];
            for (int i = 0; i < values.length; i++) {
                floatArray[i] = ((Byte) values[i]).floatValue();
            }
            return floatArray;
        }

        private double[] toDoubleArray() {
            double[] doubleArray = new double[values.length];
            for (int i = 0; i < values.length; i++) {
                doubleArray[i] = ((Byte) values[i]).floatValue();
            }
            return doubleArray;
        }

        @Override
        public int getSize() {
            return values().length;
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
