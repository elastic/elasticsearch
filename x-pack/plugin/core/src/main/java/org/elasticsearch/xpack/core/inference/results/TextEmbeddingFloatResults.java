/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
public record TextEmbeddingFloatResults(List<Embedding> embeddings) implements TextEmbeddingResults<TextEmbeddingFloatResults.Embedding> {
    public static final String NAME = "text_embedding_service_results";
    public static final String TEXT_EMBEDDING = TaskType.TEXT_EMBEDDING.toString();

    public TextEmbeddingFloatResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(TextEmbeddingFloatResults.Embedding::new));
    }

    @SuppressWarnings("deprecation")
    TextEmbeddingFloatResults(LegacyTextEmbeddingResults legacyTextEmbeddingResults) {
        this(
            legacyTextEmbeddingResults.embeddings()
                .stream()
                .map(embedding -> new Embedding(embedding.values()))
                .collect(Collectors.toList())
        );
    }

    public static TextEmbeddingFloatResults of(List<? extends InferenceResults> results) {
        List<Embedding> embeddings = new ArrayList<>(results.size());
        for (InferenceResults result : results) {
            if (result instanceof MlTextEmbeddingResults embeddingResult) {
                embeddings.add(TextEmbeddingFloatResults.Embedding.of(embeddingResult));
            } else if (result instanceof org.elasticsearch.xpack.core.ml.inference.results.ErrorInferenceResults errorResult) {
                if (errorResult.getException() instanceof ElasticsearchStatusException statusException) {
                    throw statusException;
                } else {
                    throw new ElasticsearchStatusException(
                        "Received error inference result.",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        errorResult.getException()
                    );
                }
            } else {
                throw new IllegalArgumentException(
                    "Received invalid inference result, of type " + result.getClass().getName() + " but expected TextEmbeddingResults."
                );
            }
        }
        return new TextEmbeddingFloatResults(embeddings);
    }

    @Override
    public int getFirstEmbeddingSize() {
        if (embeddings.isEmpty()) {
            throw new IllegalStateException("Embeddings list is empty");
        }
        return embeddings.getFirst().values().length;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContentHelper.array(TEXT_EMBEDDING, embeddings.iterator());
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
        return embeddings.stream().map(embedding -> new MlTextEmbeddingResults(TEXT_EMBEDDING, embedding.asDoubleArray(), false)).toList();
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(TEXT_EMBEDDING, embeddings);

        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextEmbeddingFloatResults that = (TextEmbeddingFloatResults) o;
        return Objects.equals(embeddings, that.embeddings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(embeddings);
    }

    // Note: the field "numberOfMergedEmbeddings" is not serialized, so merging
    // embeddings should happen inbetween serializations.
    public record Embedding(float[] values, int numberOfMergedEmbeddings)
        implements
            Writeable,
            ToXContentObject,
            EmbeddingResults.Embedding<Embedding> {
        public static final String EMBEDDING = "embedding";

        public Embedding(float[] values) {
            this(values, 1);
        }

        public Embedding(StreamInput in) throws IOException {
            this(in.readFloatArray());
        }

        public static Embedding of(MlTextEmbeddingResults embeddingResult) {
            float[] embeddingAsArray = embeddingResult.getInferenceAsFloat();
            return new Embedding(embeddingAsArray);
        }

        public static Embedding of(List<Float> embeddingValuesList) {
            float[] embeddingValues = new float[embeddingValuesList.size()];
            for (int i = 0; i < embeddingValuesList.size(); i++) {
                embeddingValues[i] = embeddingValuesList.get(i);
            }
            return new Embedding(embeddingValues);
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

        private double[] asDoubleArray() {
            double[] doubles = new double[values.length];
            for (int i = 0; i < values.length; i++) {
                doubles[i] = values[i];
            }
            return doubles;
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

        @Override
        public Embedding merge(Embedding embedding) {
            float[] mergedValues = new float[values.length];
            for (int i = 0; i < values.length; i++) {
                mergedValues[i] = (numberOfMergedEmbeddings * values[i] + embedding.numberOfMergedEmbeddings * embedding.values[i])
                    / (numberOfMergedEmbeddings + embedding.numberOfMergedEmbeddings);
            }
            return new Embedding(mergedValues, numberOfMergedEmbeddings + embedding.numberOfMergedEmbeddings);
        }

        @Override
        public BytesReference toBytesRef(XContent xContent) throws IOException {
            XContentBuilder b = XContentBuilder.builder(xContent);
            b.startArray();
            for (float value : values) {
                b.value(value);
            }
            b.endArray();
            return BytesReference.bytes(b);
        }
    }
}
