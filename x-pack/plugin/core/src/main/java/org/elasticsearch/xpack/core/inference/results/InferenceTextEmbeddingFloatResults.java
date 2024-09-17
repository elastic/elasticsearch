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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
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
public record InferenceTextEmbeddingFloatResults(List<InferenceFloatEmbedding> embeddings)
    implements
        InferenceServiceResults,
        TextEmbedding {
    public static final String NAME = "text_embedding_service_results";
    public static final String TEXT_EMBEDDING = TaskType.TEXT_EMBEDDING.toString();

    public InferenceTextEmbeddingFloatResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(InferenceFloatEmbedding::new));
    }

    @SuppressWarnings("deprecation")
    InferenceTextEmbeddingFloatResults(LegacyTextEmbeddingResults legacyTextEmbeddingResults) {
        this(
            legacyTextEmbeddingResults.embeddings()
                .stream()
                .map(embedding -> new InferenceFloatEmbedding(embedding.values()))
                .collect(Collectors.toList())
        );
    }

    public static InferenceTextEmbeddingFloatResults of(List<? extends InferenceResults> results) {
        List<InferenceFloatEmbedding> embeddings = new ArrayList<>(results.size());
        for (InferenceResults result : results) {
            if (result instanceof MlTextEmbeddingResults embeddingResult) {
                embeddings.add(InferenceFloatEmbedding.of(embeddingResult));
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
        return new InferenceTextEmbeddingFloatResults(embeddings);
    }

    @Override
    public int getFirstEmbeddingSize() {
        return TextEmbeddingUtils.getFirstEmbeddingSize(new ArrayList<>(embeddings));
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
        map.put(TEXT_EMBEDDING, embeddings);

        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceTextEmbeddingFloatResults that = (InferenceTextEmbeddingFloatResults) o;
        return Objects.equals(embeddings, that.embeddings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(embeddings);
    }

    public record InferenceFloatEmbedding(float[] values) implements Writeable, ToXContentObject, EmbeddingInt {
        public static final String EMBEDDING = "embedding";

        public InferenceFloatEmbedding(StreamInput in) throws IOException {
            this(in.readFloatArray());
        }

        public static InferenceFloatEmbedding of(MlTextEmbeddingResults embeddingResult) {
            float[] embeddingAsArray = embeddingResult.getInferenceAsFloat();
            return new InferenceFloatEmbedding(embeddingAsArray);
        }

        public static InferenceFloatEmbedding of(List<Float> embeddingValuesList) {
            float[] embeddingValues = new float[embeddingValuesList.size()];
            for (int i = 0; i < embeddingValuesList.size(); i++) {
                embeddingValues[i] = embeddingValuesList.get(i);
            }
            return new InferenceFloatEmbedding(embeddingValues);
        }

        @Override
        public int getSize() {
            return values.length;
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
            InferenceFloatEmbedding embedding = (InferenceFloatEmbedding) o;
            return Arrays.equals(values, embedding.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
