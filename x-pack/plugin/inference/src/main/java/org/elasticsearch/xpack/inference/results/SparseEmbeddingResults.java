/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public record SparseEmbeddingResults(List<Embedding> embeddings, boolean isTruncated) implements InferenceServiceResults {

    public static final String NAME = "sparse_embedding_results";
    public static final String SPARSE_EMBEDDING = TaskType.SPARSE_EMBEDDING.toString();
    public static final String EMBEDDING = "embedding";
    public static final String IS_TRUNCATED = "is_truncated";

    public SparseEmbeddingResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(Embedding::new), in.readBoolean());
    }

    public static SparseEmbeddingResults create(List<? extends InferenceResults> results) {
        boolean isTruncated = false;
        List<Embedding> embeddings = new ArrayList<>(results.size());

        for (InferenceResults result : results) {
            if (result instanceof TextExpansionResults expansionResults) {
                isTruncated |= expansionResults.isTruncated();
                embeddings.add(Embedding.create(expansionResults.getWeightedTokens()));
            } else {
                throw new IllegalArgumentException("Received invalid legacy inference result");
            }
        }

        return new SparseEmbeddingResults(embeddings, isTruncated);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SPARSE_EMBEDDING);
        builder.field(IS_TRUNCATED, isTruncated);
        builder.startArray(EMBEDDING);

        for (Embedding embedding : embeddings) {
            embedding.toXContent(builder, params);
        }

        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(embeddings);
        out.writeBoolean(isTruncated);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();

        var embeddingList = embeddings.stream().map(Embedding::asMap).toList();
        Map<String, Object> sparseEmbeddingMap = new LinkedHashMap<>();
        sparseEmbeddingMap.put(EMBEDDING, embeddingList);
        sparseEmbeddingMap.put(IS_TRUNCATED, isTruncated);

        map.put(SPARSE_EMBEDDING, sparseEmbeddingMap);

        return map;
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        return embeddings.stream()
            .map(
                embedding -> new TextExpansionResults(
                    DEFAULT_RESULTS_FIELD,
                    embedding.tokens()
                        .stream()
                        .map(weightedToken -> new TextExpansionResults.WeightedToken(weightedToken.token, weightedToken.weight))
                        .toList(),
                    isTruncated
                )
            )
            .toList();
    }

    public record Embedding(List<WeightedToken> tokens) implements Writeable, ToXContentObject {

        public Embedding(StreamInput in) throws IOException {
            this(in.readCollectionAsList(WeightedToken::new));
        }

        public static Embedding create(List<TextExpansionResults.WeightedToken> weightedTokens) {
            return new Embedding(weightedTokens.stream().map(token -> new WeightedToken(token.token(), token.weight())).toList());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(tokens);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            for (var weightedToken : tokens) {
                weightedToken.toXContent(builder, params);
            }

            builder.endObject();
            return builder;
        }

        public Map<String, Object> asMap() {
            return new LinkedHashMap<>(tokens.stream().collect(Collectors.toMap(WeightedToken::token, WeightedToken::weight)));
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public record WeightedToken(String token, float weight) implements Writeable, ToXContentFragment {
        public WeightedToken(StreamInput in) throws IOException {
            this(in.readString(), in.readFloat());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(token);
            out.writeFloat(weight);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(token, weight);
            return builder;
        }

        public Map<String, Object> asMap() {
            return Map.of(token, weight);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
