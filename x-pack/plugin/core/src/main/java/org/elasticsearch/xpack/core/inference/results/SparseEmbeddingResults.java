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

public record SparseEmbeddingResults(List<Embedding> embeddings) implements InferenceServiceResults {

    public static final String NAME = "sparse_embedding_results";
    public static final String SPARSE_EMBEDDING = TaskType.SPARSE_EMBEDDING.toString();

    public SparseEmbeddingResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(Embedding::new));
    }

    public static SparseEmbeddingResults of(List<? extends InferenceResults> results) {
        List<Embedding> embeddings = new ArrayList<>(results.size());

        for (InferenceResults result : results) {
            if (result instanceof TextExpansionResults expansionResults) {
                embeddings.add(Embedding.create(expansionResults.getWeightedTokens(), expansionResults.isTruncated()));
            } else {
                throw new IllegalArgumentException("Received invalid legacy inference result");
            }
        }

        return new SparseEmbeddingResults(embeddings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(SPARSE_EMBEDDING);

        for (Embedding embedding : embeddings) {
            embedding.toXContent(builder, params);
        }

        builder.endArray();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(embeddings);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        var embeddingList = embeddings.stream().map(Embedding::asMap).toList();

        map.put(SPARSE_EMBEDDING, embeddingList);
        return map;
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return transformToLegacyFormat();
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
                    embedding.isTruncated
                )
            )
            .toList();
    }

    public record Embedding(List<WeightedToken> tokens, boolean isTruncated) implements Writeable, ToXContentObject {

        public static final String EMBEDDING = "embedding";
        public static final String IS_TRUNCATED = "is_truncated";

        public Embedding(StreamInput in) throws IOException {
            this(in.readCollectionAsList(WeightedToken::new), in.readBoolean());
        }

        public static Embedding create(List<TextExpansionResults.WeightedToken> weightedTokens, boolean isTruncated) {
            return new Embedding(
                weightedTokens.stream().map(token -> new WeightedToken(token.token(), token.weight())).toList(),
                isTruncated
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(tokens);
            out.writeBoolean(isTruncated);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(IS_TRUNCATED, isTruncated);
            builder.startObject(EMBEDDING);

            for (var weightedToken : tokens) {
                weightedToken.toXContent(builder, params);
            }

            builder.endObject();
            builder.endObject();
            return builder;
        }

        public Map<String, Object> asMap() {
            var embeddingMap = new LinkedHashMap<String, Object>(
                tokens.stream().collect(Collectors.toMap(WeightedToken::token, WeightedToken::weight))
            );

            return new LinkedHashMap<>(Map.of(IS_TRUNCATED, isTruncated, EMBEDDING, embeddingMap));
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
