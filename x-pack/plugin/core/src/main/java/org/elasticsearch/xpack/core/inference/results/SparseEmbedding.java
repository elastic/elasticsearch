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
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SparseEmbedding extends Embedding<SparseEmbedding.WeightedTokens> {

    public static final String IS_TRUNCATED = "is_truncated";

    public static SparseEmbedding create(List<TextExpansionResults.WeightedToken> weightedTokens, boolean isTruncated) {
        return new SparseEmbedding(
            weightedTokens.stream().map(token -> new WeightedToken(token.token(), token.weight())).toList(),
            isTruncated
        );
    }

    private final boolean isTruncated;

    public SparseEmbedding(StreamInput in) throws IOException {
        this(in.readCollectionAsImmutableList(SparseEmbedding.WeightedToken::new), in.readBoolean());
    }

    public SparseEmbedding(WeightedTokens embeddings, boolean isTruncated) {
        super(embedding);
        this.isTruncated = isTruncated;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(embedding);
        out.writeBoolean(isTruncated);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(IS_TRUNCATED, isTruncated);
        builder.startObject(EMBEDDING);

        for (var weightedToken : embedding) {
            weightedToken.toXContent(builder, params);
        }

        builder.endObject();
        builder.endObject();
        return builder;
    }

    public boolean isTruncated() {
        return isTruncated;
    }

    public Map<String, Object> asMap() {
        var embeddingMap = new LinkedHashMap<String, Object>(
            embedding.tokens.stream().collect(Collectors.toMap(WeightedToken::token, WeightedToken::weight))
        );

        return new LinkedHashMap<>(Map.of(IS_TRUNCATED, isTruncated, EMBEDDING, embeddingMap));
    }

    public static class WeightedTokens() implements Embedding.EmbeddingValues {

        private final List<WeightedToken> tokens;

        public WeightedTokens(List<WeightedToken> tokens) {
            this.tokens = tokens;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(EMBEDDING);
            for (var weightedToken : tokens) {
                weightedToken.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int size() {
            return tokens.size();
        }

        @Override
        public XContentBuilder valuesToXContent(String fieldName, XContentBuilder builder, Params params) {
            return null;
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
