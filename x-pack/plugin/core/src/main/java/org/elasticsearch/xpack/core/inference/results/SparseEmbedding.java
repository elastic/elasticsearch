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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SparseEmbedding extends Embedding<SparseEmbedding.WeightedTokens> {

    public static final String IS_TRUNCATED = "is_truncated";

    public static SparseEmbedding fromMlResults(
        List<org.elasticsearch.xpack.core.ml.search.WeightedToken> weightedTokens,
        boolean isTruncated
    ) {
        return new SparseEmbedding(
            new WeightedTokens(weightedTokens.stream().map(token -> new WeightedToken(token.token(), token.weight())).toList()),
            isTruncated
        );
    }

    private final boolean isTruncated;

    public SparseEmbedding(StreamInput in) throws IOException {
        this(new WeightedTokens(in.readCollectionAsImmutableList(SparseEmbedding.WeightedToken::new)), in.readBoolean());
    }

    public SparseEmbedding(WeightedTokens embedding, boolean isTruncated) {
        super(embedding);
        this.isTruncated = isTruncated;
    }

    public SparseEmbedding(List<WeightedToken> tokens, boolean isTruncated) {
        super(new WeightedTokens(tokens));
        this.isTruncated = isTruncated;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(embedding.tokens);
        out.writeBoolean(isTruncated);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(IS_TRUNCATED, isTruncated);
        embedding.toXContent(builder, params);
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

    public List<WeightedToken> tokens() {
        return embedding.tokens;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        SparseEmbedding that = (SparseEmbedding) o;
        return isTruncated == that.isTruncated;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isTruncated);
    }

    public static class WeightedTokens implements Embedding.EmbeddingValues {
        private final List<WeightedToken> tokens;

        public WeightedTokens(List<WeightedToken> tokens) {
            this.tokens = tokens;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return valuesToXContent(EMBEDDING, builder, params);
        }

        @Override
        public int size() {
            return tokens.size();
        }

        @Override
        public XContentBuilder valuesToXContent(String fieldName, XContentBuilder builder, Params params) throws IOException {
            builder.startObject(fieldName);
            for (var weightedToken : tokens) {
                weightedToken.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        public List<WeightedToken> tokens() {
            return tokens;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WeightedTokens that = (WeightedTokens) o;
            return Objects.equals(tokens, that.tokens);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tokens);
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
