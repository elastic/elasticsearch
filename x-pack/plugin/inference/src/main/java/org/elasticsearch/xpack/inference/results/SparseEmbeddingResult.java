/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class SparseEmbeddingResult implements InferenceResult {

    public static final String NAME = "sparse_embedding_result";

    private final List<TextExpansionResults.WeightedToken> weightedTokens;

    public SparseEmbeddingResult(List<TextExpansionResults.WeightedToken> weightedTokens) {
        this.weightedTokens = weightedTokens;
    }

    public SparseEmbeddingResult(StreamInput in) throws IOException {
        this.weightedTokens = in.readCollectionAsImmutableList(TextExpansionResults.WeightedToken::new);
    }

    public List<TextExpansionResults.WeightedToken> getWeightedTokens() {
        return weightedTokens;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("sparse_embedding");
        for (var weightedToken : weightedTokens) {
            weightedToken.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_500_074;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(weightedTokens);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SparseEmbeddingResult that = (SparseEmbeddingResult) o;
        return Objects.equals(weightedTokens, that.weightedTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(weightedTokens);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
