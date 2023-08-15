/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.results;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class SparseEmbeddingResult implements InferenceResult {

    public static final String NAME = "sparse_embedding_result";

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
    }

    private final List<WeightedToken> weightedTokens;

    public SparseEmbeddingResult(List<WeightedToken> weightedTokens) {
        this.weightedTokens = weightedTokens;
    }

    public SparseEmbeddingResult(StreamInput in) throws IOException {
        this.weightedTokens = in.readList(WeightedToken::new);
    }

    public List<WeightedToken> getWeightedTokens() {
        return weightedTokens;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("sparse_embedding");
        for (var weightedToken : weightedTokens) {
            weightedToken.toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.current(); // TODO
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(weightedTokens);
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
}
