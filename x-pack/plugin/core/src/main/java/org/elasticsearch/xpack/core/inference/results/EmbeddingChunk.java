/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class EmbeddingChunk<T extends Embedding.EmbeddingValues> implements Writeable, ToXContentObject {

    private final String matchedText;
    private final Embedding<T> embedding;

    public EmbeddingChunk(String matchedText, Embedding<T> embedding) {
        this.matchedText = matchedText;
        this.embedding = embedding;
    }

    public String matchedText() {
        return matchedText;
    }

    public Embedding<T> embedding() {
        return embedding;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(matchedText);
        embedding.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ChunkedNlpInferenceResults.TEXT, matchedText);
        embedding.embedding.valuesToXContent(ChunkedNlpInferenceResults.INFERENCE, builder, params);
        builder.endObject();
        return builder;
    }

    public Map<String, Object> asMap() {
        var map = new HashMap<String, Object>();
        map.put(ChunkedNlpInferenceResults.TEXT, matchedText);
        map.put(ChunkedNlpInferenceResults.INFERENCE, embedding.getEmbedding());
        return map;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmbeddingChunk<?> that = (EmbeddingChunk<?>) o;
        return Objects.equals(matchedText, that.matchedText) && Objects.equals(embedding, that.embedding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(matchedText, embedding);
    }
}
