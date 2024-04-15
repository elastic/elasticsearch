/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class Embedding<T> implements Writeable, ToXContentObject {
    public static final String EMBEDDING = "embedding";

    protected final List<T> embedding;

    protected Embedding(List<T> embedding) {
        this.embedding = embedding;
    }

    public List<T> getEmbedding() {
        return embedding;
    }

    public Map<String, Object> asMap() {
        return Map.of(EMBEDDING, embedding);
    }

    public int getSize() {
        return embedding.size();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startArray(EMBEDDING);
        for (var value : embedding) {
            builder.value(value);
        }
        builder.endArray();

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Embedding<?> embedding1 = (Embedding<?>) o;
        return Objects.equals(embedding, embedding1.embedding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(embedding);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
