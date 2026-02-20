/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract class from which other dense embedding bit result classes inherit their behaviour
 */
// Note: inheriting from AbstractDenseEmbeddingByteResults gives a bad implementation of the
// Embedding.merge method for bits. TODO: implement a proper merge method
public abstract class EmbeddingBitResults implements DenseEmbeddingResults<EmbeddingByteResults.Embedding> {
    private final List<EmbeddingByteResults.Embedding> embeddings;
    private final String arrayName;

    public EmbeddingBitResults(List<EmbeddingByteResults.Embedding> embeddings, String arrayName) {
        this.embeddings = embeddings;
        this.arrayName = arrayName;
    }

    public EmbeddingBitResults(StreamInput in, String arrayName) throws IOException {
        this(in.readCollectionAsList(EmbeddingByteResults.Embedding::new), arrayName);
    }

    @Override
    public List<EmbeddingByteResults.Embedding> embeddings() {
        return embeddings;
    }

    @Override
    public int getFirstEmbeddingSize() {
        if (embeddings.isEmpty()) {
            throw new IllegalStateException("Embeddings list is empty");
        }
        // bit embeddings are encoded as bytes so convert this to bits
        return Byte.SIZE * embeddings.getFirst().values().length;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContentHelper.array(arrayName, embeddings.iterator());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(embeddings);
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return embeddings.stream().map(embedding -> new MlDenseEmbeddingResults(arrayName, embedding.toDoubleArray(), false)).toList();
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(arrayName, embeddings);

        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmbeddingBitResults that = (EmbeddingBitResults) o;
        return Objects.equals(embeddings, that.embeddings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(embeddings);
    }
}
