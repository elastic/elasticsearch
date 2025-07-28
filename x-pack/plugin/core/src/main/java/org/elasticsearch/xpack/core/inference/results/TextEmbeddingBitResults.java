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
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Writes a text embedding result in the follow json format
 * {
 *     "text_embedding_bytes": [
 *         {
 *             "embedding": [
 *                 23
 *             ]
 *         },
 *         {
 *             "embedding": [
 *                 -23
 *             ]
 *         }
 *     ]
 * }
 */
// Note: inheriting from TextEmbeddingByteResults gives a bad implementation of the
// Embedding.merge method for bits. TODO: implement a proper merge method
public record TextEmbeddingBitResults(List<TextEmbeddingByteResults.Embedding> embeddings)
    implements
        TextEmbeddingResults<TextEmbeddingByteResults.Embedding> {
    public static final String NAME = "text_embedding_service_bit_results";
    public static final String TEXT_EMBEDDING_BITS = "text_embedding_bits";

    public TextEmbeddingBitResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(TextEmbeddingByteResults.Embedding::new));
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
        return ChunkedToXContentHelper.array(TEXT_EMBEDDING_BITS, embeddings.iterator());
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
        return embeddings.stream()
            .map(embedding -> new MlTextEmbeddingResults(TEXT_EMBEDDING_BITS, embedding.toDoubleArray(), false))
            .toList();
    }

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(TEXT_EMBEDDING_BITS, embeddings);

        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextEmbeddingBitResults that = (TextEmbeddingBitResults) o;
        return Objects.equals(embeddings, that.embeddings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(embeddings);
    }
}
