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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Writes a dense embedding result in the follow json format. The "text_embedding" part of the array name may change depending on the
 * {@link TaskType} used to generate the embedding
 * <pre>
 * {
 *     "text_embedding_bits": [
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
 * </pre>
 */
// Note: inheriting from TextEmbeddingByteResults gives a bad implementation of the
// Embedding.merge method for bits. TODO: implement a proper merge method
public final class DenseEmbeddingBitResults implements DenseEmbeddingResults<DenseEmbeddingByteResults.Embedding> {
    public static final String NAME = "text_embedding_service_bit_results";
    public static final String BITS_SUFFIX = "_bits";
    private final List<DenseEmbeddingByteResults.Embedding> embeddings;
    private final String arrayName;

    public DenseEmbeddingBitResults(List<DenseEmbeddingByteResults.Embedding> embeddings) {
        this(embeddings, TEXT_EMBEDDING);
    }

    public DenseEmbeddingBitResults(List<DenseEmbeddingByteResults.Embedding> embeddings, String taskName) {
        this.embeddings = embeddings;
        this.arrayName = getArrayNameFromTaskName(taskName);
    }

    public DenseEmbeddingBitResults(StreamInput in) throws IOException {
        embeddings = in.readCollectionAsList(DenseEmbeddingByteResults.Embedding::new);
        if (in.getTransportVersion().supports(ML_MULTIMODAL_EMBEDDINGS)) {
            arrayName = in.readString();
        } else {
            arrayName = getArrayNameFromTaskName(TEXT_EMBEDDING);
        }
    }

    public static String getArrayNameFromTaskName(String taskName) {
        return taskName + BITS_SUFFIX;
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
        if (out.getTransportVersion().supports(ML_MULTIMODAL_EMBEDDINGS)) {
            out.writeString(arrayName);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return embeddings.stream().map(embedding -> new MlTextEmbeddingResults(arrayName, embedding.toDoubleArray(), false)).toList();
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
        DenseEmbeddingBitResults that = (DenseEmbeddingBitResults) o;
        return Objects.equals(embeddings, that.embeddings) && Objects.equals(arrayName, that.arrayName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(embeddings, arrayName);
    }

    @Override
    public List<DenseEmbeddingByteResults.Embedding> embeddings() {
        return embeddings;
    }

    @Override
    public String toString() {
        return "DenseEmbeddingBitResults[" + "embeddings=" + embeddings + ", " + "arrayName=" + arrayName + ']';
    }

}
