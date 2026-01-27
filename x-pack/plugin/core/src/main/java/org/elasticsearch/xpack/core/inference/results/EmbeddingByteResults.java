/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract class from which other dense embedding byte result classes inherit their behaviour
 */
public abstract class EmbeddingByteResults implements DenseEmbeddingResults<EmbeddingByteResults.Embedding> {
    private final List<Embedding> embeddings;
    private final String arrayName;

    public EmbeddingByteResults(List<Embedding> embeddings, String arrayName) {
        this.arrayName = arrayName;
        this.embeddings = embeddings;
    }

    public EmbeddingByteResults(StreamInput in, String arrayName) throws IOException {
        this(in.readCollectionAsList(Embedding::new), arrayName);
    }

    @Override
    public int getFirstEmbeddingSize() {
        if (embeddings.isEmpty()) {
            throw new IllegalStateException("Embeddings list is empty");
        }
        return embeddings.getFirst().values().length;
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
        EmbeddingByteResults that = (EmbeddingByteResults) o;
        return Objects.equals(embeddings, that.embeddings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(embeddings);
    }

    @Override
    public List<Embedding> embeddings() {
        return embeddings;
    }

    // Note: the field "numberOfMergedEmbeddings" is not serialized, so merging
    // embeddings should happen inbetween serializations.
    public record Embedding(byte[] values, int[] sumMergedValues, int numberOfMergedEmbeddings)
        implements
            Writeable,
            ToXContentObject,
            EmbeddingResults.Embedding<Embedding> {

        public Embedding(byte[] values) {
            this(values, null, 1);
        }

        public Embedding(StreamInput in) throws IOException {
            this(in.readByteArray());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByteArray(values);
        }

        public static Embedding of(List<Byte> embeddingValuesList) {
            byte[] embeddingValues = new byte[embeddingValuesList.size()];
            for (int i = 0; i < embeddingValuesList.size(); i++) {
                embeddingValues[i] = embeddingValuesList.get(i);
            }
            return new Embedding(embeddingValues);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.startArray(EMBEDDING);
            for (byte value : values) {
                builder.value(value);
            }
            builder.endArray();

            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        float[] toFloatArray() {
            float[] floatArray = new float[values.length];
            for (int i = 0; i < values.length; i++) {
                floatArray[i] = ((Byte) values[i]).floatValue();
            }
            return floatArray;
        }

        double[] toDoubleArray() {
            double[] doubleArray = new double[values.length];
            for (int i = 0; i < values.length; i++) {
                doubleArray[i] = ((Byte) values[i]).doubleValue();
            }
            return doubleArray;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Embedding embedding = (Embedding) o;
            return Arrays.equals(values, embedding.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }

        @Override
        public Embedding merge(Embedding embedding) {
            byte[] newValues = new byte[values.length];
            int[] newSumMergedValues = new int[values.length];
            int newNumberOfMergedEmbeddings = numberOfMergedEmbeddings + embedding.numberOfMergedEmbeddings;
            for (int i = 0; i < values.length; i++) {
                newSumMergedValues[i] = (numberOfMergedEmbeddings == 1 ? values[i] : sumMergedValues[i])
                    + (embedding.numberOfMergedEmbeddings == 1 ? embedding.values[i] : embedding.sumMergedValues[i]);
                // Add (newNumberOfMergedEmbeddings / 2) in the numerator to round towards the
                // closest byte instead of truncating.
                newValues[i] = (byte) ((newSumMergedValues[i] + newNumberOfMergedEmbeddings / 2) / newNumberOfMergedEmbeddings);
            }
            return new Embedding(newValues, newSumMergedValues, newNumberOfMergedEmbeddings);
        }

        @Override
        public BytesReference toBytesRef(XContent xContent) throws IOException {
            XContentBuilder builder = XContentBuilder.builder(xContent);
            builder.startArray();
            for (byte value : values) {
                builder.value(value);
            }
            builder.endArray();
            return BytesReference.bytes(builder);
        }
    }
}
