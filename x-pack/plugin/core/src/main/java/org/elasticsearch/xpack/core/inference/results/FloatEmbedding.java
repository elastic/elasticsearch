/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FloatEmbedding extends Embedding<FloatEmbedding.FloatArrayWrapper> {

    public static FloatEmbedding of(List<Float> embedding) {
        float[] embeddingFloats = new float[embedding.size()];
        for (int i = 0; i < embedding.size(); i++) {
            embeddingFloats[i] = embedding.get(i);
        }
        return new FloatEmbedding(embeddingFloats);
    }

    public static class FloatArrayWrapper implements EmbeddingValues {

        private final float[] floats;

        public FloatArrayWrapper(float[] floats) {
            this.floats = floats;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return valuesToXContent(EMBEDDING, builder, params);
        }

        public float[] getFloats() {
            return floats;
        }

        @Override
        public int size() {
            return floats.length;
        }

        @Override
        public XContentBuilder valuesToXContent(String fieldName, XContentBuilder builder, Params params) throws IOException {
            builder.startArray(fieldName);
            for (var value : floats) {
                builder.value(value);
            }
            builder.endArray();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FloatArrayWrapper that = (FloatArrayWrapper) o;
            return Arrays.equals(floats, that.floats);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(floats);
        }
    }

    public FloatEmbedding(StreamInput in) throws IOException {
        this(in.readFloatArray());
    }

    public FloatEmbedding(float[] embedding) {
        super(new FloatArrayWrapper(embedding));
    }

    public float[] asFloatArray() {
        return embedding.floats;
    }

    public double[] asDoubleArray() {
        var result = new double[embedding.floats.length];
        for (int i = 0; i < embedding.floats.length; i++) {
            result[i] = embedding.floats[i];
        }
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloatArray(embedding.floats);
    }

    public static FloatEmbedding of(org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults embeddingResult) {
        return new FloatEmbedding(embeddingResult.getInferenceAsFloat());
    }
}
