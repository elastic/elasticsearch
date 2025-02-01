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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public record InferenceByteEmbedding(byte[] values) implements Writeable, ToXContentObject, EmbeddingInt {
    public static final String EMBEDDING = "embedding";

    public InferenceByteEmbedding(StreamInput in) throws IOException {
        this(in.readByteArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(values);
    }

    public static InferenceByteEmbedding of(List<Byte> embeddingValuesList) {
        byte[] embeddingValues = new byte[embeddingValuesList.size()];
        for (int i = 0; i < embeddingValuesList.size(); i++) {
            embeddingValues[i] = embeddingValuesList.get(i);
        }
        return new InferenceByteEmbedding(embeddingValues);
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
    public int getSize() {
        return values().length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceByteEmbedding embedding = (InferenceByteEmbedding) o;
        return Arrays.equals(values, embedding.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }
}
