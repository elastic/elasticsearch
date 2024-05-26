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

public class ByteEmbedding extends Embedding<ByteEmbedding.ByteArrayWrapper> {

    /**
     * Wrapper so around a primitive byte array so that it can be
     * treated as a generic
     */
    public static class ByteArrayWrapper implements EmbeddingValues {

        final byte[] bytes;

        public ByteArrayWrapper(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray(EMBEDDING);
            for (var value : bytes) {
                builder.value(value);
            }
            builder.endArray();
            return builder;
        }

        @Override
        public int size() {
            return bytes.length;
        }

        @Override
        public XContentBuilder valuesToXContent(String fieldName, XContentBuilder builder, Params params) {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ByteArrayWrapper that = (ByteArrayWrapper) o;
            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    public ByteEmbedding(StreamInput in) throws IOException {
        this(in.readByteArray());
    }

    public ByteEmbedding(byte[] embedding) {
        super(new ByteArrayWrapper(embedding));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(embedding.bytes);
    }

    public byte[] bytes() {
        return embedding.bytes;
    }

    public float[] toFloatArray() {
        float[] floatArray = new float[embedding.bytes.length];
        for (int i = 0; i < embedding.bytes.length; i++) {
            floatArray[i] = ((Byte) embedding.bytes[i]).floatValue();
        }
        return floatArray;
    }
}
