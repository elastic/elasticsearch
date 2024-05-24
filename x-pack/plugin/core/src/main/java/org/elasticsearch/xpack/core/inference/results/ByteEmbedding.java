/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class ByteEmbedding extends Embedding<ByteEmbedding.ByteArrayWrapper> {

    /**
     * Wrapper so around a primitive byte array so that it can be
     * treated as a generic
     */
    public static class ByteArrayWrapper implements ToXContentFragment {

        final byte[] embedding;

        public ByteArrayWrapper(byte[] bytes) {
            this.embedding = bytes;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray(EMBEDDING);
            for (var value : embedding) {
                builder.value(value);
            }
            builder.endArray();
            return builder;
        }

        @Override
        public int size() {
            return embedding.length;
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
        out.writeByteArray(embedding.embedding);
    }
}
