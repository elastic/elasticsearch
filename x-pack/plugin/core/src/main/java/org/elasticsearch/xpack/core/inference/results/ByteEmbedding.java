/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class ByteEmbedding extends Embedding<Byte> {

    public ByteEmbedding(StreamInput in) throws IOException {
        this(in.readCollectionAsImmutableList(StreamInput::readByte));
    }

    public ByteEmbedding(List<Byte> embedding) {
        super(embedding);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(embedding, StreamOutput::writeByte);
    }

    public List<Float> toFloats() {
        return embedding.stream().map(Byte::floatValue).toList();
    }
}
