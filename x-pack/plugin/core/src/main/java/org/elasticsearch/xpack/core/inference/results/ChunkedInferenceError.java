/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.xcontent.XContent;

import java.util.Iterator;
import java.util.stream.Stream;

public record ChunkedInferenceError(Exception exception) implements ChunkedInference {

    @Override
    public Iterator<Chunk> chunksAsMatchedTextAndByteReference(XContent xcontent) {
        return Stream.of(exception).map(e -> new Chunk(e.getMessage(), new TextOffset(0, 0), BytesArray.EMPTY)).iterator();
    }
}
