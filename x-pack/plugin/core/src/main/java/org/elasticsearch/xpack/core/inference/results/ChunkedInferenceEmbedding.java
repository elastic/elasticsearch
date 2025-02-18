/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.xcontent.XContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public record ChunkedInferenceEmbedding(List<? extends EmbeddingResults.EmbeddingChunk> chunks) implements ChunkedInference {

    @Override
    public Iterator<Chunk> chunksAsMatchedTextAndByteReference(XContent xcontent) throws IOException {
        var asChunk = new ArrayList<Chunk>();
        for (var chunk : chunks()) {
            asChunk.add(chunk.toChunk(xcontent));
        }
        return asChunk.iterator();
    }
}
