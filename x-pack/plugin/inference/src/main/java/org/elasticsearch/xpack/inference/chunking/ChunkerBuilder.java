/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.inference.ChunkingStrategy;

public class ChunkerBuilder {
    public static Chunker fromChunkingStrategy(ChunkingStrategy chunkingStrategy) {
        if (chunkingStrategy == null) {
            return new WordBoundaryChunker();
        }

        return switch (chunkingStrategy) {
            case NONE -> NoopChunker.INSTANCE;
            case WORD -> new WordBoundaryChunker();
            case SENTENCE -> new SentenceBoundaryChunker();
        };
    }
}
