/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.chunking;

import org.elasticsearch.inference.ChunkingStrategy;

public class ChunkerBuilder {
    public static Chunker fromChunkingStrategy(ChunkingStrategy chunkingStrategy) {
        return fromChunkingStrategy(chunkingStrategy, RecursiveChunkingSettings.DEFAULT_REGEX_READ_LIMIT_FACTOR);
    }

    public static Chunker fromChunkingStrategy(ChunkingStrategy chunkingStrategy, int regexReadLimitFactor) {
        if (chunkingStrategy == null) {
            return new WordBoundaryChunker();
        }

        return switch (chunkingStrategy) {
            case NONE -> NoopChunker.INSTANCE;
            case WORD -> new WordBoundaryChunker();
            case SENTENCE -> new SentenceBoundaryChunker();
            case RECURSIVE -> new RecursiveChunker(regexReadLimitFactor);
        };
    }
}
