/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class ChunkerBuilderTests extends ESTestCase {

    public void testNullChunkingStrategy() {
        assert (ChunkerBuilder.fromChunkingStrategy(null) instanceof WordBoundaryChunker);
    }

    public void testValidChunkingStrategy() {
        chunkingStrategyToExpectedChunkerClassMap().forEach((chunkingStrategy, chunkerClass) -> {
            assert (ChunkerBuilder.fromChunkingStrategy(chunkingStrategy).getClass().equals(chunkerClass));
        });
    }

    private Map<ChunkingStrategy, Class<? extends Chunker>> chunkingStrategyToExpectedChunkerClassMap() {
        return Map.of(ChunkingStrategy.WORD, WordBoundaryChunker.class, ChunkingStrategy.SENTENCE, SentenceBoundaryChunker.class);
    }
}
