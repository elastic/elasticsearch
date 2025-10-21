/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.inference.ChunkingStrategy;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

public class ChunkerBuilderTests extends ESTestCase {

    public void testNullChunkingStrategy() {
        assertThat(ChunkerBuilder.fromChunkingStrategy(null), instanceOf(WordBoundaryChunker.class));
    }

    public void testValidChunkingStrategy() {
        chunkingStrategyToExpectedChunkerClassMap().forEach((chunkingStrategy, chunkerClass) -> {
            assertThat(ChunkerBuilder.fromChunkingStrategy(chunkingStrategy), instanceOf(chunkerClass));
        });
    }

    private Map<ChunkingStrategy, Class<? extends Chunker>> chunkingStrategyToExpectedChunkerClassMap() {
        return Map.of(
            ChunkingStrategy.NONE,
            NoopChunker.class,
            ChunkingStrategy.WORD,
            WordBoundaryChunker.class,
            ChunkingStrategy.SENTENCE,
            SentenceBoundaryChunker.class
        );
    }
}
