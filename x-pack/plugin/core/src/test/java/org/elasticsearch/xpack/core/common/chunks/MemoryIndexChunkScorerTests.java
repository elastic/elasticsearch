/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.chunks;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;

public class MemoryIndexChunkScorerTests extends ESTestCase {

    public void testScoreChunks() throws IOException {
        MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();

        List<String> chunks = Arrays.asList(
            "Cats like to sleep all day and play with mice",
            "Dogs are loyal companions and great pets",
            "The weather today is very sunny and warm",
            "Dogs love to play with toys and go for walks",
            "Elasticsearch is a great search engine"
        );

        String inferenceText = "dogs play walk";
        int maxResults = 3;

        List<MemoryIndexChunkScorer.ScoredChunk> scoredChunks = scorer.scoreChunks(chunks, inferenceText, maxResults);

        assertEquals(maxResults, scoredChunks.size());

        // The chunks about dogs should score highest, followed by the chunk about cats
        MemoryIndexChunkScorer.ScoredChunk chunk = scoredChunks.getFirst();
        assertTrue(chunk.content().equalsIgnoreCase("Dogs love to play with toys and go for walks"));
        assertThat(chunk.score(), greaterThan(0f));

        chunk = scoredChunks.get(1);
        assertTrue(chunk.content().equalsIgnoreCase("Dogs are loyal companions and great pets"));
        assertThat(chunk.score(), greaterThan(0f));

        chunk = scoredChunks.get(2);
        assertTrue(chunk.content().equalsIgnoreCase("Cats like to sleep all day and play with mice"));
        assertThat(chunk.score(), greaterThan(0f));

        // Scores should be in descending order
        for (int i = 1; i < scoredChunks.size(); i++) {
            assertTrue(scoredChunks.get(i - 1).score() >= scoredChunks.get(i).score());
        }
    }
}
