/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.chunks;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class MemoryIndexChunkScorerTests extends ESTestCase {

    private static final List<String> CHUNKS = Arrays.asList(
        "Cats like to sleep all day and play with mice",
        "Dogs are loyal companions and great pets",
        "The weather today is very sunny and warm",
        "Dogs love to play with toys and go for walks",
        "Elasticsearch is a great search engine"
    );

    public void testScoreChunks() throws IOException {
        MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();

        String inferenceText = "dogs play walk";
        int maxResults = 3;

        List<ScoredChunk> scoredChunks = scorer.scoreChunks(CHUNKS, inferenceText, maxResults, true);

        assertEquals(maxResults, scoredChunks.size());

        // The chunks about dogs should score highest, followed by the chunk about cats
        ScoredChunk chunk = scoredChunks.getFirst();
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

    public void testEmptyChunks() throws IOException {

        int maxResults = 3;

        MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();

        // Zero results
        List<ScoredChunk> scoredChunks = scorer.scoreChunks(CHUNKS, "puggles", maxResults, true);
        assertEquals(maxResults, scoredChunks.size());

        // There were no results so we return the first N chunks in order
        ScoredChunk chunk = scoredChunks.getFirst();
        assertTrue(chunk.content().equalsIgnoreCase("Cats like to sleep all day and play with mice"));
        assertThat(chunk.score(), equalTo(0f));

        chunk = scoredChunks.get(1);
        assertTrue(chunk.content().equalsIgnoreCase("Dogs are loyal companions and great pets"));
        assertThat(chunk.score(), equalTo(0f));

        chunk = scoredChunks.get(2);
        assertTrue(chunk.content().equalsIgnoreCase("The weather today is very sunny and warm"));
        assertThat(chunk.score(), equalTo(0f));

        // Zero results with no backfill
        scoredChunks = scorer.scoreChunks(CHUNKS, "puggles", maxResults, false);
        assertEquals(0, scoredChunks.size());

        // Null and Empty chunk input
        scoredChunks = scorer.scoreChunks(List.of(), "puggles", maxResults, true);
        assertTrue(scoredChunks.isEmpty());

        scoredChunks = scorer.scoreChunks(CHUNKS, "", maxResults, true);
        assertTrue(scoredChunks.isEmpty());

        scoredChunks = scorer.scoreChunks(null, "puggles", maxResults, true);
        assertTrue(scoredChunks.isEmpty());

        scoredChunks = scorer.scoreChunks(CHUNKS, null, maxResults, true);
        assertTrue(scoredChunks.isEmpty());
    }

    public void testSessionScoreMatchesOneShot() throws IOException {
        MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();
        String queryText = "dogs play walk";
        int maxResults = 3;

        List<ScoredChunk> oneShotResult = scorer.scoreChunks(CHUNKS, queryText, maxResults, true);

        try (var session = scorer.openSession(CHUNKS)) {
            List<ScoredChunk> sessionResult = session.score(queryText, maxResults, true);
            assertThat(sessionResult.size(), equalTo(oneShotResult.size()));
            for (int i = 0; i < oneShotResult.size(); i++) {
                assertThat(sessionResult.get(i).content(), equalTo(oneShotResult.get(i).content()));
                assertThat(sessionResult.get(i).score(), equalTo(oneShotResult.get(i).score()));
            }
        }
    }

    public void testSessionExposesLuceneComponents() throws IOException {
        MemoryIndexChunkScorer scorer = new MemoryIndexChunkScorer();

        try (var session = scorer.openSession(CHUNKS)) {
            assertNotNull(session.searcher());
            assertNotNull(session.reader());
            assertNotNull(session.analyzer());
            assertThat(session.reader().numDocs(), equalTo(CHUNKS.size()));
        }
    }

    public void testDefaultConstructorUsesStandardAnalyzer() throws IOException {
        try (var session = new MemoryIndexChunkScorer().openSession(CHUNKS)) {
            assertThat(session.analyzer(), instanceOf(StandardAnalyzer.class));
        }
    }

    public void testConstructorAcceptsCustomAnalyzer() throws IOException {
        var whitespace = new WhitespaceAnalyzer();
        try (var session = new MemoryIndexChunkScorer(whitespace).openSession(CHUNKS)) {
            assertSame(whitespace, session.analyzer());
        }
    }

    public void testConstructorRejectsNullAnalyzer() {
        expectThrows(NullPointerException.class, () -> new MemoryIndexChunkScorer(null));
    }

}
