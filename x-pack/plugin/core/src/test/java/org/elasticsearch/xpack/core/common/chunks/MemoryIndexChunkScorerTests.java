/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.chunks;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.Query;
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

        scoredChunks = scorer.scoreChunks(CHUNKS, " ", maxResults, true);
        assertTrue(scoredChunks.isEmpty());

        scoredChunks = scorer.scoreChunks(CHUNKS, (String) null, maxResults, true);
        assertTrue(scoredChunks.isEmpty());
    }

    public void testDefaultConstructorUsesStandardAnalyzer() {
        assertThat(new MemoryIndexChunkScorer().analyzer(), instanceOf(StandardAnalyzer.class));
    }

    public void testNullAnalyzerThrowsAssertionError() {
        expectThrows(AssertionError.class, () -> new MemoryIndexChunkScorer(null));
    }

    public void testCustomAnalyzer() {
        var whitespace = new WhitespaceAnalyzer();
        var scorer = new MemoryIndexChunkScorer(whitespace);
        assertSame(whitespace, scorer.analyzer());

        List<ScoredChunk> results = scorer.scoreChunks(CHUNKS, "dogs play walk", 3, false);
        assertFalse(results.isEmpty());
    }

    public void testBuildQuery() {
        var scorer = new MemoryIndexChunkScorer();
        assertNotNull(scorer.buildQuery("dogs play"));
    }

    public void testScoreChunksWithQuery() {
        var scorer = new MemoryIndexChunkScorer();
        Query query = scorer.buildQuery("dogs play walk");

        List<ScoredChunk> withQuery = scorer.scoreChunks(CHUNKS, query, 3, false);
        List<ScoredChunk> withText = scorer.scoreChunks(CHUNKS, "dogs play walk", 3, false);

        assertThat(withQuery.size(), equalTo(withText.size()));
        for (int i = 0; i < withQuery.size(); i++) {
            assertThat(withQuery.get(i).content(), equalTo(withText.get(i).content()));
            assertThat(withQuery.get(i).score(), equalTo(withText.get(i).score()));
        }
    }

    public void testScoreChunksWithNullQuery() {
        var scorer = new MemoryIndexChunkScorer();

        List<ScoredChunk> noBackfill = scorer.scoreChunks(CHUNKS, (Query) null, 3, false);
        assertTrue(noBackfill.isEmpty());

        List<ScoredChunk> withBackfill = scorer.scoreChunks(CHUNKS, (Query) null, 3, true);
        assertThat(withBackfill.size(), equalTo(3));
        for (ScoredChunk chunk : withBackfill) {
            assertThat(chunk.score(), equalTo(0.0f));
        }
        assertThat(withBackfill.get(0).content(), equalTo(CHUNKS.get(0)));
        assertThat(withBackfill.get(1).content(), equalTo(CHUNKS.get(1)));
        assertThat(withBackfill.get(2).content(), equalTo(CHUNKS.get(2)));
    }

    public void testScoreChunksWithQueryEmptyChunks() {
        var scorer = new MemoryIndexChunkScorer();
        Query query = scorer.buildQuery("dogs");

        assertTrue(scorer.scoreChunks(List.of(), query, 3, true).isEmpty());
        assertTrue(scorer.scoreChunks(null, query, 3, true).isEmpty());
    }

}
