/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.search;

import org.elasticsearch.search.fetch.subphase.ChunkResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.OffsetAndScore;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceFieldMapper.OffsetSource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class SemanticChunksFetchSubPhaseTests extends ESTestCase {

    private static final String TEST_FIELD = "body";
    private static final String FULL_TEXT = "The quick brown fox jumps over the lazy dog. "
        + "Elasticsearch is a distributed search engine. "
        + "Semantic text enables vector search. "
        + "Chunks are scored individually.";

    /**
     * Creates a content extractor that uses substring offsets from the test text.
     */
    private static Function<OffsetAndScore, String> testContentExtractor() {
        return entry -> {
            if (entry.offset() == null) {
                return null;
            }
            return FULL_TEXT.substring(entry.offset().start(), entry.offset().end());
        };
    }

    private static OffsetAndScore chunk(int index, int start, int end, float score) {
        return new OffsetAndScore(index, new OffsetSource(TEST_FIELD, start, end), score);
    }

    public void testChunksPerDocOnly() {
        List<OffsetAndScore> chunks = List.of(
            chunk(0, 0, 45, 0.9f),
            chunk(1, 45, 93, 0.5f),
            chunk(2, 93, 130, 0.7f),
            chunk(3, 130, FULL_TEXT.length(), 0.3f)
        );

        List<ChunkResult> results = SemanticChunksFetchSubPhase.filterAndSortChunks(chunks, testContentExtractor(), null, 2);
        assertEquals(2, results.size());
        // Sorted by score desc: 0.9, 0.7
        assertEquals(0.9f, results.get(0).score(), 0.001f);
        assertEquals(0.7f, results.get(1).score(), 0.001f);
    }

    public void testMinScoreOnly() {
        List<OffsetAndScore> chunks = List.of(
            chunk(0, 0, 45, 0.9f),
            chunk(1, 45, 93, 0.5f),
            chunk(2, 93, 130, 0.7f),
            chunk(3, 130, FULL_TEXT.length(), 0.3f)
        );

        List<ChunkResult> results = SemanticChunksFetchSubPhase.filterAndSortChunks(chunks, testContentExtractor(), 0.6f, null);
        assertEquals(2, results.size());
        assertEquals(0.9f, results.get(0).score(), 0.001f);
        assertEquals(0.7f, results.get(1).score(), 0.001f);
    }

    public void testBothMinScoreAndChunksPerDoc() {
        List<OffsetAndScore> chunks = List.of(
            chunk(0, 0, 45, 0.9f),
            chunk(1, 45, 93, 0.8f),
            chunk(2, 93, 130, 0.7f),
            chunk(3, 130, FULL_TEXT.length(), 0.3f)
        );

        // min_score=0.6 keeps 3, chunks_per_doc=2 caps to 2
        List<ChunkResult> results = SemanticChunksFetchSubPhase.filterAndSortChunks(chunks, testContentExtractor(), 0.6f, 2);
        assertEquals(2, results.size());
        assertEquals(0.9f, results.get(0).score(), 0.001f);
        assertEquals(0.8f, results.get(1).score(), 0.001f);
    }

    public void testNoChunksAboveMinScore() {
        List<OffsetAndScore> chunks = List.of(chunk(0, 0, 45, 0.1f), chunk(1, 45, 93, 0.2f));

        List<ChunkResult> results = SemanticChunksFetchSubPhase.filterAndSortChunks(chunks, testContentExtractor(), 0.5f, null);
        assertTrue(results.isEmpty());
    }

    public void testScoreOrderingWithTies() {
        // Two chunks with the same score should be ordered by start offset ascending
        List<OffsetAndScore> chunks = List.of(chunk(0, 93, 130, 0.7f), chunk(1, 0, 45, 0.7f), chunk(2, 45, 93, 0.7f));

        List<ChunkResult> results = SemanticChunksFetchSubPhase.filterAndSortChunks(chunks, testContentExtractor(), null, null);
        assertEquals(3, results.size());
        // All have same score, so ordered by startOffset ascending
        assertEquals(0, results.get(0).startOffset());
        assertEquals(45, results.get(1).startOffset());
        assertEquals(93, results.get(2).startOffset());
    }

    public void testChunksPerDocOne() {
        List<OffsetAndScore> chunks = List.of(chunk(0, 0, 45, 0.5f), chunk(1, 45, 93, 0.9f), chunk(2, 93, 130, 0.7f));

        List<ChunkResult> results = SemanticChunksFetchSubPhase.filterAndSortChunks(chunks, testContentExtractor(), null, 1);
        assertEquals(1, results.size());
        assertEquals(0.9f, results.get(0).score(), 0.001f);
        assertEquals(FULL_TEXT.substring(45, 93), results.get(0).text());
    }

    public void testMinScoreZeroReturnsAll() {
        List<OffsetAndScore> chunks = List.of(chunk(0, 0, 45, 0.9f), chunk(1, 45, 93, 0.0f), chunk(2, 93, 130, 0.1f));

        List<ChunkResult> results = SemanticChunksFetchSubPhase.filterAndSortChunks(chunks, testContentExtractor(), 0.0f, null);
        assertEquals(3, results.size());
    }

    public void testSingleChunk() {
        List<OffsetAndScore> chunks = List.of(chunk(0, 0, 45, 0.8f));

        List<ChunkResult> results = SemanticChunksFetchSubPhase.filterAndSortChunks(chunks, testContentExtractor(), null, null);
        assertEquals(1, results.size());
        assertEquals(FULL_TEXT.substring(0, 45), results.get(0).text());
        assertEquals(0, results.get(0).startOffset());
        assertEquals(45, results.get(0).endOffset());
        assertEquals(0.8f, results.get(0).score(), 0.001f);
    }

    public void testEmptyChunksList() {
        List<ChunkResult> results = SemanticChunksFetchSubPhase.filterAndSortChunks(new ArrayList<>(), testContentExtractor(), null, null);
        assertTrue(results.isEmpty());
    }

    public void testChunksPerDocLargerThanAvailable() {
        List<OffsetAndScore> chunks = List.of(chunk(0, 0, 45, 0.9f), chunk(1, 45, 93, 0.5f));

        List<ChunkResult> results = SemanticChunksFetchSubPhase.filterAndSortChunks(chunks, testContentExtractor(), null, 10);
        assertEquals(2, results.size());
    }

    public void testNullContentSkipped() {
        // If content extractor returns null, that chunk should be skipped
        Function<OffsetAndScore, String> nullExtractor = entry -> null;
        List<OffsetAndScore> chunks = List.of(chunk(0, 0, 45, 0.9f), chunk(1, 45, 93, 0.5f));

        List<ChunkResult> results = SemanticChunksFetchSubPhase.filterAndSortChunks(chunks, nullExtractor, null, null);
        assertTrue(results.isEmpty());
    }
}
