/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.chunking;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ChunkingStrategy;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder.ELASTIC_RERANKER_EXTRA_TOKEN_COUNT;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder.ELASTIC_RERANKER_TOKEN_LIMIT;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder.WORDS_PER_TOKEN;

public class ChunkingSettingsBuilderTests extends ESTestCase {

    public static final SentenceBoundaryChunkingSettings DEFAULT_SETTINGS = new SentenceBoundaryChunkingSettings(250, 1);

    public void testNullChunkingSettingsMap() {
        ChunkingSettings chunkingSettings = ChunkingSettingsBuilder.fromMap(null);
        assertEquals(ChunkingSettingsBuilder.OLD_DEFAULT_SETTINGS, chunkingSettings);

        ChunkingSettings chunkingSettingsOrNull = ChunkingSettingsBuilder.fromMap(null, false);
        assertNull(chunkingSettingsOrNull);
    }

    public void testEmptyChunkingSettingsMap() {
        ChunkingSettings chunkingSettings = ChunkingSettingsBuilder.fromMap(Collections.emptyMap());
        assertEquals(DEFAULT_SETTINGS, chunkingSettings);

        ChunkingSettings chunkingSettingsOrNull = ChunkingSettingsBuilder.fromMap(Map.of(), false);
        assertNull(chunkingSettingsOrNull);
    }

    public void testChunkingStrategyNotProvided() {
        Map<String, Object> settings = Map.of(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), randomNonNegativeInt());

        assertThrows(IllegalArgumentException.class, () -> { ChunkingSettingsBuilder.fromMap(settings); });
    }

    public void testValidChunkingSettingsMap() {
        chunkingSettingsMapToChunkingSettings().forEach((chunkingSettingsMap, chunkingSettings) -> {
            assertEquals(chunkingSettings, ChunkingSettingsBuilder.fromMap(new HashMap<>(chunkingSettingsMap)));
        });
    }

    public void testBuildChunkingSettingsForElasticReranker_QueryTokenCountLessThanHalfOfTokenLimit() {
        // Generate a word count for a non-empty query that takes up less than half the token limit
        int maxQueryTokenCount = (ELASTIC_RERANKER_TOKEN_LIMIT - ELASTIC_RERANKER_EXTRA_TOKEN_COUNT) / 2;
        int queryWordCount = randomIntBetween(1, (int) (maxQueryTokenCount * WORDS_PER_TOKEN) - 1);
        var queryTokenCount = Math.ceil(queryWordCount / WORDS_PER_TOKEN);
        ChunkingSettings chunkingSettings = ChunkingSettingsBuilder.buildChunkingSettingsForElasticRerank(queryWordCount);
        assertTrue(chunkingSettings instanceof SentenceBoundaryChunkingSettings);
        SentenceBoundaryChunkingSettings sentenceBoundaryChunkingSettings = (SentenceBoundaryChunkingSettings) chunkingSettings;
        int expectedMaxChunkSize = (int) ((ELASTIC_RERANKER_TOKEN_LIMIT - ELASTIC_RERANKER_EXTRA_TOKEN_COUNT - queryTokenCount)
            * WORDS_PER_TOKEN);
        assertEquals(expectedMaxChunkSize, (int) sentenceBoundaryChunkingSettings.maxChunkSize());
        assertEquals(1, sentenceBoundaryChunkingSettings.sentenceOverlap());
    }

    public void testBuildChunkingSettingsForElasticReranker_QueryTokenCountMoreThanHalfOfTokenLimit() {
        // Generate a word count for a non-empty query that takes up more than half the token limit
        int maxQueryTokenCount = (ELASTIC_RERANKER_TOKEN_LIMIT - ELASTIC_RERANKER_EXTRA_TOKEN_COUNT) / 2;
        int queryWordCount = randomIntBetween((int) (maxQueryTokenCount * WORDS_PER_TOKEN), Integer.MAX_VALUE);
        ChunkingSettings chunkingSettings = ChunkingSettingsBuilder.buildChunkingSettingsForElasticRerank(queryWordCount);
        assertTrue(chunkingSettings instanceof SentenceBoundaryChunkingSettings);
        SentenceBoundaryChunkingSettings sentenceBoundaryChunkingSettings = (SentenceBoundaryChunkingSettings) chunkingSettings;
        int expectedMaxChunkSize = (int) (Math.floor((float) ELASTIC_RERANKER_TOKEN_LIMIT / 2) * WORDS_PER_TOKEN);
        assertEquals(expectedMaxChunkSize, (int) sentenceBoundaryChunkingSettings.maxChunkSize());
        assertEquals(1, sentenceBoundaryChunkingSettings.sentenceOverlap());
    }

    private Map<Map<String, Object>, ChunkingSettings> chunkingSettingsMapToChunkingSettings() {
        var maxChunkSizeWordBoundaryChunkingSettings = randomIntBetween(10, 300);
        var overlap = randomIntBetween(1, maxChunkSizeWordBoundaryChunkingSettings / 2);
        var maxChunkSizeSentenceBoundaryChunkingSettings = randomIntBetween(20, 300);

        return Map.of(
            Map.of(
                ChunkingSettingsOptions.STRATEGY.toString(),
                ChunkingStrategy.WORD.toString(),
                ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
                maxChunkSizeWordBoundaryChunkingSettings,
                ChunkingSettingsOptions.OVERLAP.toString(),
                overlap
            ),
            new WordBoundaryChunkingSettings(maxChunkSizeWordBoundaryChunkingSettings, overlap),
            Map.of(
                ChunkingSettingsOptions.STRATEGY.toString(),
                ChunkingStrategy.SENTENCE.toString(),
                ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
                maxChunkSizeSentenceBoundaryChunkingSettings
            ),
            new SentenceBoundaryChunkingSettings(maxChunkSizeSentenceBoundaryChunkingSettings, 1)
        );
    }
}
