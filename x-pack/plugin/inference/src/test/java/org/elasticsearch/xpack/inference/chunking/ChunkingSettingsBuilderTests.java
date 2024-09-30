/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ChunkingStrategy;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ChunkingSettingsBuilderTests extends ESTestCase {

    public static final WordBoundaryChunkingSettings DEFAULT_SETTINGS = new WordBoundaryChunkingSettings(250, 100);

    public void testEmptyChunkingSettingsMap() {
        ChunkingSettings chunkingSettings = ChunkingSettingsBuilder.fromMap(Collections.emptyMap());

        assertEquals(DEFAULT_SETTINGS, chunkingSettings);
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

    private Map<Map<String, Object>, ChunkingSettings> chunkingSettingsMapToChunkingSettings() {
        var maxChunkSize = randomNonNegativeInt();
        var overlap = randomIntBetween(1, maxChunkSize / 2);
        return Map.of(
            Map.of(
                ChunkingSettingsOptions.STRATEGY.toString(),
                ChunkingStrategy.WORD.toString(),
                ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
                maxChunkSize,
                ChunkingSettingsOptions.OVERLAP.toString(),
                overlap
            ),
            new WordBoundaryChunkingSettings(maxChunkSize, overlap),
            Map.of(
                ChunkingSettingsOptions.STRATEGY.toString(),
                ChunkingStrategy.SENTENCE.toString(),
                ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
                maxChunkSize
            ),
            new SentenceBoundaryChunkingSettings(maxChunkSize)
        );
    }
}
