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
