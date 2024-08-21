/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ChunkingSettingsBuilderTests extends ESTestCase {

    public void testEmptyChunkingSettingsMap() {
        ChunkingSettings chunkingSettings = ChunkingSettingsBuilder.fromMap(Collections.emptyMap());

        assertTrue(chunkingSettings instanceof DefaultChunkingSettings);
    }

    public void testChunkingStrategyNotProvided() {
        Map<String, Object> settings = Map.of(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), randomNonNegativeInt());

        assertThrows(IllegalArgumentException.class, () -> { ChunkingSettingsBuilder.fromMap(settings); });
    }

    public void testValidChunkingSettingsMap() {
        chunkingSettingsMapToChunkingSettingsClass().forEach((chunkingSettings, chunkingSettingsClass) -> {
            assertEquals(chunkingSettingsClass, ChunkingSettingsBuilder.fromMap(new HashMap<>(chunkingSettings)).getClass());
        });
    }

    private Map<Map<String, Object>, Class<? extends ChunkingSettings>> chunkingSettingsMapToChunkingSettingsClass() {
        var maxChunkSize = randomNonNegativeInt();
        return Map.of(
            Map.of(
                ChunkingSettingsOptions.STRATEGY.toString(),
                ChunkingStrategy.WORD.toString(),
                ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
                maxChunkSize,
                ChunkingSettingsOptions.OVERLAP.toString(),
                randomIntBetween(1, maxChunkSize / 2)
            ),
            WordBoundaryChunkingSettings.class,
            Map.of(
                ChunkingSettingsOptions.STRATEGY.toString(),
                ChunkingStrategy.SENTENCE.toString(),
                ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(),
                maxChunkSize
            ),
            SentenceBoundaryChunkingSettings.class
        );
    }
}
