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

import java.util.HashMap;
import java.util.Map;

public class ChunkingSettingsTests extends ESTestCase {

    public static ChunkingSettings createRandomChunkingSettings() {
        ChunkingStrategy randomStrategy = randomFrom(ChunkingStrategy.values());

        switch (randomStrategy) {
            case WORD -> {
                var maxChunkSize = randomNonNegativeInt();
                return new WordBoundaryChunkingSettings(maxChunkSize, randomIntBetween(1, maxChunkSize / 2));
            }
            case SENTENCE -> {
                return new SentenceBoundaryChunkingSettings(randomNonNegativeInt());
            }
            default -> throw new IllegalArgumentException("Unsupported random strategy [" + randomStrategy + "]");
        }
    }

    public static Map<String, Object> createRandomChunkingSettingsMap() {
        ChunkingStrategy randomStrategy = randomFrom(ChunkingStrategy.values());
        Map<String, Object> chunkingSettingsMap = new HashMap<>();
        chunkingSettingsMap.put(ChunkingSettingsOptions.STRATEGY.toString(), randomStrategy.toString());

        switch (randomStrategy) {
            case WORD -> {
                var maxChunkSize = randomNonNegativeInt();
                chunkingSettingsMap.put(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), maxChunkSize);
                chunkingSettingsMap.put(ChunkingSettingsOptions.OVERLAP.toString(), randomIntBetween(1, maxChunkSize / 2));

            }
            case SENTENCE -> {
                chunkingSettingsMap.put(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), randomNonNegativeInt());
            }
            default -> {
            }
        }
        return chunkingSettingsMap;
    }
}
