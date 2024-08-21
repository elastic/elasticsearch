/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class WordBoundaryChunkingSettingsTests extends ESTestCase {

    public void testMaxChunkSizeNotProvided() {
        assertThrows(ValidationException.class, () -> {
            WordBoundaryChunkingSettings.fromMap(buildChunkingSettingsMap(Optional.empty(), Optional.of(randomNonNegativeInt())));
        });
    }

    public void testOverlapNotProvided() {
        assertThrows(ValidationException.class, () -> {
            WordBoundaryChunkingSettings.fromMap(buildChunkingSettingsMap(Optional.of(randomNonNegativeInt()), Optional.empty()));
        });
    }

    public void testValidInputsProvided() {
        int maxChunkSize = randomNonNegativeInt();
        int overlap = randomIntBetween(1, maxChunkSize / 2);
        WordBoundaryChunkingSettings settings = WordBoundaryChunkingSettings.fromMap(
            buildChunkingSettingsMap(Optional.of(maxChunkSize), Optional.of(overlap))
        );

        assertEquals(settings.getChunkingStrategy(), ChunkingStrategy.WORD.toString());
        assertEquals(settings.maxChunkSize, maxChunkSize);
        assertEquals(settings.overlap, overlap);
    }

    public Map<String, Object> buildChunkingSettingsMap(Optional<Integer> maxChunkSize, Optional<Integer> overlap) {
        Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put(ChunkingSettingsOptions.STRATEGY.toString(), ChunkingStrategy.WORD.toString());
        maxChunkSize.ifPresent(maxChunkSizeValue -> settingsMap.put(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), maxChunkSizeValue));
        overlap.ifPresent(overlapValue -> settingsMap.put(ChunkingSettingsOptions.OVERLAP.toString(), overlapValue));

        return settingsMap;
    }
}
