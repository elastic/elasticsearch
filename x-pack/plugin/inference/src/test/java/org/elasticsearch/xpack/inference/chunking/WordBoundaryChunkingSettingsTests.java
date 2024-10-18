/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ChunkingStrategy;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class WordBoundaryChunkingSettingsTests extends AbstractWireSerializingTestCase<WordBoundaryChunkingSettings> {

    public void testMaxChunkSizeNotProvided() {
        assertThrows(ValidationException.class, () -> {
            WordBoundaryChunkingSettings.fromMap(buildChunkingSettingsMap(Optional.empty(), Optional.of(randomNonNegativeInt())));
        });
    }

    public void testOverlapNotProvided() {
        assertThrows(ValidationException.class, () -> {
            WordBoundaryChunkingSettings.fromMap(buildChunkingSettingsMap(Optional.of(randomIntBetween(10, 300)), Optional.empty()));
        });
    }

    public void testInvalidInputsProvided() {
        var maxChunkSize = randomIntBetween(10, 300);
        var chunkingSettingsMap = buildChunkingSettingsMap(Optional.of(maxChunkSize), Optional.of(randomIntBetween(1, maxChunkSize / 2)));
        chunkingSettingsMap.put(randomAlphaOfLength(10), randomNonNegativeInt());

        assertThrows(ValidationException.class, () -> { WordBoundaryChunkingSettings.fromMap(chunkingSettingsMap); });
    }

    public void testOverlapGreaterThanHalfMaxChunkSize() {
        var maxChunkSize = randomIntBetween(10, 300);
        var overlap = randomIntBetween((maxChunkSize / 2) + 1, maxChunkSize);
        assertThrows(ValidationException.class, () -> {
            WordBoundaryChunkingSettings.fromMap(buildChunkingSettingsMap(Optional.of(maxChunkSize), Optional.of(overlap)));
        });
    }

    public void testValidInputsProvided() {
        int maxChunkSize = randomIntBetween(10, 300);
        int overlap = randomIntBetween(1, maxChunkSize / 2);
        WordBoundaryChunkingSettings settings = WordBoundaryChunkingSettings.fromMap(
            buildChunkingSettingsMap(Optional.of(maxChunkSize), Optional.of(overlap))
        );

        assertEquals(settings.getChunkingStrategy(), ChunkingStrategy.WORD);
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

    @Override
    protected Writeable.Reader<WordBoundaryChunkingSettings> instanceReader() {
        return WordBoundaryChunkingSettings::new;
    }

    @Override
    protected WordBoundaryChunkingSettings createTestInstance() {
        var maxChunkSize = randomIntBetween(10, 300);
        return new WordBoundaryChunkingSettings(maxChunkSize, randomIntBetween(1, maxChunkSize / 2));
    }

    @Override
    protected WordBoundaryChunkingSettings mutateInstance(WordBoundaryChunkingSettings instance) throws IOException {
        var maxChunkSize = randomValueOtherThan(instance.maxChunkSize, () -> randomIntBetween(10, 300));
        var overlap = randomValueOtherThan(instance.overlap, () -> randomIntBetween(1, maxChunkSize / 2));

        return new WordBoundaryChunkingSettings(maxChunkSize, overlap);
    }
}
