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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RecursiveChunkingSettingsTests extends AbstractWireSerializingTestCase<RecursiveChunkingSettings> {

    public void testFromMapValidSettingsWithSeparators() {
        var maxChunkSize = randomIntBetween(10, 300);
        var separators = randomList(1, 10, () -> randomAlphaOfLength(1));
        Map<String, Object> validSettings = buildChunkingSettingsMap(maxChunkSize, Optional.empty(), Optional.of(separators));

        RecursiveChunkingSettings settings = RecursiveChunkingSettings.fromMap(validSettings);

        assertEquals(maxChunkSize, settings.getMaxChunkSize());
        assertEquals(separators, settings.getSeparators());
    }

    public void testFromMapValidSettingsWithSeparatorSet() {
        var maxChunkSize = randomIntBetween(10, 300);
        var separatorSet = randomFrom(SeparatorSet.values());
        Map<String, Object> validSettings = buildChunkingSettingsMap(maxChunkSize, Optional.of(separatorSet.name()), Optional.empty());

        RecursiveChunkingSettings settings = RecursiveChunkingSettings.fromMap(validSettings);

        assertEquals(maxChunkSize, settings.getMaxChunkSize());
        assertEquals(separatorSet.getSeparators(), settings.getSeparators());
    }

    public void testFromMapMaxChunkSizeTooSmall() {
        Map<String, Object> invalidSettings = buildChunkingSettingsMap(randomIntBetween(0, 9), Optional.empty(), Optional.empty());

        assertThrows(ValidationException.class, () -> RecursiveChunkingSettings.fromMap(invalidSettings));
    }

    public void testFromMapMaxChunkSizeTooLarge() {
        Map<String, Object> invalidSettings = buildChunkingSettingsMap(randomIntBetween(301, 500), Optional.empty(), Optional.empty());

        assertThrows(ValidationException.class, () -> RecursiveChunkingSettings.fromMap(invalidSettings));
    }

    public void testFromMapInvalidSeparatorSet() {
        Map<String, Object> invalidSettings = buildChunkingSettingsMap(randomIntBetween(10, 300), Optional.of("invalid"), Optional.empty());

        assertThrows(ValidationException.class, () -> RecursiveChunkingSettings.fromMap(invalidSettings));
    }

    public void testFromMapInvalidSettingKey() {
        Map<String, Object> invalidSettings = buildChunkingSettingsMap(randomIntBetween(10, 300), Optional.empty(), Optional.empty());
        invalidSettings.put("invalid_key", "invalid_value");

        assertThrows(ValidationException.class, () -> RecursiveChunkingSettings.fromMap(invalidSettings));
    }

    public void testFromMapBothSeparatorsAndSeparatorSet() {
        Map<String, Object> invalidSettings = buildChunkingSettingsMap(
            randomIntBetween(10, 300),
            Optional.of("default"),
            Optional.of(List.of("\n\n", "\n"))
        );

        assertThrows(ValidationException.class, () -> RecursiveChunkingSettings.fromMap(invalidSettings));
    }

    public void testFromMapEmptySeparators() {
        Map<String, Object> invalidSettings = buildChunkingSettingsMap(randomIntBetween(10, 300), Optional.empty(), Optional.of(List.of()));

        assertThrows(ValidationException.class, () -> RecursiveChunkingSettings.fromMap(invalidSettings));
    }

    private Map<String, Object> buildChunkingSettingsMap(
        int maxChunkSize,
        Optional<String> separatorSet,
        Optional<List<String>> separators
    ) {
        Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put(ChunkingSettingsOptions.STRATEGY.toString(), ChunkingStrategy.RECURSIVE.toString());
        settingsMap.put(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), maxChunkSize);
        separatorSet.ifPresent(s -> settingsMap.put(ChunkingSettingsOptions.SEPARATOR_SET.toString(), s));
        separators.ifPresent(strings -> settingsMap.put(ChunkingSettingsOptions.SEPARATORS.toString(), strings));
        return settingsMap;
    }

    @Override
    protected Writeable.Reader<RecursiveChunkingSettings> instanceReader() {
        return RecursiveChunkingSettings::new;
    }

    @Override
    protected RecursiveChunkingSettings createTestInstance() {
        int maxChunkSize = randomIntBetween(10, 300);
        int numSeparators = randomIntBetween(1, 10);
        List<String> separators = new ArrayList<>();
        for (int i = 0; i < numSeparators; i++) {
            separators.add(randomAlphaOfLength(1));
        }

        return new RecursiveChunkingSettings(maxChunkSize, separators);
    }

    @Override
    protected RecursiveChunkingSettings mutateInstance(RecursiveChunkingSettings instance) throws IOException {
        int maxChunkSize = randomValueOtherThan(instance.getMaxChunkSize(), () -> randomIntBetween(10, 300));
        List<String> separators = instance.getSeparators();
        separators.add(randomAlphaOfLength(1));
        return new RecursiveChunkingSettings(maxChunkSize, separators);
    }
}
