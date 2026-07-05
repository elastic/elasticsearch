/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.chunking;

import org.elasticsearch.common.Strings;
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

import static org.elasticsearch.xpack.core.inference.chunking.RecursiveChunkingSettings.MAX_CHUNK_SIZE_LOWER_LIMIT;
import static org.hamcrest.Matchers.containsString;

public class RecursiveChunkingSettingsTests extends AbstractWireSerializingTestCase<RecursiveChunkingSettings> {

    public void testFromMapValidSettingsWithSeparators() {
        var maxChunkSize = randomIntBetween(10, 300);
        var separators = randomList(1, 10, () -> randomAlphaOfLength(1));
        Map<String, Object> validSettings = buildChunkingSettingsMap(maxChunkSize, Optional.empty(), Optional.of(separators));

        RecursiveChunkingSettings settings = RecursiveChunkingSettings.fromMap(validSettings);

        assertEquals(maxChunkSize, (int) settings.maxChunkSize());
        assertEquals(separators, settings.getSeparators());
    }

    public void testFromMapValidSettingsWithSeparatorGroup() {
        var maxChunkSize = randomIntBetween(10, 300);
        var separatorGroup = randomFrom(SeparatorGroup.values());
        Map<String, Object> validSettings = buildChunkingSettingsMap(maxChunkSize, Optional.of(separatorGroup.name()), Optional.empty());

        RecursiveChunkingSettings settings = RecursiveChunkingSettings.fromMap(validSettings);

        assertEquals(maxChunkSize, (int) settings.maxChunkSize());
        assertEquals(separatorGroup.getSeparators(), settings.getSeparators());
    }

    public void testFromMapMaxChunkSizeTooSmall() {
        Map<String, Object> invalidSettings = buildChunkingSettingsMap(randomIntBetween(0, 9), Optional.empty(), Optional.empty());

        assertThrows(ValidationException.class, () -> RecursiveChunkingSettings.fromMap(invalidSettings));
    }

    public void testFromMapInvalidSeparatorGroup() {
        Map<String, Object> invalidSettings = buildChunkingSettingsMap(randomIntBetween(10, 300), Optional.of("invalid"), Optional.empty());

        assertThrows(ValidationException.class, () -> RecursiveChunkingSettings.fromMap(invalidSettings));
    }

    public void testFromMapInvalidSettingKey() {
        Map<String, Object> invalidSettings = buildChunkingSettingsMap(randomIntBetween(10, 300), Optional.empty(), Optional.empty());
        invalidSettings.put("invalid_key", "invalid_value");

        assertThrows(ValidationException.class, () -> RecursiveChunkingSettings.fromMap(invalidSettings));
    }

    public void testFromMapBothSeparatorsAndSeparatorGroup() {
        Map<String, Object> invalidSettings = buildChunkingSettingsMap(
            randomIntBetween(10, 300),
            Optional.of("default"),
            Optional.of(List.of("\n\n", "\n"))
        );

        assertThrows(ValidationException.class, () -> RecursiveChunkingSettings.fromMap(invalidSettings));
    }

    public void testValidateWithValidSettings() {
        var settings = new RecursiveChunkingSettings(randomIntBetween(MAX_CHUNK_SIZE_LOWER_LIMIT, 300), List.of("\n\n", "\n"));
        settings.validate(); // should not throw
    }

    public void testValidateWithBoundaryMaxChunkSize() {
        var settings = new RecursiveChunkingSettings(MAX_CHUNK_SIZE_LOWER_LIMIT, List.of("\n\n", "\n"));
        settings.validate(); // should not throw
    }

    public void testValidateWithInvalidMaxChunkSize() {
        var invalidMaxChunkSize = randomIntBetween(0, MAX_CHUNK_SIZE_LOWER_LIMIT - 1);
        var settings = new RecursiveChunkingSettings(invalidMaxChunkSize, List.of("\n\n", "\n"));
        var e = assertThrows(ValidationException.class, settings::validate);
        assertThat(
            e.getMessage(),
            containsString(Strings.format("max_chunk_size [%s] must be above %s", invalidMaxChunkSize, MAX_CHUNK_SIZE_LOWER_LIMIT))
        );
    }

    public void testValidateWithEmptySeparators() {
        var settings = new RecursiveChunkingSettings(randomIntBetween(MAX_CHUNK_SIZE_LOWER_LIMIT, 300), List.of());
        var e = assertThrows(ValidationException.class, settings::validate);
        assertThat(e.getMessage(), containsString("can not have an empty list of separators"));
    }

    public void testValidateWithInvalidMaxChunkSizeAndEmptySeparators() {
        var invalidMaxChunkSize = randomIntBetween(0, MAX_CHUNK_SIZE_LOWER_LIMIT - 1);
        var settings = new RecursiveChunkingSettings(invalidMaxChunkSize, List.of());
        var e = assertThrows(ValidationException.class, settings::validate);
        assertThat(
            e.getMessage(),
            containsString(Strings.format("max_chunk_size [%s] must be above %s", invalidMaxChunkSize, MAX_CHUNK_SIZE_LOWER_LIMIT))
        );
        assertThat(e.getMessage(), containsString("can not have an empty list of separators"));
    }

    public void testFromMapEmptySeparators() {
        Map<String, Object> invalidSettings = buildChunkingSettingsMap(randomIntBetween(10, 300), Optional.empty(), Optional.of(List.of()));

        assertThrows(ValidationException.class, () -> RecursiveChunkingSettings.fromMap(invalidSettings));
    }

    private Map<String, Object> buildChunkingSettingsMap(
        int maxChunkSize,
        Optional<String> separatorGroup,
        Optional<List<String>> separators
    ) {
        Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put(ChunkingSettingsOptions.STRATEGY.toString(), ChunkingStrategy.RECURSIVE.toString());
        settingsMap.put(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), maxChunkSize);
        separatorGroup.ifPresent(s -> settingsMap.put(ChunkingSettingsOptions.SEPARATOR_GROUP.toString(), s));
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
        int maxChunkSize = randomValueOtherThan(instance.maxChunkSize(), () -> randomIntBetween(10, 300));
        List<String> separators = instance.getSeparators();
        separators.add(randomAlphaOfLength(1));
        return new RecursiveChunkingSettings(maxChunkSize, separators);
    }
}
