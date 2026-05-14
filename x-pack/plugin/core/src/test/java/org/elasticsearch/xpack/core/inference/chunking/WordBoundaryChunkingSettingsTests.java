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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.core.inference.chunking.WordBoundaryChunkingSettings.MAX_CHUNK_SIZE_LOWER_LIMIT;
import static org.hamcrest.Matchers.containsString;

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

    public void testValidateWithValidSettings() {
        var maxChunkSize = randomIntBetween(MAX_CHUNK_SIZE_LOWER_LIMIT, 300);
        var settings = new WordBoundaryChunkingSettings(maxChunkSize, randomIntBetween(1, maxChunkSize / 2));
        settings.validate(); // should not throw
    }

    public void testValidateWithBoundaryMaxChunkSize() {
        var settings = new WordBoundaryChunkingSettings(MAX_CHUNK_SIZE_LOWER_LIMIT, 1);
        settings.validate(); // should not throw
    }

    public void testValidateWithInvalidMaxChunkSize() {
        var invalidMaxChunkSize = randomIntBetween(0, MAX_CHUNK_SIZE_LOWER_LIMIT - 1);
        var settings = new WordBoundaryChunkingSettings(invalidMaxChunkSize, 1);
        var e = assertThrows(ValidationException.class, settings::validate);
        assertThat(
            e.getMessage(),
            containsString(Strings.format("max_chunk_size [%s] must be above %s", invalidMaxChunkSize, MAX_CHUNK_SIZE_LOWER_LIMIT))
        );
    }

    public void testValidateWithOverlapTooLarge() {
        var maxChunkSize = randomIntBetween(MAX_CHUNK_SIZE_LOWER_LIMIT, 300);
        var settings = new WordBoundaryChunkingSettings(maxChunkSize, (maxChunkSize / 2) + 1);
        var e = assertThrows(ValidationException.class, settings::validate);
        assertThat(e.getMessage(), containsString("must be less than or equal to half of max chunk size"));
    }

    public void testValidateWithBothInvalid() {
        var invalidMaxChunkSize = randomIntBetween(0, MAX_CHUNK_SIZE_LOWER_LIMIT - 1);
        var settings = new WordBoundaryChunkingSettings(invalidMaxChunkSize, 100);
        var e = assertThrows(ValidationException.class, settings::validate);
        assertThat(
            e.getMessage(),
            containsString(Strings.format("max_chunk_size [%s] must be above %s", invalidMaxChunkSize, MAX_CHUNK_SIZE_LOWER_LIMIT))
        );
        assertThat(e.getMessage(), containsString("must be less than or equal to half of max chunk size"));
    }

    public void testValidInputsProvided() {
        int maxChunkSize = randomIntBetween(10, 300);
        int overlap = randomIntBetween(1, maxChunkSize / 2);
        WordBoundaryChunkingSettings settings = WordBoundaryChunkingSettings.fromMap(
            buildChunkingSettingsMap(Optional.of(maxChunkSize), Optional.of(overlap))
        );

        assertEquals(settings.getChunkingStrategy(), ChunkingStrategy.WORD);
        assertEquals((int) settings.maxChunkSize(), maxChunkSize);
        assertEquals(settings.overlap(), overlap);
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
        var maxChunkSize = randomValueOtherThan(instance.maxChunkSize(), () -> randomIntBetween(10, 300));
        var overlap = randomValueOtherThan(instance.overlap(), () -> randomIntBetween(1, maxChunkSize / 2));

        return new WordBoundaryChunkingSettings(maxChunkSize, overlap);
    }
}
