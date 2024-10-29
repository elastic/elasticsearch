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

public class SentenceBoundaryChunkingSettingsTests extends AbstractWireSerializingTestCase<SentenceBoundaryChunkingSettings> {

    public void testMaxChunkSizeNotProvided() {
        assertThrows(
            ValidationException.class,
            () -> { SentenceBoundaryChunkingSettings.fromMap(buildChunkingSettingsMap(Optional.empty())); }
        );
    }

    public void testInvalidInputsProvided() {
        var chunkingSettingsMap = buildChunkingSettingsMap(Optional.of(randomIntBetween(20, 300)));
        chunkingSettingsMap.put(randomAlphaOfLength(10), randomNonNegativeInt());

        assertThrows(ValidationException.class, () -> { SentenceBoundaryChunkingSettings.fromMap(chunkingSettingsMap); });
    }

    public void testValidInputsProvided() {
        int maxChunkSize = randomIntBetween(20, 300);
        SentenceBoundaryChunkingSettings settings = SentenceBoundaryChunkingSettings.fromMap(
            buildChunkingSettingsMap(Optional.of(maxChunkSize))
        );

        assertEquals(settings.getChunkingStrategy(), ChunkingStrategy.SENTENCE);
        assertEquals(settings.maxChunkSize, maxChunkSize);
    }

    public Map<String, Object> buildChunkingSettingsMap(Optional<Integer> maxChunkSize) {
        Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put(ChunkingSettingsOptions.STRATEGY.toString(), ChunkingStrategy.SENTENCE.toString());
        maxChunkSize.ifPresent(maxChunkSizeValue -> settingsMap.put(ChunkingSettingsOptions.MAX_CHUNK_SIZE.toString(), maxChunkSizeValue));

        return settingsMap;
    }

    @Override
    protected Writeable.Reader<SentenceBoundaryChunkingSettings> instanceReader() {
        return SentenceBoundaryChunkingSettings::new;
    }

    @Override
    protected SentenceBoundaryChunkingSettings createTestInstance() {
        return new SentenceBoundaryChunkingSettings(randomIntBetween(20, 300), randomBoolean() ? 0 : 1);
    }

    @Override
    protected SentenceBoundaryChunkingSettings mutateInstance(SentenceBoundaryChunkingSettings instance) throws IOException {
        var chunkSize = randomValueOtherThan(instance.maxChunkSize, () -> randomIntBetween(20, 300));
        return new SentenceBoundaryChunkingSettings(chunkSize, instance.sentenceOverlap);
    }
}
