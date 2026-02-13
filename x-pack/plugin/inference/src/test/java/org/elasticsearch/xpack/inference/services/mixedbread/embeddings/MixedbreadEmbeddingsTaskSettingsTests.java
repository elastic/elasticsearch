/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.TestUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class MixedbreadEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<MixedbreadEmbeddingsTaskSettings> {
    public static MixedbreadEmbeddingsTaskSettings createRandom() {
        var prompt = randomAlphaOfLengthOrNull(10);
        var normalized = randomBoolean() ? randomBoolean() : null;

        return new MixedbreadEmbeddingsTaskSettings(prompt, normalized);
    }

    public void testFromMap_WithValidValues_ReturnsSettings() {
        Map<String, Object> taskMap = Map.of(
            MixedbreadUtils.PROMPT_FIELD,
            TestUtils.PROMPT_INITIAL_VALUE,
            MixedbreadUtils.NORMALIZED_FIELD,
            TestUtils.NORMALIZED_INITIAL_VALUE
        );
        var settings = MixedbreadEmbeddingsTaskSettings.fromMap(new HashMap<>(taskMap));
        assertEquals(TestUtils.PROMPT_INITIAL_VALUE, settings.getPrompt());
        assertEquals(TestUtils.NORMALIZED_INITIAL_VALUE, settings.getNormalized());
    }

    public void testFromMap_WithNullValues_ReturnsSettingsWithNulls() {
        var settings = MixedbreadEmbeddingsTaskSettings.fromMap(Map.of());
        assertNull(settings.getPrompt());
        assertNull(settings.getNormalized());
    }

    public void testUpdatedTaskSettings_WithEmptyMap_ReturnsSameSettings() {
        var initialSettings = new MixedbreadEmbeddingsTaskSettings(TestUtils.PROMPT_INITIAL_VALUE, TestUtils.NORMALIZED_INITIAL_VALUE);
        MixedbreadEmbeddingsTaskSettings updatedSettings = initialSettings.updatedTaskSettings(Map.of());
        assertThat(initialSettings, is(sameInstance(updatedSettings)));
    }

    public void testUpdatedTaskSettings_WithNewInputType_ReturnsUpdatedSettings() {
        var initialSettings = new MixedbreadEmbeddingsTaskSettings(TestUtils.PROMPT_INITIAL_VALUE, TestUtils.NORMALIZED_INITIAL_VALUE);
        Map<String, Object> newSettings = Map.of(MixedbreadUtils.PROMPT_FIELD, TestUtils.PROMPT_OVERRIDDEN_VALUE);
        MixedbreadEmbeddingsTaskSettings updatedSettings = initialSettings.updatedTaskSettings(newSettings);
        assertEquals(TestUtils.PROMPT_OVERRIDDEN_VALUE, updatedSettings.getPrompt());
        assertEquals(initialSettings.getNormalized(), updatedSettings.getNormalized());
    }

    public void testUpdatedTaskSettings_WithNewTruncation_ReturnsUpdatedSettings() {
        var initialSettings = new MixedbreadEmbeddingsTaskSettings(TestUtils.PROMPT_INITIAL_VALUE, TestUtils.NORMALIZED_INITIAL_VALUE);
        Map<String, Object> newSettings = Map.of(MixedbreadUtils.NORMALIZED_FIELD, TestUtils.NORMALIZED_OVERRIDDEN_VALUE);
        MixedbreadEmbeddingsTaskSettings updatedSettings = initialSettings.updatedTaskSettings(newSettings);
        assertEquals(initialSettings.getPrompt(), updatedSettings.getPrompt());
        assertEquals(TestUtils.NORMALIZED_OVERRIDDEN_VALUE, updatedSettings.getNormalized());
    }

    public void testUpdatedTaskSettings_WithMultipleNewValues_ReturnsUpdatedSettings() {
        var initialSettings = new MixedbreadEmbeddingsTaskSettings(TestUtils.PROMPT_INITIAL_VALUE, TestUtils.NORMALIZED_INITIAL_VALUE);
        Map<String, Object> newSettings = Map.of(
            MixedbreadUtils.PROMPT_FIELD,
            TestUtils.PROMPT_OVERRIDDEN_VALUE,
            MixedbreadUtils.NORMALIZED_FIELD,
            TestUtils.NORMALIZED_OVERRIDDEN_VALUE
        );
        MixedbreadEmbeddingsTaskSettings updatedSettings = initialSettings.updatedTaskSettings(newSettings);
        assertEquals(TestUtils.PROMPT_OVERRIDDEN_VALUE, updatedSettings.getPrompt());
        assertEquals(TestUtils.NORMALIZED_OVERRIDDEN_VALUE, updatedSettings.getNormalized());
    }

    @Override
    protected Writeable.Reader<MixedbreadEmbeddingsTaskSettings> instanceReader() {
        return MixedbreadEmbeddingsTaskSettings::new;
    }

    @Override
    protected MixedbreadEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected MixedbreadEmbeddingsTaskSettings mutateInstance(MixedbreadEmbeddingsTaskSettings instance) throws IOException {
        var prompt = instance.getPrompt();
        var normalized = instance.getNormalized();
        switch (randomInt(1)) {
            case 0 -> prompt = randomValueOtherThan(prompt, () -> randomAlphaOfLengthOrNull(10));
            case 1 -> normalized = randomValueOtherThan(normalized, () -> randomBoolean() ? randomBoolean() : null);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new MixedbreadEmbeddingsTaskSettings(prompt, normalized);
    }

    /**
     * Helper method to build a task settings map for testing.
     * @param prompt used by the model for embedding generation
     * @param normalized specifies whether to normalize the embeddings
     * @return a map representing the task settings
     */
    public static Map<String, Object> getTaskSettingsMap(@Nullable String prompt, @Nullable Boolean normalized) {
        var map = new HashMap<String, Object>();

        if (prompt != null) {
            map.put(MixedbreadUtils.PROMPT_FIELD, prompt);
        }

        if (normalized != null) {
            map.put(MixedbreadUtils.NORMALIZED_FIELD, normalized);
        }

        return map;
    }
}
