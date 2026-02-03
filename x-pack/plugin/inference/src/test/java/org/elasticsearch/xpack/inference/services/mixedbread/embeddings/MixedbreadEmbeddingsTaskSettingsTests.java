/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadService;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.TestUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class MixedbreadEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<MixedbreadEmbeddingsTaskSettings> {
    public static MixedbreadEmbeddingsTaskSettings createRandom() {
        var inputType = randomFrom(MixedbreadService.VALID_INPUT_TYPE_VALUES);
        var truncation = randomFrom(Truncation.values());

        return new MixedbreadEmbeddingsTaskSettings(inputType, truncation);
    }

    public void testFromMap_WithValidValues_ReturnsSettings() {
        Map<String, Object> taskMap = Map.of(
            MixedbreadUtils.INPUT_TYPE_FIELD,
            TestUtils.INPUT_TYPE_INITIAL_ELASTIC_VALUE.name(),
            MixedbreadUtils.TRUNCATE_FIELD,
            TestUtils.TRUNCATE_INITIAL_ELASTIC_VALUE.name()
        );
        var settings = MixedbreadEmbeddingsTaskSettings.fromMap(new HashMap<>(taskMap));
        assertEquals(TestUtils.INPUT_TYPE_INITIAL_ELASTIC_VALUE, settings.getInputType());
        assertEquals(TestUtils.TRUNCATE_INITIAL_ELASTIC_VALUE, settings.getTruncation());
    }

    public void testFromMap_WithNullValues_ReturnsSettingsWithNulls() {
        var settings = MixedbreadEmbeddingsTaskSettings.fromMap(Map.of());
        assertNull(settings.getInputType());
        assertNull(settings.getTruncation());
    }

    public void testFromMap_WithInvalidInputType_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            MixedbreadUtils.INPUT_TYPE_FIELD,
            "invalid",
            MixedbreadUtils.TRUNCATE_FIELD,
            TestUtils.TRUNCATE_INITIAL_ELASTIC_VALUE.name()
        );
        var thrownException = expectThrows(
            ValidationException.class,
            () -> MixedbreadEmbeddingsTaskSettings.fromMap(new HashMap<>(taskMap))
        );
        assertThat(
            thrownException.getMessage(),
            containsString(
                "[task_settings] Invalid value [invalid] received. "
                    + "[input_type] must be one of [classification, clustering, ingest, internal_ingest, internal_search, search]"
            )
        );
    }

    public void testFromMap_WithInvalidTruncation_ThrowsValidationException() {
        Map<String, Object> taskMap = Map.of(
            MixedbreadUtils.INPUT_TYPE_FIELD,
            TestUtils.INPUT_TYPE_INITIAL_ELASTIC_VALUE.name(),
            MixedbreadUtils.TRUNCATE_FIELD,
            "invalid"
        );
        var thrownException = expectThrows(
            ValidationException.class,
            () -> MixedbreadEmbeddingsTaskSettings.fromMap(new HashMap<>(taskMap))
        );
        assertThat(
            thrownException.getMessage(),
            containsString("[task_settings] Invalid value [invalid] received. " + "[truncate] must be one of [end, none, start]")
        );
    }

    public void testUpdatedTaskSettings_WithEmptyMap_ReturnsSameSettings() {
        var initialSettings = new MixedbreadEmbeddingsTaskSettings(
            TestUtils.INPUT_TYPE_INITIAL_ELASTIC_VALUE,
            TestUtils.TRUNCATE_INITIAL_ELASTIC_VALUE
        );
        MixedbreadEmbeddingsTaskSettings updatedSettings = initialSettings.updatedTaskSettings(Map.of());
        assertThat(initialSettings, is(sameInstance(updatedSettings)));
    }

    public void testUpdatedTaskSettings_WithNewInputType_ReturnsUpdatedSettings() {
        var initialSettings = new MixedbreadEmbeddingsTaskSettings(
            TestUtils.INPUT_TYPE_INITIAL_ELASTIC_VALUE,
            TestUtils.TRUNCATE_INITIAL_ELASTIC_VALUE
        );
        Map<String, Object> newSettings = Map.of(MixedbreadUtils.INPUT_TYPE_FIELD, TestUtils.INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE.name());
        MixedbreadEmbeddingsTaskSettings updatedSettings = initialSettings.updatedTaskSettings(newSettings);
        assertEquals(TestUtils.INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE, updatedSettings.getInputType());
        assertEquals(initialSettings.getTruncation(), updatedSettings.getTruncation());
    }

    public void testUpdatedTaskSettings_WithNewTruncation_ReturnsUpdatedSettings() {
        var initialSettings = new MixedbreadEmbeddingsTaskSettings(
            TestUtils.INPUT_TYPE_INITIAL_ELASTIC_VALUE,
            TestUtils.TRUNCATE_INITIAL_ELASTIC_VALUE
        );
        Map<String, Object> newSettings = Map.of(MixedbreadUtils.TRUNCATE_FIELD, TestUtils.TRUNCATE_OVERRIDDEN_ELASTIC_VALUE.name());
        MixedbreadEmbeddingsTaskSettings updatedSettings = initialSettings.updatedTaskSettings(newSettings);
        assertEquals(initialSettings.getInputType(), updatedSettings.getInputType());
        assertEquals(TestUtils.TRUNCATE_OVERRIDDEN_ELASTIC_VALUE, updatedSettings.getTruncation());
    }

    public void testUpdatedTaskSettings_WithMultipleNewValues_ReturnsUpdatedSettings() {
        var initialSettings = new MixedbreadEmbeddingsTaskSettings(
            TestUtils.INPUT_TYPE_INITIAL_ELASTIC_VALUE,
            TestUtils.TRUNCATE_INITIAL_ELASTIC_VALUE
        );
        Map<String, Object> newSettings = Map.of(
            MixedbreadUtils.INPUT_TYPE_FIELD,
            TestUtils.INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE.name(),
            MixedbreadUtils.TRUNCATE_FIELD,
            TestUtils.TRUNCATE_OVERRIDDEN_ELASTIC_VALUE.name()
        );
        MixedbreadEmbeddingsTaskSettings updatedSettings = initialSettings.updatedTaskSettings(newSettings);
        assertEquals(TestUtils.INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE, updatedSettings.getInputType());
        assertEquals(TestUtils.TRUNCATE_OVERRIDDEN_ELASTIC_VALUE, updatedSettings.getTruncation());
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
        var inputType = instance.getInputType();
        var truncation = instance.getTruncation();
        switch (randomInt(1)) {
            case 0 -> inputType = randomValueOtherThan(inputType, () -> randomFrom(MixedbreadService.VALID_INPUT_TYPE_VALUES));
            case 1 -> truncation = randomValueOtherThan(truncation, () -> randomFrom(Truncation.values()));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new MixedbreadEmbeddingsTaskSettings(inputType, truncation);
    }

    /**
     * Helper method to build a task settings map for testing.
     * @param inputType the input type to include in the map
     * @param truncation specifies whether input text should be truncated if it exceeds the model's maximum supported length
     * @return a map representing the task settings
     */
    public static Map<String, Object> getTaskSettingsMap(@Nullable InputType inputType, @Nullable Truncation truncation) {
        var map = new HashMap<String, Object>();

        if (inputType != null) {
            map.put(MixedbreadUtils.INPUT_TYPE_FIELD, inputType.name());
        }

        if (truncation != null) {
            map.put(MixedbreadUtils.TRUNCATE_FIELD, truncation.name());
        }

        return map;
    }
}
