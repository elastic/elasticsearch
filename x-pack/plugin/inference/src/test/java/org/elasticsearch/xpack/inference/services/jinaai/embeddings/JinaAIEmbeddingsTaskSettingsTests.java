/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIService.VALID_INPUT_TYPE_VALUES;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings.JINA_AI_CONFIGURABLE_LATE_CHUNKING;
import static org.hamcrest.Matchers.is;

public class JinaAIEmbeddingsTaskSettingsTests extends AbstractBWCWireSerializationTestCase<JinaAIEmbeddingsTaskSettings> {

    public static JinaAIEmbeddingsTaskSettings createRandom() {
        var inputType = randomBoolean() ? randomFrom(VALID_INPUT_TYPE_VALUES) : null;
        var lateChunking = randomBoolean() ? randomBoolean() : null;

        return new JinaAIEmbeddingsTaskSettings(inputType, lateChunking);
    }

    public void testIsEmpty_ReturnsTrue_ForEmptySettings() {
        var emptySettings = new JinaAIEmbeddingsTaskSettings(null, null);
        assertTrue(emptySettings.isEmpty());
    }

    public void testIsEmpty_ReturnsFalse_WhenAnyValuesAreSet() {
        var settingsWithValuesList = List.of(
            new JinaAIEmbeddingsTaskSettings(randomFrom(VALID_INPUT_TYPE_VALUES), null),
            new JinaAIEmbeddingsTaskSettings(null, randomBoolean()),
            new JinaAIEmbeddingsTaskSettings(randomFrom(VALID_INPUT_TYPE_VALUES), randomBoolean())
        );

        settingsWithValuesList.forEach(settings -> assertFalse(settings.isEmpty()));
    }

    public void testUpdatedTaskSettings_ReturnsInitialSettings_WhenNewSettingsAreEmpty() {
        var initialSettings = createRandom();
        Map<String, Object> newSettingsMap = new HashMap<>();
        JinaAIEmbeddingsTaskSettings updatedSettings = (JinaAIEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(newSettingsMap);
        assertEquals(updatedSettings, initialSettings);
    }

    public void testUpdatedTaskSettings_ReturnsUpdatedSettings_WhenNewSettingsHaveInputType() {
        var initialSettings = createRandom();
        var newInputType = randomValueOtherThan(initialSettings.getInputType(), () -> randomFrom(VALID_INPUT_TYPE_VALUES));
        Map<String, Object> newSettingsMap = new HashMap<>();
        newSettingsMap.put(JinaAIEmbeddingsTaskSettings.INPUT_TYPE, newInputType.toString());

        JinaAIEmbeddingsTaskSettings updatedSettings = (JinaAIEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(newSettingsMap);
        JinaAIEmbeddingsTaskSettings expectedSettings = new JinaAIEmbeddingsTaskSettings(newInputType, initialSettings.getLateChunking());
        assertEquals(expectedSettings, updatedSettings);
    }

    public void testUpdatedTaskSettings_ReturnsUpdatedSettings_WhenNewSettingsHaveLateChunking() {
        var initialSettings = createRandom();
        var newLateChunking = initialSettings.getLateChunking() == null ? randomBoolean() : initialSettings.getLateChunking() == false;
        Map<String, Object> newSettingsMap = new HashMap<>();
        newSettingsMap.put(JinaAIEmbeddingsTaskSettings.LATE_CHUNKING, newLateChunking);

        JinaAIEmbeddingsTaskSettings updatedSettings = (JinaAIEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(newSettingsMap);
        JinaAIEmbeddingsTaskSettings expectedSettings = new JinaAIEmbeddingsTaskSettings(initialSettings.getInputType(), newLateChunking);
        assertEquals(expectedSettings, updatedSettings);
    }

    public void testUpdatedTaskSettings_ReturnsUpdatedSettings_WhenNewSettingsHaveAllValuesSet() {
        var initialSettings = createRandom();
        var newInputType = randomValueOtherThan(initialSettings.getInputType(), () -> randomFrom(VALID_INPUT_TYPE_VALUES));
        var newLateChunking = initialSettings.getLateChunking() == null ? randomBoolean() : initialSettings.getLateChunking() == false;
        Map<String, Object> newSettingsMap = new HashMap<>();
        newSettingsMap.put(JinaAIEmbeddingsTaskSettings.INPUT_TYPE, newInputType.toString());
        newSettingsMap.put(JinaAIEmbeddingsTaskSettings.LATE_CHUNKING, newLateChunking);

        JinaAIEmbeddingsTaskSettings updatedSettings = (JinaAIEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(newSettingsMap);
        JinaAIEmbeddingsTaskSettings expectedSettings = new JinaAIEmbeddingsTaskSettings(newInputType, newLateChunking);
        assertEquals(expectedSettings, updatedSettings);
    }

    public void testFromMap_CreatesEmptySettings_WhenAllFieldsAreNull() {
        MatcherAssert.assertThat(
            JinaAIEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of())),
            is(new JinaAIEmbeddingsTaskSettings((InputType) null))
        );
    }

    public void testFromMap_CreatesEmptySettings_WhenMapIsNull() {
        MatcherAssert.assertThat(JinaAIEmbeddingsTaskSettings.fromMap(null), is(new JinaAIEmbeddingsTaskSettings((InputType) null)));
    }

    public void testFromMap_CreatesSettings_WhenInputTypeIsPresent() {
        var inputType = randomFrom(VALID_INPUT_TYPE_VALUES);

        var actualSettings = JinaAIEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(JinaAIEmbeddingsTaskSettings.INPUT_TYPE, inputType.toString()))
        );
        var expectedSettings = new JinaAIEmbeddingsTaskSettings(inputType, null);
        assertEquals(expectedSettings, actualSettings);
    }

    public void testFromMap_CreatesSettings_WhenLateChunkingIsPresent() {
        var lateChunking = randomBoolean();

        var actualSettings = JinaAIEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(JinaAIEmbeddingsTaskSettings.LATE_CHUNKING, lateChunking))
        );
        var expectedSettings = new JinaAIEmbeddingsTaskSettings(null, lateChunking);
        assertEquals(expectedSettings, actualSettings);
    }

    public void testFromMap_CreatesSettings_WhenAllFieldsOfSettingsArePresent() {
        var inputType = randomFrom(VALID_INPUT_TYPE_VALUES);
        var lateChunking = randomBoolean();

        var actualSettings = JinaAIEmbeddingsTaskSettings.fromMap(
            new HashMap<>(
                Map.of(
                    JinaAIEmbeddingsTaskSettings.INPUT_TYPE,
                    inputType.toString(),
                    JinaAIEmbeddingsTaskSettings.LATE_CHUNKING,
                    lateChunking
                )
            )
        );
        var expectedSettings = new JinaAIEmbeddingsTaskSettings(inputType, lateChunking);
        assertEquals(expectedSettings, actualSettings);
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsInvalid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> JinaAIEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(JinaAIEmbeddingsTaskSettings.INPUT_TYPE, "abc")))
        );

        MatcherAssert.assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [task_settings] Invalid value [abc] received. [input_type] must be one of [%s];",
                    getValidValuesSortedAndCombined(VALID_INPUT_TYPE_VALUES)
                )
            )
        );
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsUnspecified() {
        var exception = expectThrows(
            ValidationException.class,
            () -> JinaAIEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(JinaAIEmbeddingsTaskSettings.INPUT_TYPE, InputType.UNSPECIFIED.toString()))
            )
        );

        MatcherAssert.assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [task_settings] Invalid value [unspecified] received. [input_type] must be one of [%s];",
                    getValidValuesSortedAndCombined(VALID_INPUT_TYPE_VALUES)
                )
            )
        );
    }

    private static <E extends Enum<E>> String getValidValuesSortedAndCombined(EnumSet<E> validValues) {
        var validValuesAsStrings = validValues.stream().map(value -> value.toString().toLowerCase(Locale.ROOT)).toArray(String[]::new);
        Arrays.sort(validValuesAsStrings);

        return String.join(", ", validValuesAsStrings);
    }

    public void testXContent_ThrowsAssertionFailure_WhenInputTypeIsUnspecified() {
        var thrownException = expectThrows(AssertionError.class, () -> new JinaAIEmbeddingsTaskSettings(InputType.UNSPECIFIED));
        MatcherAssert.assertThat(thrownException.getMessage(), is("received invalid input type value [unspecified]"));
    }

    public void testOf_KeepsOriginalValuesWhenRequestSettingsAreNull() {
        var taskSettings = new JinaAIEmbeddingsTaskSettings(InputType.INGEST);
        var overriddenTaskSettings = JinaAIEmbeddingsTaskSettings.of(taskSettings, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS);
        MatcherAssert.assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOf_UsesRequestTaskSettings() {
        var taskSettings = new JinaAIEmbeddingsTaskSettings((InputType) null);
        var overriddenTaskSettings = JinaAIEmbeddingsTaskSettings.of(taskSettings, new JinaAIEmbeddingsTaskSettings(InputType.INGEST));

        MatcherAssert.assertThat(overriddenTaskSettings, is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST)));
    }

    @Override
    protected Writeable.Reader<JinaAIEmbeddingsTaskSettings> instanceReader() {
        return JinaAIEmbeddingsTaskSettings::new;
    }

    @Override
    protected JinaAIEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected JinaAIEmbeddingsTaskSettings mutateInstance(JinaAIEmbeddingsTaskSettings instance) throws IOException {
        InputType inputType = instance.getInputType();
        Boolean lateChunking = instance.getLateChunking();
        switch (randomInt(1)) {
            case 0 -> inputType = randomValueOtherThan(inputType, () -> randomFrom(VALID_INPUT_TYPE_VALUES));
            case 1 -> lateChunking = lateChunking == null ? randomBoolean() : lateChunking == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new JinaAIEmbeddingsTaskSettings(inputType, lateChunking);
    }

    public static Map<String, Object> getTaskSettingsMapEmpty() {
        return new HashMap<>();
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable InputType inputType) {
        var map = new HashMap<String, Object>();

        if (inputType != null) {
            map.put(JinaAIEmbeddingsTaskSettings.INPUT_TYPE, inputType.toString());
        }

        return map;
    }

    @Override
    protected JinaAIEmbeddingsTaskSettings mutateInstanceForVersion(JinaAIEmbeddingsTaskSettings instance, TransportVersion version) {
        if (version.supports(JINA_AI_CONFIGURABLE_LATE_CHUNKING)) {
            return new JinaAIEmbeddingsTaskSettings(instance.getInputType(), instance.getLateChunking());
        } else {
            return new JinaAIEmbeddingsTaskSettings(instance.getInputType(), null);
        }
    }
}
