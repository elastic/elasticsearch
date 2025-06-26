/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InputTypeTests.randomWithoutUnspecified;
import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIService.VALID_INPUT_TYPE_VALUES;
import static org.hamcrest.Matchers.is;

public class JinaAIEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<JinaAIEmbeddingsTaskSettings> {

    public static JinaAIEmbeddingsTaskSettings createRandom() {
        var inputType = randomBoolean() ? randomWithoutUnspecified() : null;

        return new JinaAIEmbeddingsTaskSettings(inputType);
    }

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings_NotUpdated_UseInitialSettings() {
        var initialSettings = createRandom();
        var newSettings = new JinaAIEmbeddingsTaskSettings((InputType) null);
        Map<String, Object> newSettingsMap = new HashMap<>();
        JinaAIEmbeddingsTaskSettings updatedSettings = (JinaAIEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        assertEquals(initialSettings.getInputType(), updatedSettings.getInputType());
    }

    public void testUpdatedTaskSettings_Updated_UseNewSettings() {
        var initialSettings = createRandom();
        var newSettings = new JinaAIEmbeddingsTaskSettings(randomWithoutUnspecified());
        Map<String, Object> newSettingsMap = new HashMap<>();
        newSettingsMap.put(JinaAIEmbeddingsTaskSettings.INPUT_TYPE, newSettings.getInputType().toString());
        JinaAIEmbeddingsTaskSettings updatedSettings = (JinaAIEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        assertEquals(newSettings.getInputType(), updatedSettings.getInputType());
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

    public void testFromMap_CreatesSettings_WhenAllFieldsOfSettingsArePresent() {
        MatcherAssert.assertThat(
            JinaAIEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(JinaAIEmbeddingsTaskSettings.INPUT_TYPE, InputType.INGEST.toString()))
            ),
            is(new JinaAIEmbeddingsTaskSettings(InputType.INGEST))
        );
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
        return randomValueOtherThan(instance, JinaAIEmbeddingsTaskSettingsTests::createRandom);
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
}
