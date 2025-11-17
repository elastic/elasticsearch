/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaService.VALID_INPUT_TYPE_VALUES;
import static org.hamcrest.Matchers.is;

public class NvidiaEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<NvidiaEmbeddingsTaskSettings> {

    public static NvidiaEmbeddingsTaskSettings createRandom() {
        var inputType = randomBoolean() ? randomFrom(VALID_INPUT_TYPE_VALUES) : null;
        CohereTruncation truncation = randomBoolean() ? randomFrom(CohereTruncation.values()) : null;

        return new NvidiaEmbeddingsTaskSettings(inputType, truncation);
    }

    public void testIsEmpty_AllFieldsPresent_False() {
        var taskSettings = new NvidiaEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START);
        assertThat(taskSettings.isEmpty(), is(false));
    }

    public void testIsEmpty_NoInputType_False() {
        var taskSettings = new NvidiaEmbeddingsTaskSettings(null, CohereTruncation.START);
        assertThat(taskSettings.isEmpty(), is(false));
    }

    public void testIsEmpty_NoTruncation_False() {
        var taskSettings = new NvidiaEmbeddingsTaskSettings(InputType.INGEST, null);
        assertThat(taskSettings.isEmpty(), is(false));
    }

    public void testIsEmpty_True() {
        var taskSettings = NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS;
        assertThat(taskSettings.isEmpty(), is(true));
    }

    public void testUpdatedTaskSettings_NotUpdated_UseInitialSettings() {
        var initialSettings = new NvidiaEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START);
        NvidiaEmbeddingsTaskSettings updatedSettings = (NvidiaEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(new HashMap<>())
        );
        assertThat(updatedSettings.getInputType(), is(initialSettings.getInputType()));
        assertThat(updatedSettings.getTruncation(), is(initialSettings.getTruncation()));
    }

    public void testUpdatedTaskSettings_Updated_UseNewSettings() {
        var initialSettings = new NvidiaEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.END);
        var newSettings = new NvidiaEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START);
        Map<String, Object> newSettingsMap = new HashMap<>();
        newSettingsMap.put(NvidiaEmbeddingsTaskSettings.INPUT_TYPE, newSettings.getInputType().toString());
        newSettingsMap.put(CohereServiceFields.TRUNCATE, newSettings.getTruncation().toString());
        NvidiaEmbeddingsTaskSettings updatedSettings = (NvidiaEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        assertThat(updatedSettings.getInputType(), is(newSettings.getInputType()));
        assertThat(updatedSettings.getTruncation(), is(newSettings.getTruncation()));
    }

    public void testFromMap_CreatesEmptySettings_WhenAllFieldsAreNull() {
        MatcherAssert.assertThat(
            NvidiaEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of())),
            is(NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS)
        );
    }

    public void testFromMap_CreatesEmptySettings_WhenMapIsNull() {
        MatcherAssert.assertThat(NvidiaEmbeddingsTaskSettings.fromMap(null), is(NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_CreatesSettings_WhenAllFieldsOfSettingsArePresent() {
        MatcherAssert.assertThat(
            NvidiaEmbeddingsTaskSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        NvidiaEmbeddingsTaskSettings.INPUT_TYPE,
                        InputType.INGEST.toString(),
                        CohereServiceFields.TRUNCATE,
                        CohereTruncation.START.toString()
                    )
                )
            ),
            is(new NvidiaEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START))
        );
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsInvalid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(NvidiaEmbeddingsTaskSettings.INPUT_TYPE, "abc")))
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

    public void testFromMap_ReturnsFailure_WhenTruncationIsInvalid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(CohereServiceFields.TRUNCATE, "abc")))
        );

        MatcherAssert.assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [task_settings] Invalid value [abc] received. [truncate] must be one of [%s];",
                    getValidValuesSortedAndCombined(CohereTruncation.ALL)
                )
            )
        );
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsUnspecified() {
        var exception = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(NvidiaEmbeddingsTaskSettings.INPUT_TYPE, InputType.UNSPECIFIED.toString()))
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
        var thrownException = expectThrows(AssertionError.class, () -> new NvidiaEmbeddingsTaskSettings(InputType.UNSPECIFIED, null));
        MatcherAssert.assertThat(thrownException.getMessage(), is("received invalid input type value [unspecified]"));
    }

    public void testOf_KeepsOriginalValuesWhenRequestSettingsAreNull() {
        var taskSettings = new NvidiaEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START);
        var overriddenTaskSettings = NvidiaEmbeddingsTaskSettings.of(taskSettings, NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS);
        MatcherAssert.assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOf_UsesRequestTaskSettings() {
        var taskSettings = NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS;
        var overriddenTaskSettings = NvidiaEmbeddingsTaskSettings.of(
            taskSettings,
            new NvidiaEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START)
        );

        MatcherAssert.assertThat(overriddenTaskSettings, is(new NvidiaEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.START)));
    }

    @Override
    protected Writeable.Reader<NvidiaEmbeddingsTaskSettings> instanceReader() {
        return NvidiaEmbeddingsTaskSettings::new;
    }

    @Override
    protected NvidiaEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected NvidiaEmbeddingsTaskSettings mutateInstance(NvidiaEmbeddingsTaskSettings instance) throws IOException {
        InputType inputType = instance.getInputType();
        CohereTruncation truncation = instance.getTruncation();
        switch (between(0, 1)) {
            case 0 -> inputType = randomValueOtherThan(inputType, () -> randomBoolean() ? randomFrom(VALID_INPUT_TYPE_VALUES) : null);
            case 1 -> truncation = randomValueOtherThan(truncation, () -> randomBoolean() ? randomFrom(CohereTruncation.values()) : null);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new NvidiaEmbeddingsTaskSettings(inputType, truncation);
    }

    public static Map<String, Object> buildTaskSettingsMap(@Nullable InputType inputType, @Nullable CohereTruncation truncation) {
        var map = new HashMap<String, Object>();

        if (inputType != null) {
            map.put(NvidiaEmbeddingsTaskSettings.INPUT_TYPE, inputType.toString());
        }
        if (truncation != null) {
            map.put(CohereServiceFields.TRUNCATE, truncation.toString());
        }

        return map;
    }

}
