/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.common.model.Truncation;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaService.VALID_INPUT_TYPE_VALUES;
import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceFields.INPUT_TYPE_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.NvidiaServiceFields.TRUNCATE_FIELD_NAME;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class NvidiaEmbeddingsTaskSettingsTests extends AbstractBWCWireSerializationTestCase<NvidiaEmbeddingsTaskSettings> {
    private static final InputType INPUT_TYPE_INITIAL_VALUE = InputType.INGEST;
    private static final Truncation TRUNCATE_INITIAL_VALUE = Truncation.START;
    private static final InputType INPUT_TYPE_OVERRIDDEN_VALUE = InputType.SEARCH;
    private static final Truncation TRUNCATE_OVERRIDDEN_VALUE = Truncation.END;
    private static final InputType INPUT_TYPE_INVALID_VALUE = InputType.UNSPECIFIED;
    private static final String TRUNCATE_INVALID_VALUE = "invalid_truncation_value";

    public static NvidiaEmbeddingsTaskSettings createRandom() {
        var inputType = randomBoolean() ? randomFrom(VALID_INPUT_TYPE_VALUES) : null;
        Truncation truncation = randomBoolean() ? randomFrom(Truncation.values()) : null;

        return new NvidiaEmbeddingsTaskSettings(inputType, truncation);
    }

    public void testIsEmpty_AllFieldsPresent_False() {
        var taskSettings = new NvidiaEmbeddingsTaskSettings(INPUT_TYPE_INITIAL_VALUE, TRUNCATE_INITIAL_VALUE);
        assertThat(taskSettings.isEmpty(), is(false));
    }

    public void testIsEmpty_NoInputType_False() {
        var taskSettings = new NvidiaEmbeddingsTaskSettings(null, TRUNCATE_INITIAL_VALUE);
        assertThat(taskSettings.isEmpty(), is(false));
    }

    public void testIsEmpty_NoTruncation_False() {
        var taskSettings = new NvidiaEmbeddingsTaskSettings(INPUT_TYPE_INITIAL_VALUE, null);
        assertThat(taskSettings.isEmpty(), is(false));
    }

    public void testIsEmpty_True() {
        var taskSettings = NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS;
        assertThat(taskSettings.isEmpty(), is(true));
    }

    public void testUpdatedTaskSettings_EmptySettings_UseInitialSettings() {
        var initialSettings = new NvidiaEmbeddingsTaskSettings(INPUT_TYPE_INITIAL_VALUE, TRUNCATE_INITIAL_VALUE);
        NvidiaEmbeddingsTaskSettings updatedSettings = (NvidiaEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            buildTaskSettingsMap(null, null)
        );
        assertThat(updatedSettings, is(sameInstance(initialSettings)));
    }

    public void testUpdatedTaskSettings_SameSettings_UseInitialSettings() {
        var initialSettings = new NvidiaEmbeddingsTaskSettings(INPUT_TYPE_INITIAL_VALUE, TRUNCATE_INITIAL_VALUE);
        NvidiaEmbeddingsTaskSettings updatedSettings = (NvidiaEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            buildTaskSettingsMap(INPUT_TYPE_INITIAL_VALUE, TRUNCATE_INITIAL_VALUE)
        );
        assertThat(updatedSettings, is(sameInstance(initialSettings)));
    }

    public void testUpdatedTaskSettings_UpdatedSettings_UseNewSettings() {
        var initialSettings = new NvidiaEmbeddingsTaskSettings(INPUT_TYPE_INITIAL_VALUE, TRUNCATE_INITIAL_VALUE);
        NvidiaEmbeddingsTaskSettings updatedSettings = (NvidiaEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            buildTaskSettingsMap(INPUT_TYPE_OVERRIDDEN_VALUE, TRUNCATE_OVERRIDDEN_VALUE)
        );
        assertThat(updatedSettings.getInputType(), is(INPUT_TYPE_OVERRIDDEN_VALUE));
        assertThat(updatedSettings.getTruncation(), is(TRUNCATE_OVERRIDDEN_VALUE));
    }

    public void testUpdatedTaskSettings_OnlyInputType_MergeSettings() {
        var initialSettings = new NvidiaEmbeddingsTaskSettings(INPUT_TYPE_INITIAL_VALUE, TRUNCATE_INITIAL_VALUE);
        NvidiaEmbeddingsTaskSettings updatedSettings = (NvidiaEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            buildTaskSettingsMap(INPUT_TYPE_OVERRIDDEN_VALUE, null)
        );
        assertThat(updatedSettings.getInputType(), is(INPUT_TYPE_OVERRIDDEN_VALUE));
        assertThat(updatedSettings.getTruncation(), is(TRUNCATE_INITIAL_VALUE));
    }

    public void testUpdatedTaskSettings_OnlyTruncation_MergeSettings() {
        var initialSettings = new NvidiaEmbeddingsTaskSettings(INPUT_TYPE_INITIAL_VALUE, TRUNCATE_INITIAL_VALUE);
        NvidiaEmbeddingsTaskSettings updatedSettings = (NvidiaEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            buildTaskSettingsMap(null, TRUNCATE_OVERRIDDEN_VALUE)
        );
        assertThat(updatedSettings.getInputType(), is(INPUT_TYPE_INITIAL_VALUE));
        assertThat(updatedSettings.getTruncation(), is(TRUNCATE_OVERRIDDEN_VALUE));
    }

    public void testFromMap_CreatesEmptySettings_WhenAllFieldsAreNull() {
        assertThat(
            NvidiaEmbeddingsTaskSettings.fromMap(buildTaskSettingsMap(null, null)),
            is(sameInstance(NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS))
        );
    }

    public void testFromMap_CreatesEmptySettings_WhenMapIsNull() {
        assertThat(NvidiaEmbeddingsTaskSettings.fromMap(null), is(NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_CreatesSettings_WhenAllFieldsOfSettingsArePresent() {
        assertThat(
            NvidiaEmbeddingsTaskSettings.fromMap(buildTaskSettingsMap(INPUT_TYPE_INITIAL_VALUE, TRUNCATE_INITIAL_VALUE)),
            is(new NvidiaEmbeddingsTaskSettings(INPUT_TYPE_INITIAL_VALUE, TRUNCATE_INITIAL_VALUE))
        );
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsInvalid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsTaskSettings.fromMap(buildTaskSettingsMap(INPUT_TYPE_INVALID_VALUE, null))
        );

        assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [task_settings] Invalid value [%s] received. [input_type] must be one of [%s];",
                    INPUT_TYPE_INVALID_VALUE,
                    getValidValuesSortedAndCombined(VALID_INPUT_TYPE_VALUES)
                )
            )
        );
    }

    public void testFromMap_ReturnsFailure_WhenTruncationIsInvalid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(TRUNCATE_FIELD_NAME, TRUNCATE_INVALID_VALUE)))
        );

        assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [task_settings] Invalid value [%s] received. [truncate] must be one of [%s];",
                    TRUNCATE_INVALID_VALUE,
                    getValidValuesSortedAndCombined(Truncation.ALL)
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
        var thrownException = expectThrows(AssertionError.class, () -> new NvidiaEmbeddingsTaskSettings(INPUT_TYPE_INVALID_VALUE, null));
        assertThat(thrownException.getMessage(), is("received invalid input type value [unspecified]"));
    }

    public void testOf_KeepsOriginalValuesWhenRequestSettingsAreEmpty() {
        var taskSettings = new NvidiaEmbeddingsTaskSettings(InputType.INGEST, Truncation.START);
        var overriddenTaskSettings = NvidiaEmbeddingsTaskSettings.of(taskSettings, NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS);
        assertThat(overriddenTaskSettings, is(sameInstance(taskSettings)));
    }

    public void testOf_UsesRequestTaskSettings() {
        var taskSettings = NvidiaEmbeddingsTaskSettings.EMPTY_SETTINGS;
        var overriddenTaskSettings = NvidiaEmbeddingsTaskSettings.of(
            taskSettings,
            new NvidiaEmbeddingsTaskSettings(InputType.INGEST, Truncation.START)
        );

        assertThat(overriddenTaskSettings, is(new NvidiaEmbeddingsTaskSettings(InputType.INGEST, Truncation.START)));
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
        Truncation truncation = instance.getTruncation();
        switch (between(0, 1)) {
            case 0 -> inputType = randomValueOtherThan(inputType, () -> randomBoolean() ? randomFrom(VALID_INPUT_TYPE_VALUES) : null);
            case 1 -> truncation = randomValueOtherThan(truncation, () -> randomBoolean() ? randomFrom(Truncation.values()) : null);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new NvidiaEmbeddingsTaskSettings(inputType, truncation);
    }

    /**
     * Helper method to build a task settings map for testing.
     * @param inputType the input type to include in the map
     * @param truncation the truncation to include in the map
     * @return a map representing the task settings
     */
    public static Map<String, Object> buildTaskSettingsMap(@Nullable InputType inputType, @Nullable Truncation truncation) {
        var map = new HashMap<String, Object>();

        if (inputType != null) {
            map.put(INPUT_TYPE_FIELD_NAME, inputType.toString());
        }
        if (truncation != null) {
            map.put(TRUNCATE_FIELD_NAME, truncation.toString());
        }

        return map;
    }

    @Override
    protected NvidiaEmbeddingsTaskSettings mutateInstanceForVersion(NvidiaEmbeddingsTaskSettings instance, TransportVersion version) {
        return instance;
    }
}
