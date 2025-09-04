/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceFields;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InputTypeTests.randomWithIngestAndSearch;
import static org.elasticsearch.xpack.inference.services.voyageai.VoyageAIService.VALID_INPUT_TYPE_VALUES;
import static org.hamcrest.Matchers.is;

public class VoyageAIEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<VoyageAIEmbeddingsTaskSettings> {

    public static VoyageAIEmbeddingsTaskSettings createRandom() {
        var inputType = randomBoolean() ? randomWithIngestAndSearch() : null;
        var truncation = randomBoolean();

        return new VoyageAIEmbeddingsTaskSettings(inputType, truncation);
    }

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings_NotUpdated_UseInitialSettings() {
        var initialSettings = createRandom();
        var newSettings = new VoyageAIEmbeddingsTaskSettings((InputType) null, null);
        Map<String, Object> newSettingsMap = new HashMap<>();
        VoyageAIEmbeddingsTaskSettings updatedSettings = (VoyageAIEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        assertEquals(initialSettings.getInputType(), updatedSettings.getInputType());
    }

    public void testUpdatedTaskSettings_Updated_UseNewSettings() {
        var initialSettings = createRandom();
        var newSettings = new VoyageAIEmbeddingsTaskSettings(randomWithIngestAndSearch(), randomBoolean());
        Map<String, Object> newSettingsMap = new HashMap<>();
        newSettingsMap.put(VoyageAIEmbeddingsTaskSettings.INPUT_TYPE, newSettings.getInputType().toString());
        VoyageAIEmbeddingsTaskSettings updatedSettings = (VoyageAIEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        assertEquals(newSettings.getInputType(), updatedSettings.getInputType());
    }

    public void testFromMap_CreatesEmptySettings_WhenAllFieldsAreNull() {
        MatcherAssert.assertThat(
            VoyageAIEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of())),
            is(new VoyageAIEmbeddingsTaskSettings((InputType) null, null))
        );
    }

    public void testFromMap_CreatesEmptySettings_WhenMapIsNull() {
        MatcherAssert.assertThat(
            VoyageAIEmbeddingsTaskSettings.fromMap(null),
            is(new VoyageAIEmbeddingsTaskSettings((InputType) null, null))
        );
    }

    public void testFromMap_CreatesSettings_WhenAllFieldsOfSettingsArePresent() {
        MatcherAssert.assertThat(
            VoyageAIEmbeddingsTaskSettings.fromMap(
                new HashMap<>(
                    Map.of(VoyageAIEmbeddingsTaskSettings.INPUT_TYPE, InputType.INGEST.toString(), VoyageAIServiceFields.TRUNCATION, false)
                )
            ),
            is(new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, false))
        );
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsInvalid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> VoyageAIEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(VoyageAIEmbeddingsTaskSettings.INPUT_TYPE, "abc", VoyageAIServiceFields.TRUNCATION, false))
            )
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
            () -> VoyageAIEmbeddingsTaskSettings.fromMap(
                new HashMap<>(
                    Map.of(VoyageAIEmbeddingsTaskSettings.INPUT_TYPE, InputType.INGEST.toString(), VoyageAIServiceFields.TRUNCATION, "abc")
                )
            )
        );

        MatcherAssert.assertThat(
            exception.getMessage(),
            is("Validation Failed: 1: field [truncation] is not of the expected type. The value [abc] cannot be converted to a [Boolean];")
        );
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsUnspecified() {
        var exception = expectThrows(
            ValidationException.class,
            () -> VoyageAIEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(VoyageAIEmbeddingsTaskSettings.INPUT_TYPE, InputType.UNSPECIFIED.toString()))
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
        var thrownException = expectThrows(AssertionError.class, () -> new VoyageAIEmbeddingsTaskSettings(InputType.UNSPECIFIED, null));
        MatcherAssert.assertThat(thrownException.getMessage(), is("received invalid input type value [unspecified]"));
    }

    public void testOf_KeepsOriginalValuesWhenRequestSettingsAreNull() {
        var taskSettings = new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, false);
        var overriddenTaskSettings = VoyageAIEmbeddingsTaskSettings.of(taskSettings, VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS);
        MatcherAssert.assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOf_UsesRequestTaskSettings() {
        var taskSettings = new VoyageAIEmbeddingsTaskSettings((InputType) null, null);
        var overriddenTaskSettings = VoyageAIEmbeddingsTaskSettings.of(
            taskSettings,
            new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, true)
        );

        MatcherAssert.assertThat(overriddenTaskSettings, is(new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, true)));
    }

    @Override
    protected Writeable.Reader<VoyageAIEmbeddingsTaskSettings> instanceReader() {
        return VoyageAIEmbeddingsTaskSettings::new;
    }

    @Override
    protected VoyageAIEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected VoyageAIEmbeddingsTaskSettings mutateInstance(VoyageAIEmbeddingsTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, VoyageAIEmbeddingsTaskSettingsTests::createRandom);
    }

    public static Map<String, Object> getTaskSettingsMapEmpty() {
        return new HashMap<>();
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable InputType inputType) {
        var map = new HashMap<String, Object>();

        if (inputType != null) {
            map.put(VoyageAIEmbeddingsTaskSettings.INPUT_TYPE, inputType.toString());
        }

        return map;
    }
}
