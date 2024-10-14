/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

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

import static org.elasticsearch.xpack.inference.InputTypeTests.randomWithoutUnspecified;
import static org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings.VALID_REQUEST_VALUES;
import static org.hamcrest.Matchers.is;

public class CohereEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<CohereEmbeddingsTaskSettings> {

    public static CohereEmbeddingsTaskSettings createRandom() {
        var inputType = randomBoolean() ? randomWithoutUnspecified() : null;
        var truncation = randomBoolean() ? randomFrom(CohereTruncation.values()) : null;

        return new CohereEmbeddingsTaskSettings(inputType, truncation);
    }

    public void testIsEmpty() {
        var randomSettings = createRandom();
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.getInputType() != null) {
            newSettingsMap.put(CohereEmbeddingsTaskSettings.INPUT_TYPE, newSettings.getInputType().toString());
        }
        if (newSettings.getTruncation() != null) {
            newSettingsMap.put(CohereServiceFields.TRUNCATE, newSettings.getTruncation().toString());
        }
        CohereEmbeddingsTaskSettings updatedSettings = (CohereEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        if (newSettings.getInputType() == null) {
            assertEquals(initialSettings.getInputType(), updatedSettings.getInputType());
        } else {
            assertEquals(newSettings.getInputType(), updatedSettings.getInputType());
        }
        if (newSettings.getTruncation() == null) {
            assertEquals(initialSettings.getTruncation(), updatedSettings.getTruncation());
        } else {
            assertEquals(newSettings.getTruncation(), updatedSettings.getTruncation());
        }
    }

    public void testFromMap_CreatesEmptySettings_WhenAllFieldsAreNull() {
        MatcherAssert.assertThat(
            CohereEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of())),
            is(new CohereEmbeddingsTaskSettings(null, null))
        );
    }

    public void testFromMap_CreatesEmptySettings_WhenMapIsNull() {
        MatcherAssert.assertThat(CohereEmbeddingsTaskSettings.fromMap(null), is(new CohereEmbeddingsTaskSettings(null, null)));
    }

    public void testFromMap_CreatesSettings_WhenAllFieldsOfSettingsArePresent() {
        MatcherAssert.assertThat(
            CohereEmbeddingsTaskSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        CohereEmbeddingsTaskSettings.INPUT_TYPE,
                        InputType.INGEST.toString(),
                        CohereServiceFields.TRUNCATE,
                        CohereTruncation.END.toString()
                    )
                )
            ),
            is(new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.END))
        );
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsInvalid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(CohereEmbeddingsTaskSettings.INPUT_TYPE, "abc")))
        );

        MatcherAssert.assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [task_settings] Invalid value [abc] received. [input_type] must be one of [%s];",
                    getValidValuesSortedAndCombined(VALID_REQUEST_VALUES)
                )
            )
        );
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsUnspecified() {
        var exception = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsTaskSettings.INPUT_TYPE, InputType.UNSPECIFIED.toString()))
            )
        );

        MatcherAssert.assertThat(
            exception.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [task_settings] Invalid value [unspecified] received. [input_type] must be one of [%s];",
                    getValidValuesSortedAndCombined(VALID_REQUEST_VALUES)
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
        var thrownException = expectThrows(AssertionError.class, () -> new CohereEmbeddingsTaskSettings(InputType.UNSPECIFIED, null));
        MatcherAssert.assertThat(thrownException.getMessage(), is("received invalid input type value [unspecified]"));
    }

    public void testOf_KeepsOriginalValuesWhenRequestSettingsAreNull_AndRequestInputTypeIsInvalid() {
        var taskSettings = new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.NONE);
        var overriddenTaskSettings = CohereEmbeddingsTaskSettings.of(
            taskSettings,
            CohereEmbeddingsTaskSettings.EMPTY_SETTINGS,
            InputType.UNSPECIFIED
        );
        MatcherAssert.assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOf_UsesRequestTaskSettings() {
        var taskSettings = new CohereEmbeddingsTaskSettings(null, CohereTruncation.NONE);
        var overriddenTaskSettings = CohereEmbeddingsTaskSettings.of(
            taskSettings,
            new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.END),
            InputType.UNSPECIFIED
        );

        MatcherAssert.assertThat(overriddenTaskSettings, is(new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.END)));
    }

    public void testOf_UsesRequestTaskSettings_AndRequestInputType() {
        var taskSettings = new CohereEmbeddingsTaskSettings(InputType.SEARCH, CohereTruncation.NONE);
        var overriddenTaskSettings = CohereEmbeddingsTaskSettings.of(
            taskSettings,
            new CohereEmbeddingsTaskSettings(null, CohereTruncation.END),
            InputType.INGEST
        );

        MatcherAssert.assertThat(overriddenTaskSettings, is(new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.END)));
    }

    @Override
    protected Writeable.Reader<CohereEmbeddingsTaskSettings> instanceReader() {
        return CohereEmbeddingsTaskSettings::new;
    }

    @Override
    protected CohereEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereEmbeddingsTaskSettings mutateInstance(CohereEmbeddingsTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, CohereEmbeddingsTaskSettingsTests::createRandom);
    }

    public static Map<String, Object> getTaskSettingsMapEmpty() {
        return new HashMap<>();
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable InputType inputType, @Nullable CohereTruncation truncation) {
        var map = new HashMap<String, Object>();

        if (inputType != null) {
            map.put(CohereEmbeddingsTaskSettings.INPUT_TYPE, inputType.toString());
        }

        if (truncation != null) {
            map.put(CohereServiceFields.TRUNCATE, truncation.toString());
        }

        return map;
    }
}
