/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.CoreMatchers.is;

public class InputTypeTests extends ESTestCase {
    public static InputType randomWithoutUnspecified() {
        return randomFrom(
            InputType.INGEST,
            InputType.SEARCH,
            InputType.CLUSTERING,
            InputType.CLASSIFICATION,
            InputType.INTERNAL_SEARCH,
            InputType.INTERNAL_INGEST
        );
    }

    public static InputType randomWithNull() {
        return randomBoolean()
            ? null
            : randomFrom(
                InputType.UNSPECIFIED,
                InputType.INGEST,
                InputType.SEARCH,
                InputType.CLUSTERING,
                InputType.CLASSIFICATION,
                InputType.INTERNAL_SEARCH,
                InputType.INTERNAL_INGEST
            );
    }

    public static InputType randomSearchAndIngestWithNull() {
        return randomBoolean()
            ? null
            : randomFrom(InputType.UNSPECIFIED, InputType.INGEST, InputType.SEARCH, InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST);
    }

    public static InputType randomSearchAndIngestWithNullWithoutUnspecified() {
        return randomBoolean()
            ? null
            : randomFrom(InputType.INGEST, InputType.SEARCH, InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST);
    }

    public static InputType randomWithIngestAndSearch() {
        return randomFrom(InputType.INGEST, InputType.SEARCH, InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST);
    }

    public static InputType randomWithInternalAndUnspecified() {
        return randomFrom(InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST, InputType.UNSPECIFIED);
    }

    public void testFromRestString_ValidInputType() {
        for (String internal : List.of("search", "ingest", "classification", "clustering", "unspecified")) {
            assertEquals(InputType.fromRestString(internal), InputType.fromString(internal));
        }
    }

    public void testFromRestString_ThrowsErrorForInternalInputTypes() {
        for (String internal : List.of("internal_ingest", "internal_search")) {
            var thrownException = expectThrows(IllegalArgumentException.class, () -> InputType.fromRestString(internal));

            assertThat(thrownException.getMessage(), is(format("Unrecognized input_type [%s]", internal)));
        }
    }

    public void testFromRestString_ThrowsErrorForInvalidInputTypes() {
        var thrownException = expectThrows(IllegalArgumentException.class, () -> InputType.fromRestString("foo"));

        assertThat(thrownException.getMessage(), is("No enum constant org.elasticsearch.inference.InputType.FOO"));
    }

    public void testValidateInputTypeTranslationValues() {
        assertThat(
            InputType.validateInputTypeTranslationValues(
                Map.of(
                    InputType.INGEST.toString(),
                    "ingest_value",
                    InputType.SEARCH.toString(),
                    "search_value",
                    InputType.CLASSIFICATION.toString(),
                    "classification_value",
                    InputType.CLUSTERING.toString(),
                    "clustering_value"
                ),
                new ValidationException()
            ),
            is(
                Map.of(
                    InputType.INGEST,
                    "ingest_value",
                    InputType.SEARCH,
                    "search_value",
                    InputType.CLASSIFICATION,
                    "classification_value",
                    InputType.CLUSTERING,
                    "clustering_value"
                )
            )
        );
    }

    public void testValidateInputTypeTranslationValues_ReturnsEmptyMap_WhenTranslationIsNull() {
        assertThat(InputType.validateInputTypeTranslationValues(null, new ValidationException()), is(Map.of()));
    }

    public void testValidateInputTypeTranslationValues_ReturnsEmptyMap_WhenTranslationIsAnEmptyMap() {
        assertThat(InputType.validateInputTypeTranslationValues(Map.of(), new ValidationException()), is(Map.of()));
    }

    public void testValidateInputTypeTranslationValues_ThrowsAnException_WhenInputTypeIsUnspecified() {
        var exception = expectThrows(
            ValidationException.class,
            () -> InputType.validateInputTypeTranslationValues(
                Map.of(InputType.INGEST.toString(), "ingest_value", InputType.UNSPECIFIED.toString(), "unspecified_value"),
                new ValidationException()
            )
        );

        assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: Invalid input type translation for key: [unspecified], is not a valid value. Must be "
                    + "one of [ingest, search, classification, clustering];"
            )
        );
    }

    public void testValidateInputTypeTranslationValues_ThrowsAnException_WhenInputTypeIsInternal() {
        var exception = expectThrows(
            ValidationException.class,
            () -> InputType.validateInputTypeTranslationValues(
                Map.of(InputType.INGEST.toString(), "ingest_value", InputType.INTERNAL_INGEST.toString(), "internal_ingest_value"),
                new ValidationException()
            )
        );

        assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: Invalid input type translation for key: [internal_ingest], is not a valid value. Must be "
                    + "one of [ingest, search, classification, clustering];"
            )
        );
    }

    public void testValidateInputTypeTranslationValues_ThrowsAnException_WhenValueIsNull() {
        var translation = new HashMap<String, Object>();
        translation.put(InputType.INGEST.toString(), null);

        var exception = expectThrows(
            ValidationException.class,
            () -> InputType.validateInputTypeTranslationValues(translation, new ValidationException())
        );

        assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: Input type translation value for key [ingest] must be a String that "
                    + "is not null and not empty, received: [null], type: [null].;"
            )
        );
    }

    public void testValidateInputTypeTranslationValues_ThrowsAnException_WhenValueIsAnEmptyString() {
        var translation = new HashMap<String, Object>();
        translation.put(InputType.INGEST.toString(), "");

        var exception = expectThrows(
            ValidationException.class,
            () -> InputType.validateInputTypeTranslationValues(translation, new ValidationException())
        );

        assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: Input type translation value for key [ingest] must be a String that "
                    + "is not null and not empty, received: [], type: [String].;"
            )
        );
    }

    public void testValidateInputTypeTranslationValues_ThrowsAnException_WhenValueIsNotAString() {
        var translation = new HashMap<String, Object>();
        translation.put(InputType.INGEST.toString(), 1);

        var exception = expectThrows(
            ValidationException.class,
            () -> InputType.validateInputTypeTranslationValues(translation, new ValidationException())
        );

        assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: Input type translation value for key [ingest] must be a String that "
                    + "is not null and not empty, received: [1], type: [Integer].;"
            )
        );
    }
}
