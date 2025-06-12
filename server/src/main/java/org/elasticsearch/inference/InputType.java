/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

/**
 * Defines the type of request, whether the request is to ingest a document or search for a document.
 */
public enum InputType {
    INGEST,
    SEARCH,
    UNSPECIFIED,
    CLASSIFICATION,
    CLUSTERING,

    // Use the following enums when calling the inference API internally
    INTERNAL_SEARCH,
    INTERNAL_INGEST;

    private static final EnumSet<InputType> SUPPORTED_REQUEST_VALUES = EnumSet.of(
        InputType.CLASSIFICATION,
        InputType.CLUSTERING,
        InputType.INGEST,
        InputType.SEARCH
    );

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static InputType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static InputType fromRestString(String name) {
        var inputType = InputType.fromString(name);
        if (inputType == InputType.INTERNAL_INGEST || inputType == InputType.INTERNAL_SEARCH) {
            throw new IllegalArgumentException(format("Unrecognized input_type [%s]", inputType));
        }
        return inputType;
    }

    public static boolean isInternalTypeOrUnspecified(InputType inputType) {
        return inputType == InputType.INTERNAL_INGEST || inputType == InputType.INTERNAL_SEARCH || inputType == InputType.UNSPECIFIED;
    }

    public static boolean isSpecified(InputType inputType) {
        return inputType != null && inputType != InputType.UNSPECIFIED;
    }

    public static String invalidInputTypeMessage(InputType inputType) {
        return Strings.format("received invalid input type value [%s]", inputType.toString());
    }

    /**
     * Ensures that a map used for translating input types is valid. The keys of the map are the external representation,
     * and the values correspond to the values in this class.
     * Throws a {@link ValidationException} if any value is not a valid InputType.
     *
     * @param inputTypeTranslation the map of input type translations to validate
     * @param validationException  a ValidationException to which errors will be added
     */
    public static Map<InputType, String> validateInputTypeTranslationValues(
        Map<String, Object> inputTypeTranslation,
        ValidationException validationException
    ) {
        if (inputTypeTranslation == null || inputTypeTranslation.isEmpty()) {
            return Map.of();
        }

        var translationMap = new HashMap<InputType, String>();

        for (var entry : inputTypeTranslation.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();

            if (value instanceof String == false || Strings.isNullOrEmpty((String) value)) {
                validationException.addValidationError(
                    Strings.format(
                        "Input type translation value for key [%s] must be a String that is not null and not empty, received: [%s].",
                        key,
                        value.getClass().getSimpleName()
                    )
                );

                throw validationException;
            }

            try {
                var inputTypeKey = InputType.fromRestString(key);
                translationMap.put(inputTypeKey, (String) value);
            } catch (Exception e) {
                validationException.addValidationError(
                    Strings.format(
                        "Invalid input type translation for key: [%s], is not a valid value. Must be one of %s",
                        key,
                        EnumSet.of(InputType.CLASSIFICATION, InputType.CLUSTERING, InputType.INGEST, InputType.SEARCH)
                    )
                );

                throw validationException;
            }
        }

        return translationMap;
    }
}
