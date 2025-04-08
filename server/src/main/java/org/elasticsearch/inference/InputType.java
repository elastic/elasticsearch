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

import java.util.Locale;

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
}
