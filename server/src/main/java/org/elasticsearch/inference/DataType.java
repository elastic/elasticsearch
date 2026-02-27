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

import java.util.Arrays;
import java.util.Locale;

/**
 * Describes the type of data to perform inference on
 */
public enum DataType {
    TEXT(InferenceString.DataFormat.TEXT),
    IMAGE(InferenceString.DataFormat.BASE64);

    private final InferenceString.DataFormat defaultFormat;

    DataType(InferenceString.DataFormat defaultFormat) {
        this.defaultFormat = defaultFormat;
    }

    public InferenceString.DataFormat getDefaultFormat() {
        return defaultFormat;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static DataType fromString(String name) {
        try {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(
                Strings.format("Unrecognized type [%s], must be one of %s", name, Arrays.toString(DataType.values()))
            );
        }
    }
}
