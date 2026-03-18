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
import java.util.EnumSet;
import java.util.Locale;

/**
 * Describes the type of data to perform inference on
 */
public enum DataType {
    TEXT(DataFormat.TEXT, EnumSet.of(DataFormat.TEXT)),
    IMAGE(DataFormat.BASE64, EnumSet.of(DataFormat.BASE64));

    private final DataFormat defaultFormat;
    private final EnumSet<DataFormat> supportedFormats;

    DataType(DataFormat defaultFormat, EnumSet<DataFormat> supportedFormats) {
        this.defaultFormat = defaultFormat;
        this.supportedFormats = supportedFormats;
    }

    public DataFormat getDefaultFormat() {
        return defaultFormat;
    }

    public EnumSet<DataFormat> getSupportedFormats() {
        return supportedFormats;
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
