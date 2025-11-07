/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class represents a String which may be raw text, or the String representation of some other data such as an image in base64
 */
public record InferenceString(String value, DataType dataType) {
    /**
     * Describes the type of data represented by an {@link InferenceString}
     */
    public enum DataType {
        TEXT,
        IMAGE_BASE64
    }

    private static final EnumSet<DataType> IMAGE_TYPES = EnumSet.of(DataType.IMAGE_BASE64);

    /**
     * Constructs an {@link InferenceString} with the given value and {@link DataType}
     * @param value the String value
     * @param dataType the type of data that the String represents
     */
    public InferenceString(String value, DataType dataType) {
        this.value = Objects.requireNonNull(value);
        this.dataType = Objects.requireNonNull(dataType);
    }

    public boolean isImage() {
        return IMAGE_TYPES.contains(dataType);
    }

    public boolean isText() {
        return DataType.TEXT.equals(dataType);
    }

    public static List<String> toStringList(List<InferenceString> inferenceStrings) {
        return inferenceStrings.stream().map(InferenceString::value).collect(Collectors.toList());
    }

}
