/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

public enum ModelFieldType {

    SCALAR,
    CATEGORICAL,
    VECTOR,
    TEXT;

    private static final Pattern IS_NUMBER = Pattern.compile("-?\\d+(\\.\\d+)?");
    public static ModelFieldType fromString(String value) {
        return ModelFieldType.valueOf(value.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public boolean supportsJavaObjectType(Object obj) {
        if (obj == null) {
            return false;
        }
        switch (this) {
            case VECTOR:
                return obj instanceof List || obj instanceof Object[];
            case SCALAR:
                if (obj instanceof Number) {
                    return true;
                }
                return obj instanceof String && IS_NUMBER.matcher((String)obj).matches();
            case CATEGORICAL:
                return obj instanceof String || obj instanceof Number;
            case TEXT:
                return obj instanceof String;
            default:
                return false;
        }
    }
}
