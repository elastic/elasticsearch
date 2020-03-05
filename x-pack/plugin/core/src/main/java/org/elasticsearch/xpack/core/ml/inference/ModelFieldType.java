/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import java.util.List;
import java.util.Locale;

public enum ModelFieldType {

    SCALAR,
    CATEGORICAL,
    VECTOR,
    TEXT;

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
                return obj instanceof String && isDouble((String)obj);
            case CATEGORICAL:
                return obj instanceof String || obj instanceof Number;
            case TEXT:
                return obj instanceof String;
            default:
                return false;
        }
    }

    private static boolean isDouble(final String value) {
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }
}
