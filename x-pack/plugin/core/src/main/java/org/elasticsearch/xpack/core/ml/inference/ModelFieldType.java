/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

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

}
