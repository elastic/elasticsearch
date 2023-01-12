/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.NESTED;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;

public final class DataTypes {

    private static final Collection<DataType> TYPES = Arrays.asList(UNSUPPORTED, NULL, INTEGER, LONG, DOUBLE, FLOAT, KEYWORD)
        .stream()
        .sorted(Comparator.comparing(DataType::typeName))
        .toList();

    private static final Map<String, DataType> NAME_TO_TYPE = TYPES.stream().collect(toUnmodifiableMap(DataType::typeName, t -> t));

    private static Map<String, DataType> ES_TO_TYPE;

    static {
        Map<String, DataType> map = TYPES.stream().filter(e -> e.esType() != null).collect(toMap(DataType::esType, t -> t));
        ES_TO_TYPE = Collections.unmodifiableMap(map);
    }

    private DataTypes() {}

    public static Collection<DataType> types() {
        return TYPES;
    }

    public static DataType fromEs(String name) {
        DataType type = ES_TO_TYPE.get(name);
        return type != null ? type : UNSUPPORTED;
    }

    public static DataType fromJava(Object value) {
        if (value == null) {
            return NULL;
        }
        if (value instanceof Integer) {
            return INTEGER;
        }
        if (value instanceof Long) {
            return LONG;
        }
        if (value instanceof Double) {
            return DOUBLE;
        }
        if (value instanceof Float) {
            return FLOAT;
        }
        if (value instanceof String || value instanceof Character) {
            return KEYWORD;
        }

        return null;
    }

    public static boolean isUnsupported(DataType from) {
        return from == UNSUPPORTED || from == NESTED || from == OBJECT;
    }

    public static boolean isString(DataType t) {
        return t == KEYWORD;
    }

    public static boolean isPrimitive(DataType t) {
        return t != OBJECT && t != NESTED && t != UNSUPPORTED;
    }

    public static boolean areCompatible(DataType left, DataType right) {
        if (left == right) {
            return true;
        } else {
            return (left == NULL || right == NULL) || (isString(left) && isString(right)) || (left.isNumeric() && right.isNumeric());
        }
    }
}
