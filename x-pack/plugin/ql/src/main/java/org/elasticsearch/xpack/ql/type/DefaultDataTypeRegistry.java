/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.xpack.ql.expression.literal.Interval;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.elasticsearch.xpack.ql.type.DataTypes.BINARY;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.BYTE;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.HALF_FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.NESTED;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataTypes.SCALED_FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.SHORT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;

public class DefaultDataTypeRegistry implements DataTypeRegistry {

    private static final Collection<DataType> TYPES = Arrays.asList(
            UNSUPPORTED,
            NULL,
            BOOLEAN,
            BYTE,
            SHORT,
            INTEGER,
            LONG,
            DOUBLE,
            FLOAT,
            HALF_FLOAT,
            SCALED_FLOAT,
            KEYWORD,
            TEXT,
            DATETIME,
            IP,
            BINARY,
            OBJECT,
            NESTED)
            .stream()
            .sorted(Comparator.comparing(DataType::typeName))
            .collect(toUnmodifiableList());
    
    @Override
    public Collection<DataType> dataTypes() {
        return TYPES;
    }

    @Override
    public DataType fromJava(Object value) {
        if (value == null) {
            return NULL;
        }
        if (value instanceof Integer) {
            return INTEGER;
        }
        if (value instanceof Long) {
            return LONG;
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof Double) {
            return DOUBLE;
        }
        if (value instanceof Float) {
            return FLOAT;
        }
        if (value instanceof Byte) {
            return BYTE;
        }
        if (value instanceof Short) {
            return SHORT;
        }
        if (value instanceof ZonedDateTime) {
            return DATETIME;
        }
        if (value instanceof String || value instanceof Character) {
            return KEYWORD;
        }
        if (value instanceof Interval) {
            return ((Interval<?>) value).dataType();
        }

        return null;
    }

    @Override
    public boolean isUnsupported(DataType type) {
        return false;
    }

    @Override
    public boolean canConvert(DataType from, DataType to) {
        return false;
    }

    @Override
    public Object convert(Object value, DataType type) {
        return null;
    }

    @Override
    public DataType commonType(DataType left, DataType right) {
        return null;
    }
}
