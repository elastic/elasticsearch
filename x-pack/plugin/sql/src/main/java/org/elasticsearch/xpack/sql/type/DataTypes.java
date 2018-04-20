/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.joda.time.DateTime;

public abstract class DataTypes {

    public static boolean isNull(DataType from) {
        return from == DataType.NULL;
    }

    public static boolean isUnsupported(DataType from) {
        return from == DataType.UNSUPPORTED;
    }

    public static DataType fromJava(Object value) {
        if (value == null) {
            return DataType.NULL;
        }
        if (value instanceof Integer) {
            return DataType.INTEGER;
        }
        if (value instanceof Long) {
            return DataType.LONG;
        }
        if (value instanceof Boolean) {
            return DataType.BOOLEAN;
        }
        if (value instanceof Double) {
            return DataType.DOUBLE;
        }
        if (value instanceof Float) {
            return DataType.FLOAT;
        }
        if (value instanceof Byte) {
            return DataType.BYTE;
        }
        if (value instanceof Short) {
            return DataType.SHORT;
        }
        if (value instanceof DateTime) {
            return DataType.DATE;
        }
        if (value instanceof String || value instanceof Character) {
            return DataType.KEYWORD;
        }
        throw new SqlIllegalArgumentException("No idea what's the DataType for {}", value.getClass());
    }
}
