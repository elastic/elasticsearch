/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.jdbc;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NULL;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

public abstract class JdbcUtils {

    public static Class<?> asPrimitive(Class<?> wrapperClass) {
        if (Boolean.class == wrapperClass) {
            return boolean.class;
        }
        if (Byte.class == wrapperClass) {
            return byte.class;
        }
        if (Short.class == wrapperClass) {
            return short.class;
        }
        if (Character.class == wrapperClass) {
            return char.class;
        }
        if (Integer.class == wrapperClass) {
            return int.class;
        }
        if (Long.class == wrapperClass) {
            return long.class;
        }
        if (Double.class == wrapperClass) {
            return double.class;
        }
        if (Float.class == wrapperClass) {
            return float.class;
        }
        if (Void.class == wrapperClass) {
            return void.class;
        }

        return wrapperClass;
    }

    public static int fromClass(Class<?> clazz) {
        if (clazz == null) {
            return NULL;
        }
        if (clazz == String.class) {
            return VARCHAR;
        }
        if (clazz == Boolean.class || clazz == boolean.class) {
            return BOOLEAN;
        }
        if (clazz == Byte.class || clazz == byte.class) {
            return TINYINT;
        }
        if (clazz == Short.class || clazz == short.class) {
            return SMALLINT;
        }
        if (clazz == Integer.class || clazz == int.class) {
            return INTEGER;
        }
        if (clazz == Long.class || clazz == long.class) {
            return BIGINT;
        }
        if (clazz == Float.class || clazz == float.class) {
            return REAL;
        }
        if (clazz == Double.class || clazz == double.class) {
            return DOUBLE;
        }
        if (clazz == Void.class || clazz == void.class) {
            return NULL;
        }
        if (clazz == byte[].class) {
            return VARBINARY;
        }
        if (clazz == Date.class) {
            return DATE;
        }
        if (clazz == Time.class) {
            return TIME;
        }
        if (clazz == Timestamp.class) {
            return TIMESTAMP;
        }
        if (clazz == Blob.class) {
            return BLOB;
        }
        if (clazz == Clob.class) {
            return CLOB;
        }
        if (clazz == BigDecimal.class) {
            return DECIMAL;
        }

        throw new JdbcException("Unrecognized class %s", clazz);
    }

    static boolean isSigned(int type) {
        switch (type) {
            case BIGINT:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case INTEGER:
            case SMALLINT:
            case REAL:
            case NUMERIC:
                return true;
            default:
                return false;
        }
    }

    static JDBCType type(int jdbcType) {
        return JDBCType.valueOf(jdbcType);
    }

    static String typeName(int jdbcType) {
        return type(jdbcType).getName();
    }
}