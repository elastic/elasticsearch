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

import static java.lang.String.format;
import static java.sql.Types.*;

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

    public static String nameOf(int jdbcType) {
        return JDBCType.valueOf(jdbcType).getName();
    }

    // see javax.sql.rowset.RowSetMetaDataImpl
    // and https://db.apache.org/derby/docs/10.5/ref/rrefjdbc20377.html
    public static Class<?> classOf(int jdbcType) {

        switch (jdbcType) {
            case NUMERIC:
            case DECIMAL:
                return BigDecimal.class;
            case BOOLEAN:
            case BIT:
                return Boolean.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case REAL:
                return Float.class;
            case FLOAT:
            case DOUBLE:
                return Double.class;
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
                return byte[].class;
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
                return String.class;
            case DATE:
                return Date.class;
            case TIME:
                return Time.class;
            case TIMESTAMP:
                return Timestamp.class;
            case BLOB:
                return Blob.class;
            case CLOB:
                return Clob.class;
            case TIMESTAMP_WITH_TIMEZONE:
                return Long.class;
            default:
                throw new IllegalArgumentException(format("Unsupported JDBC type %d/%s", jdbcType, nameOf(jdbcType)));
        }
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