/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;

public interface DataType {

    String esName();

    default String sqlName() {
        return sqlType().name();
    }

    JDBCType sqlType();
    
    boolean hasDocValues();

    default Object defaultValue() {
        return null;
    }

    default int size() {
        return JdbcUtils.size(sqlType());
    }

    default int precision() {
        return JdbcUtils.precision(sqlType());
    }

    default int scale() {
        return JdbcUtils.scale(sqlType());
    }

    default int displaySize() {
        return JdbcUtils.displaySize(sqlType());
    }

    default boolean isSigned() {
        return JdbcUtils.isSigned(sqlType());
    }

    default boolean isInteger() {
        return JdbcUtils.isInteger(sqlType());
    }

    default boolean isRational() {
        return JdbcUtils.isRational(sqlType());
    }

    default boolean isNumeric() {
        return isInteger() || isRational();
    }

    default boolean isComplex() {
        return !isPrimitive();
    }

    boolean isPrimitive();

    default boolean same(DataType other) {
        return getClass() == other.getClass();
    }
}
