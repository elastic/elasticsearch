/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.util.Objects;

public class ArrayType implements DataType {

    private final DateType type;
    private final int dimension;

    public ArrayType(DateType type, int dimension) {
        this.type = type;
        this.dimension = dimension;
    }

    public DateType type() {
        return type;
    }

    public int dimension() {
        return dimension;
    }

    @Override
    public String esName() {
        return "array";
    }

    @Override
    public JDBCType sqlType() {
        return JDBCType.ARRAY;
    }

    @Override
    public int precision() {
        return type.precision();
    }

    @Override
    public boolean isInteger() {
        return false;
    }

    @Override
    public boolean isRational() {
        return false;
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public boolean hasDocValues() {
        return type.hasDocValues();
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, dimension);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ArrayType other = (ArrayType) obj;
        return Objects.equals(dimension, other.dimension) && Objects.equals(type, other.type);
    }
}