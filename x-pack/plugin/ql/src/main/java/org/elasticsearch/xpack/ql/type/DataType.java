/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import java.util.Locale;
import java.util.Objects;

public class DataType {

    private final String typeName;

    private final String name;

    private final String esType;

    private final int size;

    /**
     * True if the type represents an integer number
     */
    private final boolean isInteger;

    /**
     * True if the type represents a rational number
     */
    private final boolean isRational;

    /**
     * True if the type supports doc values by default
     */
    private final boolean docValues;

    /**
     * The base type of an array type.
     */
    private final DataType arrayBaseType;

    public DataType(String esName, int size, boolean isInteger, boolean isRational, boolean hasDocValues) {
        this(null, esName, size, isInteger, isRational, hasDocValues);
    }

    public DataType(String typeName, String esType, int size, boolean isInteger, boolean isRational, boolean hasDocValues) {
        this(typeName, esType, size, isInteger, isRational, hasDocValues, null);
    }

    private DataType(String typeName, String esType, int size, boolean isInteger, boolean isRational, boolean hasDocValues,
                    DataType arrayBaseType) {
        String typeString = typeName != null ? typeName : esType;
        this.typeName = typeString.toLowerCase(Locale.ROOT);
        this.name = typeString.toUpperCase(Locale.ROOT);
        this.esType = esType;
        this.size = size;
        this.isInteger = isInteger;
        this.isRational = isRational;
        this.docValues = hasDocValues;
        this.arrayBaseType = arrayBaseType;
    }

    private DataType(DataType baseType) {
        this(baseType.name + "_ARRAY", baseType.esType, baseType.size, baseType.isInteger, baseType.isRational, baseType.docValues,
            baseType);
    }

    public String name() {
        return name;
    }

    public String typeName() {
        return typeName;
    }

    public String esType() {
        return esType;
    }

    public boolean isInteger() {
        return isInteger;
    }

    public boolean isRational() {
        return isRational;
    }

    public boolean isNumeric() {
        return isInteger || isRational;
    }

    public int size() {
        return size;
    }

    public boolean hasDocValues() {
        return docValues;
    }

    public boolean isArray() {
        return arrayBaseType != null;
    }

    public DataType arrayBaseType() {
        return arrayBaseType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, esType, size, isInteger, isRational, docValues, arrayBaseType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DataType other = (DataType) obj;
        return Objects.equals(typeName, other.typeName)
                && Objects.equals(esType, other.esType)
                && size == other.size
                && isInteger == other.isInteger
                && isRational == other.isRational
                && docValues == other.docValues
                && arrayBaseType == other.arrayBaseType;
    }

    @Override
    public String toString() {
        return name;
    }

    public static DataType arrayOf(DataType baseType) {
        // no multidimensional arrays supported
        if (baseType.isArray()) {
            throw new QlIllegalArgumentException("the base type of an array type cannot be itself an array type; provided: " + baseType);
        }
        return new DataType(baseType);
    }
}
