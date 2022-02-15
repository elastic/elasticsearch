/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.type;

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

    public DataType(String esName, int size, boolean isInteger, boolean isRational, boolean hasDocValues) {
        this(null, esName, size, isInteger, isRational, hasDocValues);
    }

    public DataType(String typeName, String esType, int size, boolean isInteger, boolean isRational, boolean hasDocValues) {
        String typeString = typeName != null ? typeName : esType;
        this.typeName = typeString.toLowerCase(Locale.ROOT);
        this.name = typeString.toUpperCase(Locale.ROOT);
        this.esType = esType;
        this.size = size;
        this.isInteger = isInteger;
        this.isRational = isRational;
        this.docValues = hasDocValues;
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

    @Override
    public int hashCode() {
        return Objects.hash(typeName, esType, size, isInteger, isRational, docValues);
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
            && docValues == other.docValues;
    }

    @Override
    public String toString() {
        return name;
    }
}
