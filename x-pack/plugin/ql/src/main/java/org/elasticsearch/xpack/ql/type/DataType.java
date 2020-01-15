/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.type;

import java.util.Locale;

public class DataType {

    private final String typeName;

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

    public DataType(int size, boolean isInteger, boolean isRational, boolean hasDocValues) {
        this(null, size, isInteger, isRational, hasDocValues);
    }

    public DataType(String esType, int size, boolean isInteger, boolean isRational, boolean hasDocValues) {
        this.typeName = getClass().getSimpleName().toLowerCase(Locale.ROOT);
        this.esType = esType;
        this.size = size;
        this.isInteger = isInteger;
        this.isRational = isRational;
        this.docValues = hasDocValues;
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
}