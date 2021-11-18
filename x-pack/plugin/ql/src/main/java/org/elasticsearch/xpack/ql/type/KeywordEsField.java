/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.type;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;

/**
 * SQL-related information about an index field with keyword type
 */
public class KeywordEsField extends EsField {

    private final int precision;
    private final boolean normalized;

    public KeywordEsField(String name) {
        this(name, Collections.emptyMap(), true, Short.MAX_VALUE, false);
    }

    public KeywordEsField(String name, Map<String, EsField> properties, boolean hasDocValues, int precision, boolean normalized) {
        this(name, properties, hasDocValues, precision, normalized, false);
    }

    public KeywordEsField(
        String name,
        Map<String, EsField> properties,
        boolean hasDocValues,
        int precision,
        boolean normalized,
        boolean isAlias
    ) {
        this(name, KEYWORD, properties, hasDocValues, precision, normalized, isAlias);
    }

    protected KeywordEsField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean hasDocValues,
        int precision,
        boolean normalized,
        boolean isAlias
    ) {
        super(name, esDataType, properties, hasDocValues, isAlias);
        this.precision = precision;
        this.normalized = normalized;
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public Exact getExactInfo() {
        return new Exact(normalized == false, "Normalized keyword field cannot be used for exact match operations");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        KeywordEsField that = (KeywordEsField) o;
        return precision == that.precision && normalized == that.normalized;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, normalized);
    }
}
