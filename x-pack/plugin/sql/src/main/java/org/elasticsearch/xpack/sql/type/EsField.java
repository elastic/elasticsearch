/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.common.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * SQL-related information about an index field
 */
public class EsField {
    private final DataType esDataType;
    private final boolean aggregatable;
    private final Map<String, EsField> properties;
    private final String name;

    public EsField(String name, DataType esDataType, Map<String, EsField> properties, boolean aggregatable) {
        this.name = name;
        this.esDataType = esDataType;
        this.aggregatable = aggregatable;
        this.properties = properties;
    }

    /**
     * Returns the field path
     */
    public String getName() {
        return name;
    }

    /**
     * The field type
     */
    public DataType getDataType() {
        return esDataType;
    }

    /**
     * This field can be aggregated
     */
    public boolean isAggregatable() {
        return aggregatable;
    }

    /**
     * Returns list of properties for the nested and object fields, list of subfield if the field
     * was indexed in a few different ways or null otherwise
     */
    @Nullable
    public Map<String, EsField> getProperties() {
        return properties;
    }

    /**
     * Returns the path to the keyword version of this field if this field is text and it has a subfield that is
     * indexed as keyword, null if such field is not found or the field name itself in all other cases
     */
    public EsField getExactField() {
        return this;
    }

    /**
     * Returns the precision of the field
     * <p>
     * Precision is the specified column size. For numeric data, this is the maximum precision. For character
     * data, this is the length in characters. For datetime datatypes, this is the length in characters of the
     * String representation (assuming the maximum allowed defaultPrecision of the fractional seconds component).
     */
    public int getPrecision() {
        return esDataType.defaultPrecision;
    }

    /**
     * True if this field name can be used in sorting, aggregations and term queries as is
     * <p>
     * This will be true for most fields except analyzed text fields that cannot be used directly and should be
     * replaced with the field returned by {@link EsField#getExactField()} instead.
     */
    public boolean isExact() {
        return true;
    }

    @Override
    public String toString() {
        return name + "@" + esDataType.name() + "=" + properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EsField field = (EsField) o;
        return aggregatable == field.aggregatable && esDataType == field.esDataType 
                && Objects.equals(name, field.name)
                && Objects.equals(properties, field.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(esDataType, aggregatable, properties, name);
    }
}