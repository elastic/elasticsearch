/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.type;

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
    private final boolean isAlias;

    public EsField(String name, DataType esDataType, Map<String, EsField> properties, boolean aggregatable) {
        this(name, esDataType, properties, aggregatable, false);
    }
    
    public EsField(String name, DataType esDataType, Map<String, EsField> properties, boolean aggregatable, boolean isAlias) {
        this.name = name;
        this.esDataType = esDataType;
        this.aggregatable = aggregatable;
        this.properties = properties;
        this.isAlias = isAlias;
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
     * This field is an alias to another field
     */
    public boolean isAlias() {
        return isAlias;
    }

    /**
     * Returns the path to the keyword version of this field if this field is text and it has a subfield that is
     * indexed as keyword, throws an exception if such field is not found or the field name itself in all other cases.
     * To avoid the exception {@link EsField#getExactInfo()} should be used beforehand, to check if an exact field exists
     * and if not get the errorMessage which explains why is that.
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
     * Returns and {@link Exact} object with all the necessary info about the field:
     * <ul>
     *  <li>If it has an exact underlying field or not</li>
     *  <li>and if not an error message why it doesn't</li>
     * </ul>
     */
    public Exact getExactInfo() {
        return Exact.EXACT_FIELD;
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
        return aggregatable == field.aggregatable && isAlias == field.isAlias && esDataType == field.esDataType
                && Objects.equals(name, field.name)
                && Objects.equals(properties, field.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(esDataType, aggregatable, properties, name, isAlias);
    }

    public static final class Exact {

        private static Exact EXACT_FIELD = new Exact(true, null);

        private boolean hasExact;
        private String errorMsg;

        public Exact(boolean hasExact, String errorMsg) {
            this.hasExact = hasExact;
            this.errorMsg = errorMsg;
        }

        public boolean hasExact() {
            return hasExact;
        }

        public String errorMsg() {
            return errorMsg;
        }
    }
}
