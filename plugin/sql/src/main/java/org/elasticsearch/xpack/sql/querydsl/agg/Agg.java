/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import java.util.Locale;
import java.util.Objects;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.xpack.sql.util.StringUtils;

import static java.lang.String.format;

public abstract class Agg {
    private final String id;
    private final String fieldName;
    private final String propertyPath;
    private final String asParentPath;

    Agg(String id, String propertyPath, String fieldName) {
        this.id = id;
        this.propertyPath = propertyPath;
        int index = propertyPath.lastIndexOf(".");
        this.asParentPath = index > 0 ? propertyPath.substring(0, index) : StringUtils.EMPTY;
        this.fieldName = fieldName;
    }

    public String id() {
        return id;
    }

    public String propertyPath() {
        return propertyPath;
    }

    public String asParentPath() {
        return asParentPath;
    }

    public String fieldName() {
        return fieldName;
    }

    abstract AggregationBuilder toBuilder();

    @Override
    public int hashCode() {
        return Objects.hash(id, propertyPath);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Agg other = (Agg) obj;
        return Objects.equals(id, other.id) 
                && Objects.equals(fieldName, other.fieldName)
                && Objects.equals(propertyPath, other.propertyPath);
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "%s(%s)#%s", getClass().getSimpleName(), fieldName, propertyPath);
    }
}