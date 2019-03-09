/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;

/**
 * Base class holding common properties for Elasticsearch aggregations.
 */
public abstract class Agg {

    private final String id;
    private final String fieldName;

    Agg(String id, String fieldName) {
        this.id = id;
        this.fieldName = fieldName;
    }

    public String id() {
        return id;
    }

    protected String fieldName() {
        return fieldName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, fieldName);
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
                && Objects.equals(fieldName, other.fieldName);
    }

    @Override
    public String toString() {
        return format(Locale.ROOT, "%s(%s)", getClass().getSimpleName(), fieldName);
    }
}