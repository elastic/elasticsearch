/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Objects;

/**
 * Allows checking a field's cardinality against given lower and upper bounds
 */
public class FieldCardinalityConstraint {

    private final String field;
    private final long lowerBound;
    private final long upperBound;

    public static FieldCardinalityConstraint between(String field, long lowerBound, long upperBound) {
        return new FieldCardinalityConstraint(field, lowerBound, upperBound);
    }

    private FieldCardinalityConstraint(String field, long lowerBound, long upperBound) {
        this.field = Objects.requireNonNull(field);
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public String getField() {
        return field;
    }

    public long getLowerBound() {
        return lowerBound;
    }

    public long getUpperBound() {
        return upperBound;
    }

    public void check(long fieldCardinality) {
        if (fieldCardinality < lowerBound) {
            throw ExceptionsHelper.badRequestException(
                                    "Field [{}] must have at least [{}] distinct values but there were [{}]",
                                    field, lowerBound, fieldCardinality);
        }
        if (fieldCardinality > upperBound) {
            throw ExceptionsHelper.badRequestException(
                "Field [{}] must have at most [{}] distinct values but there were at least [{}]",
                field, upperBound, fieldCardinality);
        }
    }
}
