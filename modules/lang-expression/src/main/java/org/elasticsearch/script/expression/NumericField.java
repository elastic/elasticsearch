/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.search.DoubleValuesSource;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.MultiValueMode;

/**
 * Expressions API for numeric fields.
 */
final class NumericField {
    // no instance
    private NumericField() {}

    // supported variables
    static final String VALUE_VARIABLE = "value";
    static final String EMPTY_VARIABLE = "empty";
    static final String LENGTH_VARIABLE = "length";

    // supported methods
    static final String GETVALUE_METHOD = "getValue";
    static final String ISEMPTY_METHOD = "isEmpty";
    static final String SIZE_METHOD = "size";
    static final String MINIMUM_METHOD = "min";
    static final String MAXIMUM_METHOD = "max";
    static final String AVERAGE_METHOD = "avg";
    static final String MEDIAN_METHOD = "median";
    static final String SUM_METHOD = "sum";
    static final String COUNT_METHOD = "count";

    static DoubleValuesSource getVariable(IndexFieldData<?> fieldData, String fieldName, String variable) {
        return switch (variable) {
            case VALUE_VARIABLE -> new FieldDataValueSource(fieldData, MultiValueMode.MIN);
            case EMPTY_VARIABLE -> new EmptyMemberValueSource(fieldData);
            case LENGTH_VARIABLE -> new CountMethodValueSource(fieldData);
            default -> throw new IllegalArgumentException(
                "Member variable [" + variable + "] does not exist for " + "numeric field [" + fieldName + "]."
            );
        };
    }

    static DoubleValuesSource getMethod(IndexFieldData<?> fieldData, String fieldName, String method) {
        return switch (method) {
            case GETVALUE_METHOD -> new FieldDataValueSource(fieldData, MultiValueMode.MIN);
            case ISEMPTY_METHOD -> new EmptyMemberValueSource(fieldData);
            case SIZE_METHOD -> new CountMethodValueSource(fieldData);
            case MINIMUM_METHOD -> new FieldDataValueSource(fieldData, MultiValueMode.MIN);
            case MAXIMUM_METHOD -> new FieldDataValueSource(fieldData, MultiValueMode.MAX);
            case AVERAGE_METHOD -> new FieldDataValueSource(fieldData, MultiValueMode.AVG);
            case MEDIAN_METHOD -> new FieldDataValueSource(fieldData, MultiValueMode.MEDIAN);
            case SUM_METHOD -> new FieldDataValueSource(fieldData, MultiValueMode.SUM);
            case COUNT_METHOD -> new CountMethodValueSource(fieldData);
            default -> throw new IllegalArgumentException(
                "Member method [" + method + "] does not exist for numeric field [" + fieldName + "]."
            );
        };
    }
}
