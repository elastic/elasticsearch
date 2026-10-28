/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.Column;

import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomNumericField;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomStringField;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.fieldOrUnmapped;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.maybeUnmappedField;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.randomScalarField;

/**
 * Generates random multivalue (mv_*) function expressions.
 */
public final class MvFunctionGenerator {

    private MvFunctionGenerator() {}

    /**
     * Generates a multivalue function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String mvFunction(List<Column> columns, boolean allowUnmapped) {
        String anyField = fieldOrUnmapped(randomScalarField(columns), allowUnmapped);
        if (anyField == null) {
            anyField = maybeUnmappedField(allowUnmapped);
            if (anyField == null) {
                return null;
            }
        }

        String genericMvFunc = randomFrom(
            "mv_count(" + anyField + ")",
            "mv_first(" + anyField + ")",
            "mv_last(" + anyField + ")",
            "mv_dedupe(" + anyField + ")"
        );

        String numericField = fieldOrUnmapped(randomNumericField(columns), allowUnmapped);
        if (numericField != null && randomBoolean()) {
            return randomFrom(
                "mv_min(" + numericField + ")",
                "mv_max(" + numericField + ")",
                "mv_avg(" + numericField + ")",
                "mv_sum(" + numericField + ")",
                "mv_median(" + numericField + ")"
            );
        }

        String stringField = fieldOrUnmapped(randomStringField(columns), allowUnmapped);
        if (stringField != null && randomBoolean()) {
            return randomFrom(
                "mv_concat(" + stringField + ", \", \")",
                "mv_sort(" + stringField + ")",
                "mv_sort(" + stringField + ", \"desc\")"
            );
        }

        return genericMvFunc;
    }

    /**
     * Generates mv_slice functions.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String mvSliceZipFunction(List<Column> columns, boolean allowUnmapped) {
        String field = fieldOrUnmapped(randomScalarField(columns), allowUnmapped);
        if (field == null) {
            return null;
        }
        int start = randomIntBetween(0, 3);
        int end = start + randomIntBetween(1, 5);
        return randomFrom("mv_slice(" + field + ", " + start + ", " + end + ")", "mv_slice(" + field + ", " + start + ")");
    }
}
