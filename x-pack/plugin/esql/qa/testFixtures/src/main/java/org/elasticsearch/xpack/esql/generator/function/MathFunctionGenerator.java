/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.Column;

import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomNumericField;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.fieldOrUnmapped;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.maybeUnmappedField;

/**
 * Generates random math function expressions.
 */
public final class MathFunctionGenerator {

    private MathFunctionGenerator() {}

    /**
     * Generates a math function that takes a numeric argument and returns a numeric value.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String mathFunction(List<Column> columns, boolean allowUnmapped) {
        String numericField = fieldOrUnmapped(randomNumericField(columns), allowUnmapped);
        if (numericField == null) {
            numericField = maybeUnmappedField(allowUnmapped);
            if (numericField == null) {
                return null;
            }
        }
        return randomFrom(
            "abs(" + numericField + ")",
            "ceil(" + numericField + ")",
            "floor(" + numericField + ")",
            "signum(" + numericField + ")",
            "sqrt(abs(" + numericField + "))",
            "cbrt(" + numericField + ")",
            "exp(" + numericField + " % 10)",
            "log10(abs(" + numericField + ") + 1)",
            "round(" + numericField + ")",
            "round(" + numericField + ", " + randomIntBetween(0, 5) + ")",
            "sin(" + numericField + ")",
            "cos(" + numericField + ")",
            "tan(" + numericField + ")",
            "asin(" + numericField + " % 1)",
            "acos(" + numericField + " % 1)",
            "atan(" + numericField + ")",
            "sinh(" + numericField + " % 10)",
            "cosh(" + numericField + " % 10)",
            "tanh(" + numericField + ")",
            "pi()",
            "e()",
            "tau()"
        );
    }

    /**
     * Generates a binary math function.
     * May randomly use unmapped field names to test NULL data type handling.
     * Note: greatest/least are handled separately to ensure type compatibility.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String binaryMathFunction(List<Column> columns, boolean allowUnmapped) {
        String field1 = fieldOrUnmapped(randomNumericField(columns), allowUnmapped);
        String field2 = fieldOrUnmapped(randomNumericField(columns), allowUnmapped);
        if (field1 == null || field2 == null) {
            return null;
        }
        return randomFrom(
            "pow(" + field1 + ", 2)",
            "pow(" + field1 + ", abs(" + field2 + ") % 5 + 1)",
            "log(abs(" + field1 + ") + 1, abs(" + field2 + ") + 2)",
            "atan2(" + field1 + ", " + field2 + ")",
            "hypot(" + field1 + ", " + field2 + ")",
            "copy_sign(" + field1 + ", " + field2 + ")",
            "scalb(" + field1 + ", " + randomIntBetween(-5, 5) + ")"
        );
    }

    /**
     * Generates a clamp function (clamp, clamp_min, clamp_max).
     * Note: clamp/clamp_min/clamp_max do NOT accept NULL for the field parameter,
     * so unmapped fields (which resolve to NULL type) must not be used here.
     *
     * @param columns the available columns
     * @return a clamp expression or {@code null} if no numeric field is available
     */
    public static String clampFunction(List<Column> columns) {
        String numericField = randomNumericField(columns);
        if (numericField == null) {
            return null;
        }
        int min = randomIntBetween(-100, 50);
        int max = min + randomIntBetween(1, 100);
        return randomFrom(
            "clamp(" + numericField + ", " + min + ", " + max + ")",
            "clamp_min(" + numericField + ", " + min + ")",
            "clamp_max(" + numericField + ", " + max + ")"
        );
    }
}

