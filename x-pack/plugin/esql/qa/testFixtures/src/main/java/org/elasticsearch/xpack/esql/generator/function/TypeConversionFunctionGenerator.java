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
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomNumericField;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomStringField;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.fieldOrUnmapped;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.maybeUnmappedField;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.randomScalarField;

/**
 * Generates random type conversion function expressions.
 */
public final class TypeConversionFunctionGenerator {

    private TypeConversionFunctionGenerator() {}

    /**
     * Generates a type conversion function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String conversionFunction(List<Column> columns, boolean allowUnmapped) {
        String unmapped = maybeUnmappedField(allowUnmapped);
        if (unmapped != null) {
            return randomFrom(
                "to_string(" + unmapped + ")",
                "to_integer(" + unmapped + ")",
                "to_long(" + unmapped + ")",
                "to_double(" + unmapped + ")"
            );
        }

        String anyField = randomScalarField(columns);
        if (anyField != null && randomBoolean()) {
            return "to_string(" + fieldOrUnmapped(anyField, allowUnmapped) + ")";
        }

        String numericField = randomNumericField(columns);
        if (numericField != null) {
            return randomFrom(
                "to_integer(" + numericField + ")",
                "to_long(" + numericField + ")",
                "to_double(" + numericField + ")",
                "to_string(" + numericField + ")",
                "to_degrees(" + numericField + ")",
                "to_radians(" + numericField + ")"
            );
        }

        String stringField = randomStringField(columns);
        if (stringField != null) {
            return randomFrom("to_string(" + stringField + ")", "to_lower(" + stringField + ")");
        }

        return null;
    }
}

