/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.Column;

import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.needsQuoting;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.quote;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomNumericField;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.COMMONLY_SUPPORTED_TYPES;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.shouldAddUnmappedFieldWithProbabilityIncrease;
import static org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator.randomUnmappedFieldName;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.fieldOrUnmapped;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.shouldAddUnmappedField;

/**
 * Generates random conditional function expressions.
 */
public final class ConditionalFunctionGenerator {

    private ConditionalFunctionGenerator() {}

    /**
     * Generates a CASE expression.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String caseFunction(List<Column> columns, boolean allowUnmapped) {
        String numericField = fieldOrUnmapped(randomNumericField(columns), allowUnmapped);
        if (numericField == null) {
            return null;
        }
        int threshold = randomIntBetween(0, 100);
        return "case(" + numericField + " > " + threshold + ", \"high\", \"low\")";
    }

    /**
     * Generates a COALESCE expression.
     * IMPORTANT: All arguments must be of the same type. COALESCE does NOT do type coercion.
     * Only uses columns with commonly supported types to ensure the result can be consumed
     * by other functions.
     * <p>
     * May randomly include unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String coalesceFunction(List<Column> columns, boolean allowUnmapped) {
        var columnsByType = columns.stream()
            .filter(c -> COMMONLY_SUPPORTED_TYPES.contains(c.type()))
            .collect(Collectors.groupingBy(Column::type));

        List<Column> sameTypeColumns = null;
        for (var entry : columnsByType.entrySet()) {
            if (entry.getValue().isEmpty() == false) {
                sameTypeColumns = entry.getValue();
                break;
            }
        }

        if (sameTypeColumns == null || sameTypeColumns.isEmpty()) {
            return null;
        }

        String field1Raw = sameTypeColumns.get(randomIntBetween(0, sameTypeColumns.size() - 1)).name();
        String field1 = needsQuoting(field1Raw) ? quote(field1Raw) : field1Raw;

        if (allowUnmapped && shouldAddUnmappedFieldWithProbabilityIncrease(2)) {
            String unmapped = randomUnmappedFieldName();
            return "coalesce(" + unmapped + ", " + field1 + ")";
        }

        if (sameTypeColumns.size() >= 2) {
            String field2Raw = sameTypeColumns.get(randomIntBetween(0, sameTypeColumns.size() - 1)).name();
            String field2 = needsQuoting(field2Raw) ? quote(field2Raw) : field2Raw;
            if (field1.equals(field2) == false) {
                return "coalesce(" + field1 + ", " + field2 + ")";
            }
        }

        return "coalesce(" + field1 + ", null)";
    }

    /**
     * Generates a GREATEST or LEAST expression.
     * IMPORTANT: All arguments must be of the same type. These functions do NOT do type coercion.
     * May randomly include unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String greatestLeastFunction(List<Column> columns, boolean allowUnmapped) {
        String targetType = randomFrom("integer", "long", "double");
        List<String> sameTypeFields = columns.stream()
            .filter(c -> c.type().equals(targetType))
            .map(c -> needsQuoting(c.name()) ? quote(c.name()) : c.name())
            .collect(Collectors.toList());

        if (allowUnmapped && shouldAddUnmappedField() && sameTypeFields.isEmpty() == false) {
            sameTypeFields.add(randomUnmappedFieldName());
        }

        if (sameTypeFields.size() < 2) {
            String numericField = randomNumericField(columns);
            if (numericField != null) {
                String func = randomBoolean() ? "greatest" : "least";
                int val1 = randomIntBetween(-100, 100);
                int val2 = randomIntBetween(-100, 100);
                return func + "(" + numericField + ", " + val1 + ", " + val2 + ")";
            }
            return null;
        }

        String func = randomBoolean() ? "greatest" : "least";
        int numArgs = Math.min(sameTypeFields.size(), randomIntBetween(2, 4));
        return func + "(" + String.join(", ", sameTypeFields.subList(0, numArgs)) + ")";
    }
}

