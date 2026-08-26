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
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomName;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomNumericField;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomStringField;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.shouldAddUnmappedFieldWithProbabilityIncrease;
import static org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator.randomUnmappedFieldName;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.fieldOrUnmapped;

/**
 * Generates random boolean expressions used in WHERE clauses.
 */
public final class BooleanExpressionGenerator {

    private BooleanExpressionGenerator() {}

    /**
     * Generates an IS NULL / IS NOT NULL expression.
     * May randomly use unmapped field names - especially useful for testing IS NULL on unmapped fields.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String isNullExpression(List<Column> columns, boolean allowUnmapped) {
        if (allowUnmapped && shouldAddUnmappedFieldWithProbabilityIncrease(3)) {
            String unmapped = randomUnmappedFieldName();
            return unmapped + (randomBoolean() ? " IS NULL" : " IS NOT NULL");
        }
        String field = randomName(columns);
        if (field == null) {
            return null;
        }
        return field + (randomBoolean() ? " IS NULL" : " IS NOT NULL");
    }

    /**
     * Generates an IN expression.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String inExpression(List<Column> columns, boolean allowUnmapped) {
        String numericField = fieldOrUnmapped(randomNumericField(columns), allowUnmapped);
        if (numericField != null && randomBoolean()) {
            int val1 = randomIntBetween(0, 100);
            int val2 = randomIntBetween(0, 100);
            int val3 = randomIntBetween(0, 100);
            return numericField + " IN (" + val1 + ", " + val2 + ", " + val3 + ")";
        }
        String stringField = fieldOrUnmapped(randomStringField(columns), allowUnmapped);
        if (stringField != null) {
            return stringField + " IN (\"a\", \"b\", \"c\")";
        }
        return null;
    }

    /**
     * Generates a LIKE expression.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String likeExpression(List<Column> columns, boolean allowUnmapped) {
        String stringField = fieldOrUnmapped(randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        String pattern = randomFrom("*", "a*", "*b", "*test*", "???");
        return stringField + " LIKE \"" + pattern + "\"";
    }

    /**
     * Generates an RLIKE expression.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String rlikeExpression(List<Column> columns, boolean allowUnmapped) {
        String stringField = fieldOrUnmapped(randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        String pattern = randomFrom(".*", "a.*", ".*b", ".*test.*", ".{3}");
        return stringField + " RLIKE \"" + pattern + "\"";
    }
}
