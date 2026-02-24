/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;
import org.elasticsearch.xpack.esql.generator.function.BooleanExpressionGenerator;
import org.elasticsearch.xpack.esql.generator.function.ConditionalFunctionGenerator;
import org.elasticsearch.xpack.esql.generator.function.DateFunctionGenerator;
import org.elasticsearch.xpack.esql.generator.function.FullTextFunctionGenerator;
import org.elasticsearch.xpack.esql.generator.function.IpFunctionGenerator;
import org.elasticsearch.xpack.esql.generator.function.MathFunctionGenerator;
import org.elasticsearch.xpack.esql.generator.function.MvFunctionGenerator;
import org.elasticsearch.xpack.esql.generator.function.StringFunctionGenerator;
import org.elasticsearch.xpack.esql.generator.function.TypeConversionFunctionGenerator;
import org.elasticsearch.xpack.esql.generator.function.TypeSafeExpressionGenerator;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * Generates random ESQL function expressions for testing.
 * This generator creates expressions for scalar functions across different categories:
 * math, string, date, type conversion, conditional, multivalue, ip, boolean expressions, and full-text.
 * <p>
 * Some functions will randomly use unmapped field names (from
 * {@link org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator#UNMAPPED_FIELD_NAMES})
 * to test how functions handle the NULL data type that comes from the "unmapped fields" functionality.
 * <p>
 * Note: Unmapped fields can only be used before schema-fixing commands (STATS, KEEP, DROP) are encountered.
 * After these commands, the field list is fixed and new unmapped fields cannot be introduced.
 * <p>
 * This class is a facade over category-specific generators in {@code org.elasticsearch.xpack.esql.generator.function}.
 * Callers should use this class so we can evolve the underlying generators without churning imports across the codebase.
 */
public class FunctionGenerator {

    /**
     * Types that are commonly supported across most scalar and aggregate functions.
     */
    public static final Set<String> COMMONLY_SUPPORTED_TYPES = Set.of(
        "integer",
        "long",
        "double",
        "keyword",
        "text",
        "date",
        "datetime",
        "boolean",
        "ip",
        "version"
    );

    /**
     * Probability (0-100) of using an unmapped field name instead of a real field.
     * This tests how functions handle NULL data type from unmapped fields.
     */
    private static final int UNMAPPED_FIELD_PROBABILITY = 10;

    /**
     * Command names that fix the schema - after these commands, no new unmapped fields can be introduced.
     */
    private static final Set<String> SCHEMA_FIXING_COMMANDS = Set.of("stats", "keep", "drop");

    /**
     * Checks if unmapped fields are allowed based on the command history.
     */
    public static boolean areUnmappedFieldsAllowed(List<CommandGenerator.CommandDescription> previousCommands) {
        if (previousCommands == null) {
            return false;
        }
        if (isUnmappedFieldsEnabled(previousCommands) == false) {
            return false;
        }
        for (CommandGenerator.CommandDescription cmd : previousCommands) {
            // Note: "inline stats" is different from "stats" - inline stats doesn't fix the schema
            if (SCHEMA_FIXING_COMMANDS.contains(cmd.commandName())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if SET unmapped_fields="nullify" was included in the FROM command.
     */
    public static boolean isUnmappedFieldsEnabled(List<CommandGenerator.CommandDescription> previousCommands) {
        if (previousCommands == null || previousCommands.isEmpty()) {
            return false;
        }
        CommandGenerator.CommandDescription fromCmd = previousCommands.get(0);
        Object enabled = fromCmd.context().get(FromGenerator.UNMAPPED_FIELDS_ENABLED);
        return Boolean.TRUE.equals(enabled);
    }

    /**
     * Returns {@code true} with a probability derived from {@link #UNMAPPED_FIELD_PROBABILITY}.
     * Used to occasionally inject unmapped field names into generated expressions.
     *
     * @param probabilityIncrease multiplier in the interval [1, 9]
     */
    public static boolean shouldAddUnmappedFieldWithProbabilityIncrease(int probabilityIncrease) {
        assert probabilityIncrease > 0 && probabilityIncrease < 10 : "Probability increase should be in interval [1, 9]";
        return randomIntBetween(0, 100) < UNMAPPED_FIELD_PROBABILITY * probabilityIncrease;
    }

    // ========= Math =========

    /**
     * Generates a math function that takes a numeric argument and returns a numeric value.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String mathFunction(List<Column> columns, boolean allowUnmapped) {
        return MathFunctionGenerator.mathFunction(columns, allowUnmapped);
    }

    /**
     * Generates a binary math function.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String binaryMathFunction(List<Column> columns, boolean allowUnmapped) {
        return MathFunctionGenerator.binaryMathFunction(columns, allowUnmapped);
    }

    /**
     * Generates a clamp function (clamp, clamp_min, clamp_max).
     * Unmapped fields (NULL type) are not allowed for clamp's field argument.
     */
    public static String clampFunction(List<Column> columns, boolean allowUnmapped) {
        return MathFunctionGenerator.clampFunction(columns);
    }

    // ========= String =========

    /**
     * Generates a string function that returns a string.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String stringFunction(List<Column> columns, boolean allowUnmapped) {
        return StringFunctionGenerator.stringFunction(columns, allowUnmapped);
    }

    /**
     * Generates a string function that returns an integer (length-like functions).
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String stringToIntFunction(List<Column> columns, boolean allowUnmapped) {
        return StringFunctionGenerator.stringToIntFunction(columns, allowUnmapped);
    }

    /**
     * Generates a string function that returns a boolean.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String stringToBoolFunction(List<Column> columns, boolean allowUnmapped) {
        return StringFunctionGenerator.stringToBoolFunction(columns, allowUnmapped);
    }

    /**
     * Generates a concat function with multiple arguments.
     * May randomly include unmapped field names to test NULL data type handling.
     */
    public static String concatFunction(List<Column> columns, boolean allowUnmapped) {
        return StringFunctionGenerator.concatFunction(columns, allowUnmapped);
    }

    /**
     * Generates a split function.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String splitFunction(List<Column> columns, boolean allowUnmapped) {
        return StringFunctionGenerator.splitFunction(columns, allowUnmapped);
    }

    // ========= Date =========

    /**
     * Generates a date function.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String dateFunction(List<Column> columns, boolean allowUnmapped) {
        return DateFunctionGenerator.dateFunction(columns, allowUnmapped);
    }

    /**
     * Generates a date_diff function.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String dateDiffFunction(List<Column> columns, boolean allowUnmapped) {
        return DateFunctionGenerator.dateDiffFunction(columns, allowUnmapped);
    }

    // ========= Type conversion =========

    /**
     * Generates a type conversion function.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String conversionFunction(List<Column> columns, boolean allowUnmapped) {
        return TypeConversionFunctionGenerator.conversionFunction(columns, allowUnmapped);
    }

    // ========= Conditional =========

    /**
     * Generates a CASE expression.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String caseFunction(List<Column> columns, boolean allowUnmapped) {
        return ConditionalFunctionGenerator.caseFunction(columns, allowUnmapped);
    }

    /**
     * Generates a COALESCE expression.
     * All arguments must be of the same type (no implicit type coercion).
     */
    public static String coalesceFunction(List<Column> columns, boolean allowUnmapped) {
        return ConditionalFunctionGenerator.coalesceFunction(columns, allowUnmapped);
    }

    /**
     * Generates a GREATEST or LEAST expression.
     * All arguments must be of the same type (no implicit type coercion).
     */
    public static String greatestLeastFunction(List<Column> columns, boolean allowUnmapped) {
        return ConditionalFunctionGenerator.greatestLeastFunction(columns, allowUnmapped);
    }

    // ========= Multivalue =========

    /**
     * Generates a multivalue function.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String mvFunction(List<Column> columns, boolean allowUnmapped) {
        return MvFunctionGenerator.mvFunction(columns, allowUnmapped);
    }

    /**
     * Generates mv_slice functions.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String mvSliceZipFunction(List<Column> columns, boolean allowUnmapped) {
        return MvFunctionGenerator.mvSliceZipFunction(columns, allowUnmapped);
    }

    // ========= IP =========

    /**
     * Generates an cidr_match function.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String cidrMatchFunction(List<Column> columns, boolean allowUnmapped) {
        return IpFunctionGenerator.cidrMatchFunction(columns, allowUnmapped);
    }

    /**
     * Generates an ip_prefix function.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String ipPrefixFunction(List<Column> columns, boolean allowUnmapped) {
        return IpFunctionGenerator.ipPrefixFunction(columns, allowUnmapped);
    }

    // ========= Boolean expressions =========

    /**
     * Generates an IS NULL / IS NOT NULL expression.
     * May randomly use unmapped field names - especially useful for testing IS NULL on unmapped fields.
     */
    public static String isNullExpression(List<Column> columns, boolean allowUnmapped) {
        return BooleanExpressionGenerator.isNullExpression(columns, allowUnmapped);
    }

    /**
     * Generates an IN expression.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String inExpression(List<Column> columns, boolean allowUnmapped) {
        return BooleanExpressionGenerator.inExpression(columns, allowUnmapped);
    }

    /**
     * Generates a LIKE expression.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String likeExpression(List<Column> columns, boolean allowUnmapped) {
        return BooleanExpressionGenerator.likeExpression(columns, allowUnmapped);
    }

    /**
     * Generates an RLIKE expression.
     * May randomly use unmapped field names to test NULL data type handling.
     */
    public static String rlikeExpression(List<Column> columns, boolean allowUnmapped) {
        return BooleanExpressionGenerator.rlikeExpression(columns, allowUnmapped);
    }

    // ========= Full-text =========

    /**
     * Generates a random full-text search boolean expression.
     * See {@link org.elasticsearch.xpack.esql.generator.function.FullTextFunctionGenerator} for placement and field-origin constraints.
     */
    public static String fullTextFunction(List<Column> columns, List<CommandGenerator.CommandDescription> previousCommands) {
        return FullTextFunctionGenerator.fullTextFunction(columns, previousCommands);
    }

    // ========= Type-safe expressions =========

    /**
     * Generates an expression whose resulting type is in {@code acceptedTypes}.
     */
    public static String typeSafeExpression(List<Column> columns, Set<String> acceptedTypes, boolean allowUnmapped) {
        return TypeSafeExpressionGenerator.typeSafeExpression(columns, acceptedTypes, allowUnmapped);
    }
}

