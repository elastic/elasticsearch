/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.EvalGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator.randomUnmappedFieldName;

/**
 * Generates random ESQL function expressions for testing.
 * This generator creates expressions for scalar functions across different categories:
 * math, string, date, type conversion, conditional, and multivalue functions.
 * <p>
 * Some functions will randomly use unmapped field names (from {@link org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator#UNMAPPED_FIELD_NAMES})
 * to test how functions handle the NULL data type that comes from the "unmapped fields" functionality.
 * <p>
 * Note: Unmapped fields can only be used before schema-fixing commands (STATS, KEEP, DROP) are encountered.
 * After these commands, the field list is fixed and new unmapped fields cannot be introduced.
 */
public class FunctionGenerator {

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
     * Types that are commonly supported across most scalar and aggregate functions.
     * Functions like coalesce() can produce expressions of any type, but when those expressions
     * are used as arguments to other functions (like top(), greatest(), etc.), the type must
     * be compatible. Restricting to these types avoids type errors when composing function calls.
     * <p>
     * Notably excludes: date_range, geo_point, geo_shape, cartesian_point, cartesian_shape,
     * histogram, unsigned_long, aggregate_metric_double, and other rare types.
     */
    static final Set<String> COMMONLY_SUPPORTED_TYPES = Set.of(
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
     * Types that are NOT accepted by most scalar functions. These are special metric/internal types
     * that should be excluded when selecting fields for general-purpose function arguments.
     * <p>
     * Most scalar functions (mv_slice, mv_count, to_string, etc.) reject these types with errors like:
     * "must be [any type except counter types, dense_vector, aggregate_metric_double, ...]"
     */
    private static final Set<String> SCALAR_UNSUPPORTED_TYPES = Set.of(
        "counter_long",
        "counter_double",
        "counter_integer",
        "aggregate_metric_double",
        "dense_vector",
        "tdigest",
        "histogram",
        "exponential_histogram",
        "date_range"
    );

    /**
     * Returns a field name suitable for use as a scalar function argument.
     * Excludes types that are rejected by most scalar functions (counter types, aggregate_metric_double, etc.).
     *
     * @param columns the available columns
     * @return a field name of a type accepted by most scalar functions, or null if none available
     */
    static String randomScalarField(List<Column> columns) {
        List<Column> suitable = columns.stream().filter(c -> SCALAR_UNSUPPORTED_TYPES.contains(c.type()) == false).toList();
        if (suitable.isEmpty()) {
            return null;
        }
        return EsqlQueryGenerator.randomName(suitable);
    }

    /**
     * Checks if unmapped fields are allowed based on the command history.
     * Unmapped fields require two conditions to be met:
     * <ol>
     *   <li>The SET unmapped_fields="nullify" directive must be present in the FROM command</li>
     *   <li>No schema-fixing commands (STATS, KEEP, DROP) must have been encountered yet,
     *       since those commands fix the field list and new unmapped fields cannot be introduced after them</li>
     * </ol>
     *
     * @param previousCommands the list of previous commands in the query
     * @return true if unmapped fields can be used, false otherwise
     */
    public static boolean areUnmappedFieldsAllowed(List<CommandGenerator.CommandDescription> previousCommands) {
        if (previousCommands == null) {
            return false;
        }
        // Check if SET unmapped_fields="nullify" was included in the FROM command
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
        // The FROM command is always the first command
        CommandGenerator.CommandDescription fromCmd = previousCommands.get(0);
        Object enabled = fromCmd.context().get(FromGenerator.UNMAPPED_FIELDS_ENABLED);
        return Boolean.TRUE.equals(enabled);
    }

    /**
     * Returns an unmapped field name with some probability, otherwise returns null.
     * Use this to occasionally inject unmapped fields into function arguments.
     *
     * @param allowUnmapped if false, always returns null (unmapped fields not allowed)
     */
    private static String maybeUnmappedField(boolean allowUnmapped) {
        if (allowUnmapped == false) {
            return null;
        }
        return shouldAddUnmappedField() ? randomUnmappedFieldName() : null;
    }

    private static boolean shouldAddUnmappedField() {
        return shouldAddUnmappedFieldWithProbabilityIncrease(1);
    }

    public static boolean shouldAddUnmappedFieldWithProbabilityIncrease(int probabilityIncrease) {
        assert probabilityIncrease > 0 && probabilityIncrease < 10 : "Probability increase should be in interval [1, 9]";
        return randomIntBetween(0, 100) < UNMAPPED_FIELD_PROBABILITY * probabilityIncrease;
    }

    /**
     * Returns a field name, with some probability returning an unmapped field name instead.
     *
     * @param realField the real field to use if not using unmapped
     * @param allowUnmapped if false, never returns an unmapped field
     * @return either the unmapped field name or the real field
     */
    private static String fieldOrUnmapped(String realField, boolean allowUnmapped) {
        if (realField == null) {
            return null;
        }
        String unmapped = maybeUnmappedField(allowUnmapped);
        return unmapped != null ? unmapped : realField;
    }

    // ========== MATH FUNCTIONS ==========

    /**
     * Generates a math function that takes a numeric argument and returns a numeric value.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String mathFunction(List<Column> columns, boolean allowUnmapped) {
        String numericField = fieldOrUnmapped(EsqlQueryGenerator.randomNumericField(columns), allowUnmapped);
        if (numericField == null) {
            // Fall back to unmapped field if no numeric fields available
            numericField = maybeUnmappedField(allowUnmapped);
            if (numericField == null) {
                return null;
            }
        }
        return randomFrom(
            // Unary math functions
            "abs(" + numericField + ")",
            "ceil(" + numericField + ")",
            "floor(" + numericField + ")",
            "signum(" + numericField + ")",
            "sqrt(abs(" + numericField + "))",  // abs to avoid negative sqrt
            "cbrt(" + numericField + ")",
            "exp(" + numericField + " % 10)",  // mod to avoid overflow
            "log10(abs(" + numericField + ") + 1)",  // +1 to avoid log(0)
            "round(" + numericField + ")",
            "round(" + numericField + ", " + randomIntBetween(0, 5) + ")",
            // Trigonometric functions
            "sin(" + numericField + ")",
            "cos(" + numericField + ")",
            "tan(" + numericField + ")",
            "asin(" + numericField + " % 1)",  // mod 1 to keep in [-1,1]
            "acos(" + numericField + " % 1)",
            "atan(" + numericField + ")",
            "sinh(" + numericField + " % 10)",
            "cosh(" + numericField + " % 10)",
            "tanh(" + numericField + ")",
            // Constants
            "pi()",
            "e()",
            "tau()"
        );
    }

    /**
     * Generates a binary math function.
     * May randomly use unmapped field names to test NULL data type handling.
     * Note: greatest/least are handled separately in greatestLeastFunction to ensure type compatibility.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String binaryMathFunction(List<Column> columns, boolean allowUnmapped) {
        String field1 = fieldOrUnmapped(EsqlQueryGenerator.randomNumericField(columns), allowUnmapped);
        String field2 = fieldOrUnmapped(EsqlQueryGenerator.randomNumericField(columns), allowUnmapped);
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
     * @param allowUnmapped ignored for the field parameter since clamp rejects NULL fields
     */
    public static String clampFunction(List<Column> columns, boolean allowUnmapped) {
        // clamp/clamp_min/clamp_max reject NULL for the field parameter, so don't use unmapped fields
        String numericField = EsqlQueryGenerator.randomNumericField(columns);
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

    // ========== STRING FUNCTIONS ==========

    /**
     * Generates a string function that returns a string.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String stringFunction(List<Column> columns, boolean allowUnmapped) {
        String stringField = fieldOrUnmapped(EsqlQueryGenerator.randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        return randomFrom(
            "to_lower(" + stringField + ")",
            "to_upper(" + stringField + ")",
            "trim(" + stringField + ")",
            "ltrim(" + stringField + ")",
            "rtrim(" + stringField + ")",
            "reverse(" + stringField + ")",
            "left(" + stringField + ", " + randomIntBetween(1, 10) + ")",
            "right(" + stringField + ", " + randomIntBetween(1, 10) + ")",
            "substring(" + stringField + ", " + randomIntBetween(0, 5) + ", " + randomIntBetween(1, 10) + ")",
            "repeat(" + stringField + ", " + randomIntBetween(1, 3) + ")",
            "space(" + randomIntBetween(0, 10) + ")",
            "replace(" + stringField + ", \"a\", \"b\")",
            "md5(" + stringField + ")",
            "sha1(" + stringField + ")",
            "sha256(" + stringField + ")",
            "to_base64(" + stringField + ")",
            "from_base64(to_base64(" + stringField + "))",
            "url_encode(" + stringField + ")",
            "url_decode(" + stringField + ")"
        );
    }

    /**
     * Generates a string function that returns an integer (length-like functions).
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String stringToIntFunction(List<Column> columns, boolean allowUnmapped) {
        String stringField = fieldOrUnmapped(EsqlQueryGenerator.randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        return randomFrom(
            "length(" + stringField + ")",
            "bit_length(" + stringField + ")",
            "byte_length(" + stringField + ")",
            "locate(" + stringField + ", \"a\")",
            "locate(" + stringField + ", \"a\", " + randomIntBetween(0, 5) + ")"
        );
    }

    /**
     * Generates a string function that returns a boolean.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String stringToBoolFunction(List<Column> columns, boolean allowUnmapped) {
        String stringField = fieldOrUnmapped(EsqlQueryGenerator.randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        String searchStr = "\"" + randomAlphaOfLength(randomIntBetween(1, 3)) + "\"";
        return randomFrom(
            "starts_with(" + stringField + ", " + searchStr + ")",
            "ends_with(" + stringField + ", " + searchStr + ")",
            "contains(" + stringField + ", " + searchStr + ")"
        );
    }

    /**
     * Generates a concat function with multiple arguments.
     * May randomly include unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String concatFunction(List<Column> columns, boolean allowUnmapped) {
        List<String> stringFields = columns.stream()
            .filter(c -> c.type().equals("keyword") || c.type().equals("text"))
            .map(c -> EsqlQueryGenerator.needsQuoting(c.name()) ? EsqlQueryGenerator.quote(c.name()) : c.name())
            .limit(randomIntBetween(2, 4))
            .collect(Collectors.toList());
        if (stringFields.isEmpty()) {
            return null;
        }
        // Possibly add an unmapped field to the concat arguments
        if (allowUnmapped && shouldAddUnmappedField()) {
            stringFields.add(randomUnmappedFieldName());
        }
        if (stringFields.size() < 2) {
            return null;
        }
        return "concat(" + String.join(", ", stringFields) + ")";
    }

    /**
     * Generates a split function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String splitFunction(List<Column> columns, boolean allowUnmapped) {
        String stringField = fieldOrUnmapped(EsqlQueryGenerator.randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        String delimiter = randomFrom(",", " ", "-", "_", ":");
        return "split(" + stringField + ", \"" + delimiter + "\")";
    }

    // ========== DATE FUNCTIONS ==========

    /**
     * Generates a date function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String dateFunction(List<Column> columns, boolean allowUnmapped) {
        String dateField = fieldOrUnmapped(EsqlQueryGenerator.randomName(columns, Set.of("date", "datetime")), allowUnmapped);
        if (dateField == null) {
            return null;
        }
        String datePart = randomFrom(
            "YEAR",
            "MONTH_OF_YEAR",
            "DAY_OF_MONTH",
            "HOUR_OF_DAY",
            "MINUTE_OF_HOUR",
            "SECOND_OF_MINUTE",
            "DAY_OF_WEEK",
            "DAY_OF_YEAR",
            "ALIGNED_WEEK_OF_YEAR"
        );
        String interval = randomFrom("1 day", "1 hour", "1 week", "1 month", "1 year");
        return randomFrom(
            "date_extract(\"" + datePart + "\", " + dateField + ")",
            "date_trunc(" + interval + ", " + dateField + ")",
            "date_format(\"yyyy-MM-dd\", " + dateField + ")",
            "day_name(" + dateField + ")",
            "month_name(" + dateField + ")",
            "now()"
        );
    }

    /**
     * Generates a date_diff function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String dateDiffFunction(List<Column> columns, boolean allowUnmapped) {
        List<String> dateFields = columns.stream()
            .filter(c -> c.type().equals("date") || c.type().equals("datetime"))
            .map(c -> EsqlQueryGenerator.needsQuoting(c.name()) ? EsqlQueryGenerator.quote(c.name()) : c.name())
            .collect(Collectors.toList());
        // Possibly add an unmapped field
        if (allowUnmapped && shouldAddUnmappedField()) {
            dateFields.add(randomUnmappedFieldName());
        }
        if (dateFields.size() < 2) {
            return null;
        }
        String unit = randomFrom("second", "minute", "hour", "day", "week", "month", "year");
        return "date_diff(\"" + unit + "\", " + dateFields.get(0) + ", " + dateFields.get(1) + ")";
    }

    // ========== TYPE CONVERSION FUNCTIONS ==========

    /**
     * Generates a type conversion function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String conversionFunction(List<Column> columns, boolean allowUnmapped) {
        // Occasionally use unmapped field for conversion functions
        String unmapped = maybeUnmappedField(allowUnmapped);
        if (unmapped != null) {
            return randomFrom(
                "to_string(" + unmapped + ")",
                "to_integer(" + unmapped + ")",
                "to_long(" + unmapped + ")",
                "to_double(" + unmapped + ")"
            );
        }

        // to_string - works on most types (but not counter types, aggregate_metric_double, etc.)
        String anyField = randomScalarField(columns);
        if (anyField != null && randomBoolean()) {
            return "to_string(" + fieldOrUnmapped(anyField, allowUnmapped) + ")";
        }

        // Numeric conversions
        String numericField = EsqlQueryGenerator.randomNumericField(columns);
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

        // String to various types
        String stringField = EsqlQueryGenerator.randomStringField(columns);
        if (stringField != null) {
            return randomFrom("to_string(" + stringField + ")", "to_lower(" + stringField + ")");
        }

        return null;
    }

    // ========== CONDITIONAL FUNCTIONS ==========

    /**
     * Generates a CASE expression.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String caseFunction(List<Column> columns, boolean allowUnmapped) {
        String numericField = fieldOrUnmapped(EsqlQueryGenerator.randomNumericField(columns), allowUnmapped);
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
     * by other functions (e.g. top(), greatest/least, aggregation functions).
     * May randomly include unmapped field names to test NULL data type handling.
     * This is especially useful for coalesce since it's designed to handle nulls.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String coalesceFunction(List<Column> columns, boolean allowUnmapped) {
        // COALESCE requires all arguments to be the SAME type
        // Only use commonly supported types so the result type is compatible with other functions
        var columnsByType = columns.stream()
            .filter(c -> COMMONLY_SUPPORTED_TYPES.contains(c.type()))
            .collect(Collectors.groupingBy(Column::type));

        // Find a type that has at least one field
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
        String field1 = EsqlQueryGenerator.needsQuoting(field1Raw) ? EsqlQueryGenerator.quote(field1Raw) : field1Raw;

        // Coalesce is perfect for testing unmapped fields - it handles nulls by design
        // Use unmapped field as first argument (will be null, so second arg is returned)
        if (allowUnmapped && shouldAddUnmappedFieldWithProbabilityIncrease(2)) {
            String unmapped = randomUnmappedFieldName();
            return "coalesce(" + unmapped + ", " + field1 + ")";
        }

        // Pick a second field of the same type
        if (sameTypeColumns.size() >= 2) {
            String field2Raw = sameTypeColumns.get(randomIntBetween(0, sameTypeColumns.size() - 1)).name();
            String field2 = EsqlQueryGenerator.needsQuoting(field2Raw) ? EsqlQueryGenerator.quote(field2Raw) : field2Raw;
            if (field1.equals(field2) == false) {
                return "coalesce(" + field1 + ", " + field2 + ")";
            }
        }

        // Fallback: use null literal as second argument (always valid)
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
        // GREATEST/LEAST require all arguments to be the SAME type - pick one type and stick with it
        String targetType = randomFrom("integer", "long", "double");
        List<String> sameTypeFields = columns.stream()
            .filter(c -> c.type().equals(targetType))
            .map(c -> EsqlQueryGenerator.needsQuoting(c.name()) ? EsqlQueryGenerator.quote(c.name()) : c.name())
            .collect(Collectors.toList());

        // Possibly add an unmapped field (which has NULL type, accepted by these functions)
        if (allowUnmapped && shouldAddUnmappedField() && sameTypeFields.isEmpty() == false) {
            sameTypeFields.add(randomUnmappedFieldName());
        }

        if (sameTypeFields.size() < 2) {
            // Not enough fields of the same type, try with constants of a consistent type
            String numericField = EsqlQueryGenerator.randomNumericField(columns);
            if (numericField != null) {
                // Use the same field multiple times with different constant comparisons
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

    // ========== MULTIVALUE FUNCTIONS ==========

    /**
     * Generates a multivalue function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String mvFunction(List<Column> columns, boolean allowUnmapped) {
        // Use randomScalarField to avoid counter types, aggregate_metric_double, etc.
        String anyField = fieldOrUnmapped(randomScalarField(columns), allowUnmapped);
        if (anyField == null) {
            // Fall back to just unmapped field
            anyField = maybeUnmappedField(allowUnmapped);
            if (anyField == null) {
                return null;
            }
        }

        // Functions that work on any type
        String genericMvFunc = randomFrom(
            "mv_count(" + anyField + ")",
            "mv_first(" + anyField + ")",
            "mv_last(" + anyField + ")",
            "mv_dedupe(" + anyField + ")"
        );

        String numericField = fieldOrUnmapped(EsqlQueryGenerator.randomNumericField(columns), allowUnmapped);
        if (numericField != null && randomBoolean()) {
            return randomFrom(
                "mv_min(" + numericField + ")",
                "mv_max(" + numericField + ")",
                "mv_avg(" + numericField + ")",
                "mv_sum(" + numericField + ")",
                "mv_median(" + numericField + ")"
            );
        }

        String stringField = fieldOrUnmapped(EsqlQueryGenerator.randomStringField(columns), allowUnmapped);
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
     * Generates mv_slice or mv_zip functions.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String mvSliceZipFunction(List<Column> columns, boolean allowUnmapped) {
        // Use randomScalarField to avoid counter types, aggregate_metric_double, etc.
        String field = fieldOrUnmapped(randomScalarField(columns), allowUnmapped);
        if (field == null) {
            return null;
        }
        int start = randomIntBetween(0, 3);
        int end = start + randomIntBetween(1, 5);
        return randomFrom("mv_slice(" + field + ", " + start + ", " + end + ")", "mv_slice(" + field + ", " + start + ")");
    }

    // ========== IP FUNCTIONS ==========

    /**
     * Generates an cidr_match function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String cidrMatchFunction(List<Column> columns, boolean allowUnmapped) {
        String ipField = fieldOrUnmapped(EsqlQueryGenerator.randomName(columns, Set.of("ip")), allowUnmapped);
        if (ipField == null) {
            return null;
        }
        String cidr = randomFrom("10.0.0.0/8", "192.168.0.0/16", "172.16.0.0/12", "0.0.0.0/0");
        return "cidr_match(" + ipField + ", \"" + cidr + "\")";
    }

    /**
     * Generates an ip_prefix function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String ipPrefixFunction(List<Column> columns, boolean allowUnmapped) {
        String ipField = fieldOrUnmapped(EsqlQueryGenerator.randomName(columns, Set.of("ip")), allowUnmapped);
        if (ipField == null) {
            return null;
        }
        return "ip_prefix(" + ipField + ", " + randomIntBetween(8, 32) + ", " + randomIntBetween(48, 128) + ")";
    }

    // ========== BOOLEAN EXPRESSIONS ==========

    /**
     * Generates an IS NULL / IS NOT NULL expression.
     * May randomly use unmapped field names - especially useful for testing IS NULL on unmapped fields.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String isNullExpression(List<Column> columns, boolean allowUnmapped) {
        // Higher probability for unmapped fields in IS NULL expressions since they're always null
        if (allowUnmapped && shouldAddUnmappedFieldWithProbabilityIncrease(3)) {
            String unmapped = randomUnmappedFieldName();
            // Unmapped fields are always null, so IS NULL should be true, IS NOT NULL should be false
            return unmapped + (randomBoolean() ? " IS NULL" : " IS NOT NULL");
        }
        String field = EsqlQueryGenerator.randomName(columns);
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
        String numericField = fieldOrUnmapped(EsqlQueryGenerator.randomNumericField(columns), allowUnmapped);
        if (numericField != null && randomBoolean()) {
            int val1 = randomIntBetween(0, 100);
            int val2 = randomIntBetween(0, 100);
            int val3 = randomIntBetween(0, 100);
            return numericField + " IN (" + val1 + ", " + val2 + ", " + val3 + ")";
        }
        String stringField = fieldOrUnmapped(EsqlQueryGenerator.randomStringField(columns), allowUnmapped);
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
        String stringField = fieldOrUnmapped(EsqlQueryGenerator.randomStringField(columns), allowUnmapped);
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
        String stringField = fieldOrUnmapped(EsqlQueryGenerator.randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        String pattern = randomFrom(".*", "a.*", ".*b", ".*test.*", ".{3}");
        return stringField + " RLIKE \"" + pattern + "\"";
    }

    // ========== FULL-TEXT SEARCH FUNCTIONS ==========
    // These functions return boolean and are intended for use in WHERE clauses.
    // The query argument must be a foldable string literal. The field argument must reference an actual index field.
    //
    // Placement constraints (from FullTextFunction.checkFullTextQueryFunctions):
    //   - All full-text functions: forbidden after LIMIT, STATS, or UnionAll
    //   - QSTR and KQL only: all preceding commands must be FROM, WHERE, or SORT (OrderBy);
    //     any other command (EVAL, KEEP, DROP, RENAME, ENRICH, etc.) blocks them
    //   - Cannot operate on fields from non-STANDARD index modes (time_series)

    /**
     * Preceding command names that are compatible with QSTR and KQL placement rules.
     * These correspond to plan nodes Filter, OrderBy, and EsRelation respectively.
     */
    private static final Set<String> QSTR_KQL_SAFE_COMMANDS = Set.of("from", "where", "sort");

    /**
     * Checks whether full-text functions can be generated at the current position in the pipeline.
     * Blocks if:
     * <ul>
     *   <li>Source is not a standard FROM (e.g. time series "ts" or "PROMQL")</li>
     *   <li>A LIMIT or STATS command appeared earlier in the pipeline</li>
     * </ul>
     */
    private static boolean isFullTextAllowed(List<CommandGenerator.CommandDescription> previousCommands) {
        if (previousCommands == null || previousCommands.isEmpty()) {
            return false;
        }
        if ("from".equals(previousCommands.get(0).commandName()) == false) {
            return false;
        }
        for (CommandGenerator.CommandDescription cmd : previousCommands) {
            if ("limit".equals(cmd.commandName())
                || "stats".equals(cmd.commandName())
                || "inline stats".equals(cmd.commandName())
                || "change_point".equals(cmd.commandName())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks whether QSTR and KQL can be generated at the current position.
     * These have a stricter constraint: all preceding commands must be FROM, WHERE, or SORT.
     */
    private static boolean isQstrKqlAllowed(List<CommandGenerator.CommandDescription> previousCommands) {
        if (isFullTextAllowed(previousCommands) == false) {
            return false;
        }
        for (CommandGenerator.CommandDescription cmd : previousCommands) {
            if (QSTR_KQL_SAFE_COMMANDS.contains(cmd.commandName()) == false) {
                return false;
            }
        }
        return true;
    }

    private static final Pattern RENAME_PAIR = Pattern.compile(
        "\\s*`?([^`]+?)`?\\s+[Aa][Ss]\\s+`?([^`]+?)`?\\s*"
    );

    /**
     * Returns only columns whose names match fields from the original index mapping.
     * The FROM command's context stores the initial field names after execution
     * (see {@link FromGenerator#INDEX_FIELD_NAMES}). This method intersects the current
     * column list with that set so that field-based full-text functions only reference
     * actual index fields, not columns created by EVAL, GROK, DISSECT, RENAME, etc.
     *
     * @param columns the current output columns
     * @param previousCommands the commands executed so far
     * @return filtered column list containing only index fields, or {@code null} if unavailable
     */
    @SuppressWarnings("unchecked")
    static List<Column> indexFieldColumns(List<Column> columns, List<CommandGenerator.CommandDescription> previousCommands) {
        if (previousCommands == null || previousCommands.isEmpty()) {
            return null;
        }
        Object stored = previousCommands.get(0).context().get(FromGenerator.INDEX_FIELD_NAMES);
        if (stored instanceof Set<?> == false) {
            return null;
        }
        Set<String> safeNames = new HashSet<>((Set<String>) stored);
        for (CommandGenerator.CommandDescription cmd : previousCommands) {
            if ("eval".equals(cmd.commandName())) {
                Object newCols = cmd.context().get(EvalGenerator.NEW_COLUMNS);
                if (newCols instanceof List<?> list) {
                    list.forEach(name -> safeNames.remove((String) name));
                }
            } else if ("mv_expand".equals(cmd.commandName())) {
                String expandedField = cmd.commandString().replaceFirst("(?i)^\\s*\\|\\s*mv_expand\\s+", "").trim();
                if (expandedField.startsWith("`") && expandedField.endsWith("`")) {
                    expandedField = expandedField.substring(1, expandedField.length() - 1);
                }
                safeNames.remove(expandedField);
            } else if ("rename".equals(cmd.commandName())) {
                String cmdStr = cmd.commandString().replaceFirst("(?i)^\\s*\\|\\s*rename\\s+", "");
                for (String pair : cmdStr.split(",")) {
                    Matcher m = RENAME_PAIR.matcher(pair);
                    if (m.matches()) {
                        String oldName = m.group(1);
                        String newName = m.group(2);
                        boolean wasSafe = safeNames.remove(oldName);
                        if (wasSafe) {
                            safeNames.add(newName);
                        } else {
                            safeNames.remove(newName);
                        }
                    }
                }
            }
        }
        return columns.stream().filter(c -> safeNames.contains(c.name())).toList();
    }

    /**
     * Types accepted by match() and multi_match() for the field parameter.
     * From {@code Match.FIELD_DATA_TYPES} / {@code MultiMatch.FIELD_DATA_TYPES}.
     */
    private static final Set<String> MATCH_FIELD_TYPES = Set.of(
        "keyword",
        "text",
        "boolean",
        "date",
        "datetime",
        "double",
        "integer",
        "ip",
        "long",
        "unsigned_long",
        "version"
    );

    /**
     * Types accepted by match_phrase() for the field parameter.
     * From {@code MatchPhrase.FIELD_DATA_TYPES}.
     */
    private static final Set<String> MATCH_PHRASE_FIELD_TYPES = Set.of("keyword", "text");

    private static final String[] SAMPLE_QUERY_WORDS = { "test", "hello", "world", "data", "search", "quick", "brown", "fox" };

    private static String randomQueryWord() {
        return randomFrom(SAMPLE_QUERY_WORDS);
    }

    /**
     * Builds an optional map-literal argument for full-text functions.
     * Options are passed as a JSON-like map: {@code {"key": value, "key2": value2}}.
     * Returns empty string most of the time (no options added).
     *
     * @param optionPool each entry is {@code [name, value1, value2, ...]} where values are
     *                   already in the correct literal form (quoted strings, raw numbers/booleans)
     */
    private static String maybeOptions(String[][] optionPool) {
        if (randomIntBetween(0, 4) > 0) {
            return "";
        }
        int count = Math.min(randomIntBetween(1, 2), optionPool.length);
        Set<Integer> usedIndices = new HashSet<>();
        StringBuilder sb = new StringBuilder(", {");
        int added = 0;
        for (int i = 0; i < count; i++) {
            int idx = randomIntBetween(0, optionPool.length - 1);
            if (usedIndices.add(idx) == false) {
                continue;
            }
            String[] entry = optionPool[idx];
            String name = entry[0];
            String value = entry[randomIntBetween(1, entry.length - 1)];
            if (added > 0) {
                sb.append(", ");
            }
            sb.append("\"").append(name).append("\": ").append(value);
            added++;
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Option pool for match() — derived from {@code @MapParam.MapParamEntry} annotations on {@code Match}.
     * Values are in JSON-literal form: strings quoted, numbers/booleans raw.
     */
    private static final String[][] MATCH_OPTIONS = {
        { "operator", "\"AND\"", "\"OR\"" },
        { "fuzziness", "\"AUTO\"", "1", "2" },
        { "lenient", "true", "false" },
        { "boost", "1.0", "2.5" },
        { "zero_terms_query", "\"none\"", "\"all\"" },
    };

    /**
     * Option pool for match_phrase() — from {@code @MapParam.MapParamEntry} on {@code MatchPhrase}.
     */
    private static final String[][] MATCH_PHRASE_OPTIONS = {
        { "slop", "0", "1", "2" },
        { "boost", "1.0", "2.5" },
        { "zero_terms_query", "\"none\"", "\"all\"" },
    };

    /**
     * Option pool for qstr() — from {@code @MapParam.MapParamEntry} on {@code QueryString}.
     */
    private static final String[][] QSTR_OPTIONS = {
        { "default_operator", "\"OR\"", "\"AND\"" },
        { "lenient", "true", "false" },
        { "fuzziness", "\"AUTO\"", "1" },
        { "boost", "1.0", "2.5" },
    };

    /**
     * Option pool for kql() — from {@code @MapParam.MapParamEntry} on {@code Kql}.
     */
    private static final String[][] KQL_OPTIONS = {
        { "case_insensitive", "true", "false" },
        { "boost", "1.0", "2.5" },
    };

    /**
     * Option pool for multi_match() — from {@code @MapParam.MapParamEntry} on {@code MultiMatch}.
     */
    private static final String[][] MULTI_MATCH_OPTIONS = {
        { "operator", "\"AND\"", "\"OR\"" },
        { "lenient", "true", "false" },
        { "boost", "1.0", "2.5" },
        { "type", "\"best_fields\"", "\"most_fields\"", "\"phrase\"" },
    };

    /**
     * Generates a {@code match(field, "query")} expression, or its operator variant {@code field : "query"}.
     * {@code MatchOperator} extends {@code Match} — they share all constraints.
     * The operator form does not support options.
     */
    public static String matchFunction(List<Column> columns) {
        String field = EsqlQueryGenerator.randomName(columns, MATCH_FIELD_TYPES);
        if (field == null) {
            return null;
        }
        String query = randomQueryWord();
        if (randomBoolean()) {
            return field + " : \"" + query + "\"";
        }
        return "match(" + field + ", \"" + query + "\"" + maybeOptions(MATCH_OPTIONS) + ")";
    }

    /**
     * Generates a {@code match_phrase(field, "query")} expression.
     * field accepts: keyword, text only.
     * query must be a string literal.
     */
    public static String matchPhraseFunction(List<Column> columns) {
        String field = EsqlQueryGenerator.randomName(columns, MATCH_PHRASE_FIELD_TYPES);
        if (field == null) {
            return null;
        }
        String phrase = randomQueryWord() + " " + randomQueryWord();
        return "match_phrase(" + field + ", \"" + phrase + "\"" + maybeOptions(MATCH_PHRASE_OPTIONS) + ")";
    }

    /**
     * Generates a {@code qstr("field:query")} expression using Lucene query string syntax.
     * query is a string literal; no field argument.
     */
    public static String qstrFunction(List<Column> columns) {
        String field = EsqlQueryGenerator.randomName(columns, MATCH_PHRASE_FIELD_TYPES);
        String query;
        if (field != null && randomBoolean()) {
            // field-scoped query string: "field_name:value"
            String rawName = field.startsWith("`") ? field.substring(1, field.length() - 1) : field;
            query = rawName + ":" + randomQueryWord();
        } else {
            query = randomQueryWord();
        }
        return "qstr(\"" + query + "\"" + maybeOptions(QSTR_OPTIONS) + ")";
    }

    /**
     * Generates a {@code kql("field:query")} expression using KQL syntax.
     * query is a string literal; no field argument.
     */
    public static String kqlFunction(List<Column> columns) {
        String field = EsqlQueryGenerator.randomName(columns, MATCH_PHRASE_FIELD_TYPES);
        String query;
        if (field != null && randomBoolean()) {
            String rawName = field.startsWith("`") ? field.substring(1, field.length() - 1) : field;
            query = rawName + ": " + randomQueryWord();
        } else {
            query = randomQueryWord();
        }
        return "kql(\"" + query + "\"" + maybeOptions(KQL_OPTIONS) + ")";
    }

    /**
     * Generates a {@code multi_match("query", field1, field2 [, ...])} expression.
     * Fields accept the same types as match(). Query must be a string literal.
     */
    public static String multiMatchFunction(List<Column> columns) {
        List<String> fields = columns.stream()
            .filter(c -> MATCH_FIELD_TYPES.contains(c.type()))
            .map(c -> EsqlQueryGenerator.needsQuoting(c.name()) ? EsqlQueryGenerator.quote(c.name()) : c.name())
            .collect(Collectors.toList());
        if (fields.size() < 2) {
            return null;
        }
        int count = Math.min(fields.size(), randomIntBetween(2, 4));
        List<String> selected = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            selected.add(fields.get(randomIntBetween(0, fields.size() - 1)));
        }
        return "multi_match(\"" + randomQueryWord() + "\", " + String.join(", ", selected) + maybeOptions(MULTI_MATCH_OPTIONS) + ")";
    }

    /**
     * Generates a random full-text search boolean expression. Picks one of: match (including
     * its {@code :} operator variant), match_phrase, qstr, kql, or multi_match.
     * <p>
     * Respects two sets of constraints:
     * <ul>
     *   <li><b>Placement</b>: full-text functions are forbidden after LIMIT/STATS;
     *       QSTR and KQL additionally require all preceding commands to be FROM/WHERE/SORT.</li>
     *   <li><b>Field origin</b>: match, match_phrase, and multi_match
     *       require fields from the actual index mapping (FieldAttribute), not columns
     *       created by EVAL, GROK, DISSECT, etc.</li>
     * </ul>
     * Returns {@code null} when no valid function can be generated.
     */
    public static String fullTextFunction(List<Column> columns, List<CommandGenerator.CommandDescription> previousCommands) {
        if (isFullTextAllowed(previousCommands) == false) {
            return null;
        }

        boolean qstrKqlAllowed = isQstrKqlAllowed(previousCommands);

        List<Column> indexColumns = indexFieldColumns(columns, previousCommands);
        boolean fieldBasedAllowed = indexColumns != null && indexColumns.isEmpty() == false;

        if (fieldBasedAllowed && qstrKqlAllowed) {
            return switch (randomIntBetween(0, 4)) {
                case 0 -> matchFunction(indexColumns);
                case 1 -> matchPhraseFunction(indexColumns);
                case 2 -> qstrFunction(columns);
                case 3 -> kqlFunction(columns);
                default -> multiMatchFunction(indexColumns);
            };
        } else if (fieldBasedAllowed) {
            return switch (randomIntBetween(0, 2)) {
                case 0 -> matchFunction(indexColumns);
                case 1 -> matchPhraseFunction(indexColumns);
                default -> multiMatchFunction(indexColumns);
            };
        } else if (qstrKqlAllowed) {
            return randomBoolean() ? qstrFunction(columns) : kqlFunction(columns);
        } else {
            return null;
        }
    }

    // ========== TYPE-SAFE EXPRESSION GENERATORS ==========

    /**
     * Generates a random expression that is guaranteed to return one of the given accepted types.
     * This should be used when the expression will be passed as an argument to a function with
     * specific type constraints (e.g. top(), greatest/least, etc.).
     * <p>
     * Prefers generating a function expression wrapping a compatible field, but falls back
     * to a plain field reference if no function can be generated.
     *
     * @param columns the available columns
     * @param acceptedTypes the set of types the calling function accepts (e.g. {"integer", "long", "double", "keyword", "date"})
     * @param allowUnmapped if true, may use unmapped field names
     * @return an expression string whose output type is in acceptedTypes, or null if none can be generated
     */
    public static String typeSafeExpression(List<Column> columns, Set<String> acceptedTypes, boolean allowUnmapped) {
        // First try to generate a function expression with a known compatible return type
        if (randomIntBetween(0, 10) < 5) {
            String funcExpr = typeSafeFunctionExpression(columns, acceptedTypes, allowUnmapped);
            if (funcExpr != null) {
                return funcExpr;
            }
        }
        // Fall back to a direct field reference of a compatible type
        return EsqlQueryGenerator.randomName(columns, acceptedTypes);
    }

    /**
     * Generates a function expression whose return type is guaranteed to be in the accepted types set.
     * Each generator is mapped to its known return type category.
     *
     * @param columns the available columns
     * @param acceptedTypes types the consuming function accepts
     * @param allowUnmapped if true, may use unmapped field names
     * @return a function expression string with a compatible return type, or null
     */
    private static String typeSafeFunctionExpression(List<Column> columns, Set<String> acceptedTypes, boolean allowUnmapped) {
        boolean acceptsNumeric = acceptedTypes.contains("integer") || acceptedTypes.contains("long") || acceptedTypes.contains("double");
        boolean acceptsString = acceptedTypes.contains("keyword") || acceptedTypes.contains("text");
        boolean acceptsDate = acceptedTypes.contains("date") || acceptedTypes.contains("datetime");

        // Build a list of candidate generators that produce compatible types
        // Each entry is a generator function that returns an expression of the stated type
        ArrayList<Supplier<String>> candidates = new ArrayList<>();

        if (acceptsNumeric) {
            candidates.add(() -> mathFunction(columns, allowUnmapped));         // returns numeric
            candidates.add(() -> binaryMathFunction(columns, allowUnmapped));   // returns numeric
            candidates.add(() -> stringToIntFunction(columns, allowUnmapped));  // returns integer
            candidates.add(() -> clampFunction(columns, allowUnmapped));        // returns numeric
        }
        if (acceptsString) {
            candidates.add(() -> stringFunction(columns, allowUnmapped));       // returns string
            candidates.add(() -> concatFunction(columns, allowUnmapped));       // returns keyword
        }
        if (acceptsDate) {
            // date_trunc returns date, now() returns date
            String dateField = EsqlQueryGenerator.randomName(columns, Set.of("date", "datetime"));
            if (dateField != null) {
                String interval = randomFrom("1 day", "1 hour", "1 week", "1 month", "1 year");
                candidates.add(() -> "date_trunc(" + interval + ", " + dateField + ")");
                candidates.add(() -> "now()");
            }
        }

        if (candidates.isEmpty()) {
            return null;
        }

        // Try a few candidates (some may return null if no suitable fields exist)
        for (int attempt = 0; attempt < 3; attempt++) {
            String result = randomFrom(candidates).get();
            if (result != null) {
                return result;
            }
        }
        return null;
    }
}
