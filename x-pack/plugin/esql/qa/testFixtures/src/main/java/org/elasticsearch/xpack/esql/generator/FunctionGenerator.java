/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.List;
import java.util.Set;
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
     * Checks if unmapped fields are allowed based on the command history.
     * Unmapped fields are NOT allowed after STATS (not INLINE STATS), KEEP, or DROP commands
     * because these commands fix the field list.
     *
     * @param previousCommands the list of previous commands in the query
     * @return true if unmapped fields can be used, false otherwise
     */
    public static boolean areUnmappedFieldsAllowed(List<CommandGenerator.CommandDescription> previousCommands) {
        if (previousCommands == null) {
            return true;
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
     * Returns an unmapped field name with some probability, otherwise returns null.
     * Use this to occasionally inject unmapped fields into function arguments.
     *
     * @param allowUnmapped if false, always returns null (unmapped fields not allowed)
     */
    private static String maybeUnmappedField(boolean allowUnmapped) {
        if (!allowUnmapped) {
            return null;
        }
        return randomIntBetween(0, 100) < UNMAPPED_FIELD_PROBABILITY ? randomUnmappedFieldName() : null;
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
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String clampFunction(List<Column> columns, boolean allowUnmapped) {
        String numericField = fieldOrUnmapped(EsqlQueryGenerator.randomNumericField(columns), allowUnmapped);
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
            .map(Column::name)
            .limit(randomIntBetween(2, 4))
            .collect(Collectors.toList());
        if (stringFields.isEmpty()) {
            return null;
        }
        // Possibly add an unmapped field to the concat arguments
        if (allowUnmapped && randomIntBetween(0, 100) < UNMAPPED_FIELD_PROBABILITY) {
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
        String datePart = randomFrom("YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "DAY_OF_WEEK", "DAY_OF_YEAR", "WEEK");
        String interval = randomFrom("1 day", "1 hour", "1 week", "1 month", "1 year");
        return randomFrom(
            "date_extract(\"" + datePart + "\", " + dateField + ")",
            "date_trunc(" + dateField + ", " + interval + ")",
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
            .map(Column::name)
            .collect(Collectors.toList());
        // Possibly add an unmapped field
        if (allowUnmapped && randomIntBetween(0, 100) < UNMAPPED_FIELD_PROBABILITY) {
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

        // to_string - works on almost anything
        String anyField = EsqlQueryGenerator.randomName(columns);
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
            return randomFrom(
                "to_string(" + stringField + ")",
                "to_lower(" + stringField + ")"
            );
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
     * May randomly include unmapped field names to test NULL data type handling.
     * This is especially useful for coalesce since it's designed to handle nulls.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String coalesceFunction(List<Column> columns, boolean allowUnmapped) {
        // COALESCE requires all arguments to be the SAME type
        // Group columns by type and pick fields from the same type group
        var columnsByType = columns.stream()
            .collect(Collectors.groupingBy(Column::type));

        // Find a type that has at least one field
        String targetType = null;
        List<Column> sameTypeColumns = null;
        for (var entry : columnsByType.entrySet()) {
            if (entry.getValue().size() >= 1) {
                targetType = entry.getKey();
                sameTypeColumns = entry.getValue();
                break;
            }
        }

        if (sameTypeColumns == null || sameTypeColumns.isEmpty()) {
            return null;
        }

        String field1 = sameTypeColumns.get(randomIntBetween(0, sameTypeColumns.size() - 1)).name();

        // Coalesce is perfect for testing unmapped fields - it handles nulls by design
        // Use unmapped field as first argument (will be null, so second arg is returned)
        if (allowUnmapped && randomIntBetween(0, 100) < UNMAPPED_FIELD_PROBABILITY * 2) {
            String unmapped = randomUnmappedFieldName();
            return "coalesce(" + unmapped + ", " + field1 + ")";
        }

        // Pick a second field of the same type
        if (sameTypeColumns.size() >= 2) {
            String field2 = sameTypeColumns.get(randomIntBetween(0, sameTypeColumns.size() - 1)).name();
            if (!field1.equals(field2)) {
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
            .map(Column::name)
            .collect(Collectors.toList());

        // Possibly add an unmapped field (which has NULL type, accepted by these functions)
        if (allowUnmapped && randomIntBetween(0, 100) < UNMAPPED_FIELD_PROBABILITY && sameTypeFields.size() >= 1) {
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
        String anyField = fieldOrUnmapped(EsqlQueryGenerator.randomName(columns), allowUnmapped);
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
        String field = fieldOrUnmapped(EsqlQueryGenerator.randomName(columns), allowUnmapped);
        if (field == null) {
            return null;
        }
        int start = randomIntBetween(0, 3);
        int end = start + randomIntBetween(1, 5);
        return randomFrom(
            "mv_slice(" + field + ", " + start + ", " + end + ")",
            "mv_slice(" + field + ", " + start + ")"
        );
    }

    // ========== IP FUNCTIONS ==========

    /**
     * Generates an IP function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String ipFunction(List<Column> columns, boolean allowUnmapped) {
        String ipField = fieldOrUnmapped(EsqlQueryGenerator.randomName(columns, Set.of("ip")), allowUnmapped);
        if (ipField == null) {
            return null;
        }
        String cidr = randomFrom("10.0.0.0/8", "192.168.0.0/16", "172.16.0.0/12", "0.0.0.0/0");
        return randomFrom(
            "cidr_match(" + ipField + ", \"" + cidr + "\")",
            "ip_prefix(" + ipField + ", " + randomIntBetween(8, 32) + ", " + randomIntBetween(48, 128) + ")"
        );
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
        if (allowUnmapped && randomIntBetween(0, 100) < UNMAPPED_FIELD_PROBABILITY * 3) {
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

    // ========== COMBINED GENERATORS ==========

    /**
     * Generates a random scalar function expression that returns a value (not boolean).
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String randomScalarFunction(List<Column> columns, boolean allowUnmapped) {
        return switch (randomIntBetween(0, 13)) {
            case 0 -> mathFunction(columns, allowUnmapped);
            case 1 -> binaryMathFunction(columns, allowUnmapped);
            case 2 -> stringFunction(columns, allowUnmapped);
            case 3 -> stringToIntFunction(columns, allowUnmapped);
            case 4 -> dateFunction(columns, allowUnmapped);
            case 5 -> conversionFunction(columns, allowUnmapped);
            case 6 -> caseFunction(columns, allowUnmapped);
            case 7 -> coalesceFunction(columns, allowUnmapped);
            case 8 -> mvFunction(columns, allowUnmapped);
            case 9 -> concatFunction(columns, allowUnmapped);
            case 10 -> greatestLeastFunction(columns, allowUnmapped);
            case 11 -> splitFunction(columns, allowUnmapped);
            case 12 -> clampFunction(columns, allowUnmapped);
            default -> mvSliceZipFunction(columns, allowUnmapped);
        };
    }

    /**
     * Generates a random boolean expression using functions.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String randomBooleanFunction(List<Column> columns, boolean allowUnmapped) {
        return switch (randomIntBetween(0, 5)) {
            case 0 -> stringToBoolFunction(columns, allowUnmapped);
            case 1 -> isNullExpression(columns, allowUnmapped);
            case 2 -> inExpression(columns, allowUnmapped);
            case 3 -> likeExpression(columns, allowUnmapped);
            case 4 -> rlikeExpression(columns, allowUnmapped);
            default -> ipFunction(columns, allowUnmapped);
        };
    }
}
