/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * Registry of {@link SpecialFunctionGenerator} instances, keyed by ES|QL function name.
 * <p>
 * Functions listed here are <em>not</em> added to
 * {@link org.elasticsearch.xpack.esql.generator.GenerativeFunctionCatalog#EXCLUDED_FUNCTIONS}:
 * the composite generator calls their custom generator instead of the generic recursive builder.
 *
 * <p><b>Adding a new entry</b>
 * Implement a {@link SpecialFunctionGenerator} lambda in {@link #buildRegistry()} and put it
 * under the function's JSON name.  Use {@code recurse.recurse(type, cols, unmapped, depth - 1)}
 * for parameters that accept arbitrary expressions, and emit a literal directly for parameters
 * that require a compile-time constant or a value from a fixed vocabulary.
 */
public final class SpecialFunctionGeneratorRegistry {

    // --- fixed literal sets --------------------------------------------------

    /** Single-byte strings safe to use as {@code split} delimiters. */
    private static final String[] SPLIT_DELIMITERS = { ",", " ", ".", "-", "/", "|", ":", "_" };

    /** Algorithm names accepted by the {@code hash} function. */
    private static final String[] HASH_ALGORITHMS = { "MD5", "SHA-256", "SHA-384", "SHA-512" };

    /** Time-unit strings accepted by {@code date_diff} and {@code date_unit_count}. */
    private static final String[] DATE_DIFF_UNITS = { "second", "minute", "hour", "day", "week", "month", "year" };

    /** Date-part names accepted by {@code date_extract}. */
    private static final String[] DATE_EXTRACT_PARTS = {
        "YEAR",
        "MONTH_OF_YEAR",
        "DAY_OF_MONTH",
        "HOUR_OF_DAY",
        "MINUTE_OF_HOUR",
        "SECOND_OF_MINUTE",
        "DAY_OF_WEEK",
        "DAY_OF_YEAR",
        "ALIGNED_WEEK_OF_YEAR" };

    /** Positive bucket sizes for numeric {@code round_to} calls. */
    private static final double[] ROUND_TO_BUCKETS = { 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0 };

    // --- registry ------------------------------------------------------------

    private static final Map<String, SpecialFunctionGenerator> REGISTRY = buildRegistry();

    private static Map<String, SpecialFunctionGenerator> buildRegistry() {
        Map<String, SpecialFunctionGenerator> r = new HashMap<>();

        // ---- String functions -----------------------------------------------

        // space(n): n must be non-negative. Space.toEvaluator() calls checkNumber() eagerly
        // for foldable inputs, throwing IllegalArgumentException before the @Evaluator(warnExceptions)
        // mechanism can catch it and return null. Always use a non-negative literal.
        r.put("space", (name, sig, cols, unmapped, depth, recurse) -> name + "(" + randomIntBetween(0, 10) + ")");

        // split(string, delim): delimiter must be a single byte at runtime.
        // SplitVariableEvaluator throws InvalidArgumentException for multi-byte delimiters
        // instead of returning null, so we always pass a literal single-char delimiter.
        r.put("split", (name, sig, cols, unmapped, depth, recurse) -> {
            String str = recurse.recurse(sig.params().get(0).type(), cols, unmapped, depth - 1);
            if (str == null) return null;
            return name + "(" + str + ", \"" + randomFrom(SPLIT_DELIMITERS) + "\")";
        });

        // hash(algorithm, input): first argument must be a literal algorithm name.
        r.put("hash", (name, sig, cols, unmapped, depth, recurse) -> {
            String input = recurse.recurse(sig.params().get(1).type(), cols, unmapped, depth - 1);
            if (input == null) return null;
            return name + "(\"" + randomFrom(HASH_ALGORITHMS) + "\", " + input + ")";
        });

        // ---- Date functions -------------------------------------------------

        // date_diff(unit, startTimestamp, endTimestamp): unit must be a literal.
        r.put("date_diff", (name, sig, cols, unmapped, depth, recurse) -> {
            String start = recurse.recurse(sig.params().get(1).type(), cols, unmapped, depth - 1);
            if (start == null) return null;
            String end = recurse.recurse(sig.params().get(2).type(), cols, unmapped, depth - 1);
            if (end == null) return null;
            return name + "(\"" + randomFrom(DATE_DIFF_UNITS) + "\", " + start + ", " + end + ")";
        });

        // date_extract(datePart, date): datePart must be a literal.
        r.put("date_extract", (name, sig, cols, unmapped, depth, recurse) -> {
            String date = recurse.recurse(sig.params().get(1).type(), cols, unmapped, depth - 1);
            if (date == null) return null;
            return name + "(\"" + randomFrom(DATE_EXTRACT_PARTS) + "\", " + date + ")";
        });

        // date_unit_count(to_unit, from_unit, date): both unit params must be literals.
        r.put("date_unit_count", (name, sig, cols, unmapped, depth, recurse) -> {
            String date = recurse.recurse(sig.params().get(2).type(), cols, unmapped, depth - 1);
            if (date == null) return null;
            return name + "(\"" + randomFrom(DATE_DIFF_UNITS) + "\", \"" + randomFrom(DATE_DIFF_UNITS) + "\", " + date + ")";
        });

        // ---- MV functions ---------------------------------------------------

        // mv_slice(field, start[, end]): start and end must be integer literals.
        // MvSlice.toEvaluator calls stringToInt("null") when a foldable expression evaluates
        // to null, crashing instead of returning null gracefully (ES|QL bug).
        // Using only non-negative indices (count-from-start) avoids the sign-constraint check.
        r.put("mv_slice", (name, sig, cols, unmapped, depth, recurse) -> {
            String field = recurse.recurse(sig.params().get(0).type(), cols, unmapped, depth - 1);
            if (field == null) return null;
            int start = randomIntBetween(0, 3);
            int end = start + randomIntBetween(1, 5);
            return randomBoolean() ? name + "(" + field + ", " + start + ", " + end + ")" : name + "(" + field + ", " + start + ")";
        });

        // mv_percentile(field, percentile): percentile must be in [0, 100].
        // Using a literal keeps the value in range and avoids runtime validation errors.
        r.put("mv_percentile", (name, sig, cols, unmapped, depth, recurse) -> {
            String field = recurse.recurse(sig.params().get(0).type(), cols, unmapped, depth - 1);
            if (field == null) return null;
            return name + "(" + field + ", " + randomIntBetween(0, 100) + ")";
        });

        // ---- Math functions -------------------------------------------------

        // round_to(field, bucket): bucket must be a foldable constant.
        // Skip date/date_nanos and long-bucket overloads — date literals are complex to synthesise,
        // and long integer literals in ES|QL require explicit casts that are hard to produce cleanly.
        // For integer-bucket overloads, emit an integer literal; for double-bucket, emit a double
        // literal. Keeping the bucket type consistent with the chosen signature prevents ES|QL from
        // matching a different overload (e.g., round_to(integer_col, 1.0) resolves as
        // (integer, double)->double even when the generator picked (integer, integer)->integer).
        r.put("round_to", (name, sig, cols, unmapped, depth, recurse) -> {
            String bucketType = sig.params().get(1).type();
            if ("date".equals(bucketType) || "date_nanos".equals(bucketType) || "long".equals(bucketType)) {
                return null;
            }
            String field = recurse.recurse(sig.params().get(0).type(), cols, unmapped, depth - 1);
            if (field == null) return null;
            String bucket;
            if ("integer".equals(bucketType)) {
                bucket = String.valueOf(randomIntBetween(1, 100));
            } else {
                // double
                bucket = String.valueOf(ROUND_TO_BUCKETS[randomIntBetween(0, ROUND_TO_BUCKETS.length - 1)]);
            }
            return name + "(" + field + ", " + bucket + ")";
        });

        // ---- Date functions (cont.) -----------------------------------------

        // date_format([format,] date): format must be a literal date-format string.
        // The 1-param overload just needs a date expression; the 2-param overload adds a
        // format pattern. An arbitrary keyword expression would likely be an invalid format
        // and cause a runtime error instead of returning null.
        r.put("date_format", (name, sig, cols, unmapped, depth, recurse) -> {
            if (sig.params().size() == 1) {
                String dateExpr = recurse.recurse(sig.params().get(0).type(), cols, unmapped, depth - 1);
                if (dateExpr == null) return null;
                return name + "(" + dateExpr + ")";
            }
            // 2-param: (keyword/text format, date/date_nanos)
            String dateExpr = recurse.recurse(sig.params().get(1).type(), cols, unmapped, depth - 1);
            if (dateExpr == null) return null;
            return name + "(\"yyyy-MM-dd\", " + dateExpr + ")";
        });

        // ---- Scoring functions ----------------------------------------------

        // decay(value, origin, scale[, options]): origin and scale must be compile-time constants.
        // Numeric overloads only: double uses 0.0/1.0, integer uses 0/1.
        // Skip long/unsigned_long (literals need explicit casts) and all geo/date/keyword overloads
        // whose other required params (geo_point, time_duration, etc.) are already unsupported.
        r.put("decay", (name, sig, cols, unmapped, depth, recurse) -> {
            String originType = sig.params().get(1).type();
            String valueLiteral;
            String scaleLiteral;
            if ("double".equals(originType)) {
                valueLiteral = "0.0";
                scaleLiteral = "1.0";
            } else if ("integer".equals(originType)) {
                valueLiteral = "0";
                scaleLiteral = "1";
            } else {
                return null;
            }
            String valueExpr = recurse.recurse(sig.params().get(0).type(), cols, unmapped, depth - 1);
            if (valueExpr == null) return null;
            return name + "(" + valueExpr + ", " + valueLiteral + ", " + scaleLiteral + ")";
        });

        // ---- IP functions ---------------------------------------------------

        // ip_prefix(ip, ipv4_prefix_length, ipv6_prefix_length): both prefix lengths must be
        // integer literals in valid ranges. IpPrefix validates them eagerly instead of returning
        // null, so arbitrary expressions would produce runtime errors.
        r.put("ip_prefix", (name, sig, cols, unmapped, depth, recurse) -> {
            String ipField = recurse.recurse("ip", cols, unmapped, depth - 1);
            if (ipField == null) return null;
            return name + "(" + ipField + ", " + randomIntBetween(8, 32) + ", " + randomIntBetween(48, 128) + ")";
        });

        // ---- Math functions -------------------------------------------------

        // scalb(field, exponent): exponent must be an integer literal; unconstrained values
        // cause overflow/underflow to infinity. Skip long-exponent overloads.
        r.put("scalb", (name, sig, cols, unmapped, depth, recurse) -> {
            if ("long".equals(sig.params().get(1).type())) return null;
            String field = recurse.recurse(sig.params().get(0).type(), cols, unmapped, depth - 1);
            if (field == null) return null;
            return name + "(" + field + ", " + randomIntBetween(-5, 5) + ")";
        });

        // pow(base, exponent): unconstrained exponents cause double overflow to infinity.
        // Use only integer-exponent overloads with small positive literals; skip double/long/
        // unsigned_long exponent overloads.
        r.put("pow", (name, sig, cols, unmapped, depth, recurse) -> {
            String expType = sig.params().get(1).type();
            if ("double".equals(expType) || "long".equals(expType) || "unsigned_long".equals(expType)) {
                return null;
            }
            String base = recurse.recurse(sig.params().get(0).type(), cols, unmapped, depth - 1);
            if (base == null) return null;
            return name + "(" + base + ", " + randomIntBetween(1, 4) + ")";
        });

        // ---- Conditional functions -------------------------------------------

        // case(condition, trueValue[, elseValue]): the generic recursive builder always stops
        // before optional params, so composite-generated CASE calls never got an elseValue, and
        // the condition was always a single leaf expression, never an AND/OR of two conditions.
        // That combination — a constant_keyword equality ANDed with another condition, feeding
        // CASE(cond, field, null) — is exactly what hid the bug fixed by
        // https://github.com/elastic/elasticsearch/pull/149752. Give both a chance to happen so
        // generative tests can reproduce that bug class. See
        // https://github.com/elastic/elasticsearch/issues/150055
        r.put("case", (name, sig, cols, unmapped, depth, recurse) -> {
            String condition = recurse.recurse("boolean", cols, unmapped, depth - 1);
            if (condition == null) return null;
            if (randomBoolean()) {
                String secondCondition = recurse.recurse("boolean", cols, unmapped, depth - 1);
                if (secondCondition != null) {
                    condition += (randomBoolean() ? " AND " : " OR ") + secondCondition;
                }
            }
            String trueValue = recurse.recurse(sig.params().get(1).type(), cols, unmapped, depth - 1);
            if (trueValue == null) return null;
            StringBuilder call = new StringBuilder(name).append("(").append(condition).append(", ").append(trueValue);
            if (sig.params().size() > 2 && randomBoolean()) {
                if (randomBoolean()) {
                    // the literal, untyped null else-value that triggered #149752
                    call.append(", null");
                } else {
                    String elseValue = recurse.recurse(sig.params().get(2).type(), cols, unmapped, depth - 1);
                    if (elseValue != null) {
                        call.append(", ").append(elseValue);
                    }
                }
            }
            return call.append(")").toString();
        });

        return Collections.unmodifiableMap(r);
    }

    /**
     * Returns the special generator registered for {@code functionName},
     * or {@code null} if no special handling is needed.
     */
    public static SpecialFunctionGenerator forFunction(String functionName) {
        return REGISTRY.get(functionName);
    }

    private SpecialFunctionGeneratorRegistry() {}
}
