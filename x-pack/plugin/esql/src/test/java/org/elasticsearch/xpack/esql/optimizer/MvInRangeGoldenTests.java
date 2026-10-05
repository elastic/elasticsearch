/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.EnumSet;

/**
 * Golden tests for {@code MV_IN_RANGE} Lucene pushdown shapes: exact YES (push, no filter),
 * RECHECK (push + filter), no push, and negated exact ({@code must_not(range)}).
 */
public class MvInRangeGoldenTests extends GoldenTestCase {
    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public MvInRangeGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION);

    /** Exact type: pushes a bare range and drops the FilterExec. */
    public void testExactInteger() {
        String query = """
                FROM employees
                | WHERE mv_in_range(salary, 25000, 30000)
            """;
        runGoldenTest(query, STAGES);
    }

    public void testExactLong() {
        String query = """
                FROM all_types
                | WHERE mv_in_range(long, 10::long, 20::long)
            """;
        runGoldenTest(query, STAGES);
    }

    public void testExactUnsignedLong() {
        String query = """
                FROM all_types
                | WHERE mv_in_range(unsigned_long, 10::unsigned_long, 20::unsigned_long)
            """;
        runGoldenTest(query, STAGES);
    }

    public void testExactDate() {
        String query = """
                FROM all_types
                | WHERE mv_in_range(date, "2020-01-01"::datetime, "2021-01-01"::datetime)
            """;
        runGoldenTest(query, STAGES);
    }

    public void testExactDateNanos() {
        String query = """
                FROM all_types
                | WHERE mv_in_range(date_nanos, "2020-01-01"::date_nanos, "2021-01-01"::date_nanos)
            """;
        runGoldenTest(query, STAGES);
    }

    /** Exact keyword: same YES pushdown as integral. */
    public void testExactKeyword() {
        String query = """
                FROM employees
                | WHERE mv_in_range(first_name, "A", "M")
            """;
        runGoldenTest(query, STAGES);
    }

    /** Exact ip: byte-faithful range, FilterExec dropped. */
    public void testExactIp() {
        String query = """
                FROM all_types
                | WHERE mv_in_range(ip, "1.1.1.1"::ip, "2.2.2.2"::ip)
            """;
        runGoldenTest(query, STAGES);
    }

    /** Exact version: byte-faithful range, FilterExec dropped. */
    public void testExactVersion() {
        String query = """
                FROM all_types
                | WHERE mv_in_range(version, "1.0.0"::version, "2.0.0"::version)
            """;
        runGoldenTest(query, STAGES);
    }

    /** NOT of an exact type: pushes must_not(range), no FilterExec. */
    public void testNotExact() {
        String query = """
                FROM employees
                | WHERE NOT mv_in_range(salary, 25000, 30000)
            """;
        runGoldenTest(query, STAGES);
    }

    /** DOUBLE family: pushes an inclusive-superset range but retains FilterExec (RECHECK). */
    public void testRecheckDouble() {
        String query = """
                FROM all_types
                | WHERE mv_in_range(double, 1.0, 2.0)
            """;
        runGoldenTest(query, STAGES);
    }

    /** float maps to DOUBLE: same RECHECK shape (push + filter). */
    public void testRecheckFloat() {
        String query = """
                FROM all_types
                | WHERE mv_in_range(float, 1.0, 2.0)
            """;
        runGoldenTest(query, STAGES);
    }

    public void testRecheckHalfFloat() {
        String query = """
                FROM all_types
                | WHERE mv_in_range(half_float, 1.0, 2.0)
            """;
        runGoldenTest(query, STAGES);
    }

    public void testRecheckScaledFloat() {
        String query = """
                FROM all_types
                | WHERE mv_in_range(scaled_float, 1.0, 2.0)
            """;
        runGoldenTest(query, STAGES);
    }

    /** NOT of a RECHECK type cannot push: FilterExec stays, no Lucene range. */
    public void testNotRecheckNotPushed() {
        String query = """
                FROM all_types
                | WHERE NOT mv_in_range(double, 1.0, 2.0)
            """;
        runGoldenTest(query, STAGES);
    }

    /** Text is never pushed: analyzed-token range is not a whole-value comparison. */
    public void testTextNotPushed() {
        String query = """
                FROM all_types
                | WHERE mv_in_range(text, "a", "z")
            """;
        runGoldenTest(query, STAGES);
    }

    /** Multivalued bound has no well-defined range: stays entirely in the FilterExec. */
    public void testMultivaluedBoundNotPushed() {
        String query = """
                FROM employees
                | WHERE mv_in_range(salary, [25000, 26000], 30000)
            """;
        runGoldenTest(query, STAGES);
    }

    /** Exact type with exclusive bounds: inclusivity flags push into the Lucene range. */
    public void testExactExclusiveBounds() {
        String query = """
                FROM employees
                | WHERE mv_in_range(salary, 25000, 30000, {"include_lower": false, "include_upper": false})
            """;
        runGoldenTest(query, STAGES);
    }

    /** RECHECK exclusive bounds: Lucene range stays inclusive; exclusivity stays in the retained filter. */
    public void testRecheckExclusiveBounds() {
        String query = """
                FROM all_types
                | WHERE mv_in_range(double, 0.0, 1.0, {"include_lower": false, "include_upper": false})
            """;
        runGoldenTest(query, STAGES);
    }
}
