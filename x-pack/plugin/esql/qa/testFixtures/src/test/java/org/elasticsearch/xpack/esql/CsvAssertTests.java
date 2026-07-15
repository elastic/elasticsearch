/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.xpack.esql.CsvTestUtils.Type.DOUBLE;
import static org.elasticsearch.xpack.esql.CsvTestUtils.Type.KEYWORD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for the {@code zero_threshold:} csv-spec directive's clamping behavior in
 * {@code CsvAssert}'s {@code ValueTransformer}. See {@link CsvSpecReader.ZeroThreshold} for the
 * full rationale: {@code CHANGE_POINT} p-values are computed from order-sensitive floating-point
 * summation, so two independently-computed vanishingly-small p-values can differ by several
 * orders of magnitude across node topologies while both remaining, for any practical purpose,
 * zero. These tests exercise the comparison directly (rather than through a full csv-spec test)
 * to pin the clamping semantics: both below threshold compare equal, only one below threshold
 * still fails, and the directive has no effect when absent.
 */
public class CsvAssertTests extends ESTestCase {

    private static CsvTestUtils.ExpectedResults results(Object value) {
        return new CsvTestUtils.ExpectedResults(List.of("pvalue"), List.of(DOUBLE), List.of(List.of(value)));
    }

    private static List<List<Object>> actual(Object value) {
        return List.of(List.of(value));
    }

    public void testBothBelowThresholdCompareEqualDespiteDifferingMagnitude() {
        // 9.678892E-24 vs 4.762904E-21: several orders of magnitude apart, both effectively zero.
        CsvAssert.assertDataWithValueConverter(results(9.678892E-24), actual(4.762904E-21), false, false, false, 1e-19, null);
    }

    public void testExactZeroVsDeepUnderflowCompareEqualBelowThreshold() {
        // 0.0 vs 6.754801E-159: the other documented failure shape from CI.
        CsvAssert.assertDataWithValueConverter(results(0.0), actual(6.754801E-159), false, false, false, 1e-90, null);
    }

    public void testOnlyOneBelowThresholdStillFails() {
        // 1e-25 is below the threshold (clamped to 0.0), but 1e-2 is not: must not compare equal.
        AssertionError e = expectThrows(
            AssertionError.class,
            () -> CsvAssert.assertDataWithValueConverter(results(1e-25), actual(1e-2), false, false, false, 1e-19, null)
        );
        assertThat(e.getMessage(), containsString("Data mismatch"));
    }

    public void testNeitherBelowThresholdComparesNormally() {
        // Both values are above the threshold, so the usual (strict) double comparison applies and a
        // genuine difference must still fail.
        AssertionError e = expectThrows(
            AssertionError.class,
            () -> CsvAssert.assertDataWithValueConverter(results(0.5), actual(0.6), false, false, false, 1e-19, null)
        );
        assertThat(e.getMessage(), containsString("Data mismatch"));
    }

    public void testNullThresholdLeavesComparisonUnchanged() {
        // With no zero_threshold directive (null), two vanishingly-small-but-different p-values must
        // still fail: this is exactly the flake the directive exists to opt into tolerating.
        AssertionError e = expectThrows(
            AssertionError.class,
            () -> CsvAssert.assertDataWithValueConverter(results(9.678892E-24), actual(4.762904E-21), false, false, false, null, null)
        );
        assertThat(e.getMessage(), containsString("Data mismatch"));
    }

    public void testNullThresholdStillAllowsExactMatch() {
        CsvAssert.assertDataWithValueConverter(results(1.5), actual(1.5), false, false, false, null, null);
    }

    public void testNonDoubleColumnsUnaffectedByThreshold() {
        CsvTestUtils.ExpectedResults expected = new CsvTestUtils.ExpectedResults(
            List.of("type"),
            List.of(KEYWORD),
            List.of(List.of("dip"))
        );
        CsvAssert.assertDataWithValueConverter(expected, List.of(List.of("dip")), false, false, false, 1e-19, null);
    }
}
