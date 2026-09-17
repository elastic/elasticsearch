/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class ErrorPolicyTests extends ESTestCase {

    public void testStrictPolicy() {
        ErrorPolicy strict = ErrorPolicy.STRICT;
        assertEquals(0, strict.maxErrors());
        assertEquals(0.0, strict.maxErrorRatio(), 0.0);
        assertFalse(strict.logErrors());
        assertTrue(strict.isStrict());
    }

    public void testLenientPolicy() {
        ErrorPolicy lenient = ErrorPolicy.LENIENT;
        assertEquals(Long.MAX_VALUE, lenient.maxErrors());
        assertEquals(1.0, lenient.maxErrorRatio(), 0.0);
        assertTrue(lenient.logErrors());
        assertFalse(lenient.isStrict());
    }

    public void testCustomPolicy() {
        ErrorPolicy custom = new ErrorPolicy(100, 0.1, true);
        assertEquals(100, custom.maxErrors());
        assertEquals(0.1, custom.maxErrorRatio(), 0.001);
        assertTrue(custom.logErrors());
        assertFalse(custom.isStrict());
    }

    public void testConvenienceConstructor() {
        ErrorPolicy simple = new ErrorPolicy(50, true);
        assertEquals(50, simple.maxErrors());
        assertEquals(0.0, simple.maxErrorRatio(), 0.0);
        assertTrue(simple.logErrors());
    }

    public void testZeroMaxErrorsIsNotStrictInSkipRowMode() {
        ErrorPolicy zeroErrors = new ErrorPolicy(0, true);
        assertFalse(zeroErrors.isStrict());
        assertEquals(ErrorPolicy.Mode.SKIP_ROW, zeroErrors.mode());
    }

    public void testFailFastModeIsStrict() {
        ErrorPolicy failFast = new ErrorPolicy(ErrorPolicy.Mode.FAIL_FAST, 0, 0.0, false);
        assertTrue(failFast.isStrict());
    }

    public void testIsStrictOnlyChecksMode() {
        ErrorPolicy failFastWithBudget = new ErrorPolicy(ErrorPolicy.Mode.FAIL_FAST, 100, 0.5, true);
        assertTrue(failFastWithBudget.isStrict());
    }

    public void testNegativeMaxErrorsThrows() {
        expectThrows(IllegalArgumentException.class, () -> new ErrorPolicy(-1, false));
    }

    public void testInvalidRatioThrows() {
        expectThrows(IllegalArgumentException.class, () -> new ErrorPolicy(0, -0.1, false));
        expectThrows(IllegalArgumentException.class, () -> new ErrorPolicy(0, 1.1, false));
    }

    public void testBudgetExceededByCount() {
        ErrorPolicy policy = new ErrorPolicy(5, false);
        assertFalse(policy.isBudgetExceeded(5, 100));
        assertTrue(policy.isBudgetExceeded(6, 100));
    }

    public void testBudgetExceededByRatio() {
        ErrorPolicy policy = new ErrorPolicy(Long.MAX_VALUE, 0.1, false);
        assertFalse(policy.isBudgetExceeded(1, 100));
        assertTrue(policy.isBudgetExceeded(11, 100));
        assertTrue(policy.isBudgetExceeded(2, 10));
    }

    public void testBudgetExceededByEitherCountOrRatio() {
        ErrorPolicy policy = new ErrorPolicy(5, 0.5, false);
        assertTrue(policy.isBudgetExceeded(6, 100));
        assertTrue(policy.isBudgetExceeded(3, 5));
        assertFalse(policy.isBudgetExceeded(2, 100));
    }

    public void testBudgetNotExceededWithZeroRows() {
        ErrorPolicy policy = new ErrorPolicy(Long.MAX_VALUE, 0.1, false);
        assertFalse(policy.isBudgetExceeded(0, 0));
    }

    public void testNullFieldMode() {
        ErrorPolicy nullField = new ErrorPolicy(ErrorPolicy.Mode.NULL_FIELD, 100, 0.0, true);
        assertTrue(nullField.isPermissive());
        assertFalse(nullField.isStrict());
        assertEquals(ErrorPolicy.Mode.NULL_FIELD, nullField.mode());
    }

    public void testModeCannotBeNull() {
        expectThrows(IllegalArgumentException.class, () -> new ErrorPolicy(null, 0, 0.0, false));
    }

    public void testModeParsing() {
        assertEquals(ErrorPolicy.Mode.FAIL_FAST, ErrorPolicy.Mode.parse("FAIL_FAST"));
        assertEquals(ErrorPolicy.Mode.FAIL_FAST, ErrorPolicy.Mode.parse("fail_fast"));
        assertEquals(ErrorPolicy.Mode.SKIP_ROW, ErrorPolicy.Mode.parse("SKIP_ROW"));
        assertEquals(ErrorPolicy.Mode.SKIP_ROW, ErrorPolicy.Mode.parse("skip_row"));
        assertEquals(ErrorPolicy.Mode.NULL_FIELD, ErrorPolicy.Mode.parse("null_field"));
        assertEquals(ErrorPolicy.Mode.NULL_FIELD, ErrorPolicy.Mode.parse("NULL_FIELD"));
        assertNull(ErrorPolicy.Mode.parse(null));
        assertNull(ErrorPolicy.Mode.parse(""));
    }

    public void testModeParsingInvalid() {
        expectThrows(IllegalArgumentException.class, () -> ErrorPolicy.Mode.parse("invalid"));
    }

    public void testValidateRegistrationBudgetRequiresMode() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> ErrorPolicy.validateRegistrationBudget(Map.of(ErrorPolicy.CONFIG_MAX_ERRORS, "100"))
        );
        assertThat(ex.getMessage(), containsString(ErrorPolicy.CONFIG_ERROR_MODE));
        assertThat(ex.getMessage(), containsString("skip_row"));
        assertThat(ex.getMessage(), containsString("null_field"));

        IllegalArgumentException ex2 = expectThrows(
            IllegalArgumentException.class,
            () -> ErrorPolicy.validateRegistrationBudget(Map.of(ErrorPolicy.CONFIG_MAX_ERROR_RATIO, "0.1"))
        );
        assertThat(ex2.getMessage(), containsString(ErrorPolicy.CONFIG_ERROR_MODE));
        assertThat(ex2.getMessage(), containsString("skip_row"));
        assertThat(ex2.getMessage(), containsString("null_field"));
    }

    public void testValidateRegistrationBudgetAllowsExplicitMode() {
        ErrorPolicy.validateRegistrationBudget(Map.of(ErrorPolicy.CONFIG_MAX_ERRORS, "100", ErrorPolicy.CONFIG_ERROR_MODE, "skip_row"));
        ErrorPolicy.validateRegistrationBudget(Map.of(ErrorPolicy.CONFIG_MAX_ERRORS, "50", ErrorPolicy.CONFIG_ERROR_MODE, "null_field"));
        ErrorPolicy.validateRegistrationBudget(Map.of(ErrorPolicy.CONFIG_ERROR_MODE, "fail_fast"));
        ErrorPolicy.validateRegistrationBudget(Map.of());
        ErrorPolicy.validateRegistrationBudget(null);
    }
}
