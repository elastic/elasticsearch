/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class ValidationResultTests extends ESTestCase {

    public void testSuccess_ReturnsResultWithCorrectState() {
        var value = "result-value";
        var result = ValidationResult.success(value);

        assertTrue(result.isSuccess());
        assertFalse(result.isFailed());
        assertFalse(result.isUndefined());
        assertThat(result.result(), is(value));
    }

    public void testSuccess_ThrowsWhenResultIsNull() {
        expectThrows(NullPointerException.class, () -> ValidationResult.success(null));
    }

    public void testFailed_ReturnsResultWithCorrectState() {
        var result = ValidationResult.<String>failed();

        assertTrue(result.isFailed());
        assertFalse(result.isSuccess());
        assertFalse(result.isUndefined());
        assertNull(result.result());
    }

    public void testFailed_ReturnsSameInstanceForSameType() {
        var failed1 = ValidationResult.<String>failed();
        var failed2 = ValidationResult.<String>failed();
        assertSame(failed1, failed2);
    }

    public void testUndefined_ReturnsResultWithCorrectState() {
        var result = ValidationResult.<String>undefined();

        assertTrue(result.isUndefined());
        assertFalse(result.isSuccess());
        assertFalse(result.isFailed());
        assertNull(result.result());
    }

    public void testUndefined_ReturnsSameInstanceForSameType() {
        var undefined1 = ValidationResult.<String>undefined();
        var undefined2 = ValidationResult.<String>undefined();
        assertSame(undefined1, undefined2);
    }
}
