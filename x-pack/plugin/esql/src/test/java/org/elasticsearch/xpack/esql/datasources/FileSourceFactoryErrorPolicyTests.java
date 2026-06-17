/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileSourceFactoryErrorPolicyTests extends ESTestCase {

    public void testResolveMaxErrorsFromConfig() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        ErrorPolicy policy = FileSourceFactory.resolveErrorPolicy(Map.of("max_errors", "100"), reader);
        assertEquals(100, policy.maxErrors());
        assertEquals(ErrorPolicy.Mode.SKIP_ROW, policy.mode());
        assertTrue(policy.logErrors());
    }

    public void testResolveMaxErrorRatioFromConfig() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        ErrorPolicy policy = FileSourceFactory.resolveErrorPolicy(Map.of("max_error_ratio", "0.1"), reader);
        assertEquals(Long.MAX_VALUE, policy.maxErrors());
        assertEquals(0.1, policy.maxErrorRatio(), 0.001);
        assertTrue(policy.logErrors());
    }

    public void testResolveBothMaxErrorsAndRatio() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        ErrorPolicy policy = FileSourceFactory.resolveErrorPolicy(Map.of("max_errors", "50", "max_error_ratio", "0.2"), reader);
        assertEquals(50, policy.maxErrors());
        assertEquals(0.2, policy.maxErrorRatio(), 0.001);
    }

    public void testResolveZeroMaxErrors() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        ErrorPolicy policy = FileSourceFactory.resolveErrorPolicy(Map.of("max_errors", "0"), reader);
        assertEquals(0, policy.maxErrors());
        assertEquals(ErrorPolicy.Mode.SKIP_ROW, policy.mode());
    }

    public void testResolveFallsBackToFormatDefault() {
        FormatReader reader = mock(FormatReader.class);
        ErrorPolicy customDefault = new ErrorPolicy(50, false);
        when(reader.defaultErrorPolicy()).thenReturn(customDefault);

        ErrorPolicy policy = FileSourceFactory.resolveErrorPolicy(Map.of(), reader);
        assertSame(customDefault, policy);
    }

    public void testResolveNullConfig() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        ErrorPolicy policy = FileSourceFactory.resolveErrorPolicy(null, reader);
        assertSame(ErrorPolicy.STRICT, policy);
    }

    public void testResolveInvalidMaxErrors() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        expectThrows(
            IllegalArgumentException.class,
            () -> FileSourceFactory.resolveErrorPolicy(Map.of("max_errors", "not_a_number"), reader)
        );
    }

    public void testResolveInvalidMaxErrorRatio() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        expectThrows(
            IllegalArgumentException.class,
            () -> FileSourceFactory.resolveErrorPolicy(Map.of("max_error_ratio", "not_a_number"), reader)
        );
    }

    public void testResolveNullFieldMode() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        ErrorPolicy policy = FileSourceFactory.resolveErrorPolicy(Map.of("error_mode", "null_field", "max_errors", "50"), reader);
        assertEquals(ErrorPolicy.Mode.NULL_FIELD, policy.mode());
        assertEquals(50, policy.maxErrors());
        assertTrue(policy.isPermissive());
    }

    public void testResolveSkipRowMode() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        ErrorPolicy policy = FileSourceFactory.resolveErrorPolicy(Map.of("error_mode", "skip_row", "max_errors", "10"), reader);
        assertEquals(ErrorPolicy.Mode.SKIP_ROW, policy.mode());
        assertEquals(10, policy.maxErrors());
    }

    public void testResolveFailFastMode() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        ErrorPolicy policy = FileSourceFactory.resolveErrorPolicy(Map.of("error_mode", "fail_fast"), reader);
        assertSame(ErrorPolicy.STRICT, policy);
    }

    public void testResolveFailFastWithBudgetThrows() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        expectThrows(
            IllegalArgumentException.class,
            () -> FileSourceFactory.resolveErrorPolicy(Map.of("error_mode", "fail_fast", "max_errors", "10"), reader)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> FileSourceFactory.resolveErrorPolicy(Map.of("error_mode", "fail_fast", "max_error_ratio", "0.1"), reader)
        );
    }

    public void testResolveInvalidErrorMode() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        expectThrows(
            IllegalArgumentException.class,
            () -> FileSourceFactory.resolveErrorPolicy(Map.of("error_mode", "invalid_mode"), reader)
        );
    }

    public void testResolveEmptyErrorModeThrows() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        expectThrows(IllegalArgumentException.class, () -> FileSourceFactory.resolveErrorPolicy(Map.of("error_mode", ""), reader));
    }

    public void testResolveNullFieldModeWithoutBudgetDefaultsToUnlimited() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        ErrorPolicy policy = FileSourceFactory.resolveErrorPolicy(Map.of("error_mode", "null_field"), reader);
        assertEquals(ErrorPolicy.Mode.NULL_FIELD, policy.mode());
        assertEquals(Long.MAX_VALUE, policy.maxErrors());
        assertFalse(policy.logErrors());
    }

    public void testResolveSkipRowModeWithoutBudgetDefaultsToUnlimited() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        ErrorPolicy policy = FileSourceFactory.resolveErrorPolicy(Map.of("error_mode", "skip_row"), reader);
        assertEquals(ErrorPolicy.Mode.SKIP_ROW, policy.mode());
        assertEquals(Long.MAX_VALUE, policy.maxErrors());
        assertFalse(policy.logErrors());
    }

    public void testLogErrorsEnabledWhenBudgetIsFinite() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        ErrorPolicy withCount = FileSourceFactory.resolveErrorPolicy(Map.of("max_errors", "10"), reader);
        assertTrue(withCount.logErrors());

        ErrorPolicy withRatio = FileSourceFactory.resolveErrorPolicy(Map.of("max_error_ratio", "0.05"), reader);
        assertTrue(withRatio.logErrors());
    }

    public void testLogErrorsDisabledWhenBudgetIsUnlimited() {
        FormatReader reader = mock(FormatReader.class);
        when(reader.defaultErrorPolicy()).thenReturn(ErrorPolicy.STRICT);

        ErrorPolicy policy = FileSourceFactory.resolveErrorPolicy(Map.of("error_mode", "skip_row"), reader);
        assertFalse(policy.logErrors());
    }
}
