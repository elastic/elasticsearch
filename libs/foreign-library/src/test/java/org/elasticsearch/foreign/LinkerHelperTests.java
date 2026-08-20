/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import junit.framework.TestCase;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Locale;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BOOLEAN;
import static java.lang.foreign.ValueLayout.JAVA_INT;

/**
 * Tests for {@link LinkerHelper#systemError()} and {@link LinkerHelper#downcallHandleWithSystemError}.
 *
 * <p>These tests use JUnit 3 ({@code junit.framework.TestCase}) directly rather than
 * {@code ESTestCase} to keep the foreign-library module's test dependencies minimal.
 */
public class LinkerHelperTests extends TestCase {

    /**
     * On POSIX, {@link LinkerHelper#systemError()} returns the {@code errno} value captured by a
     * {@code downcallHandleWithSystemError} call. {@code close(-1)} always fails with {@code EBADF}
     * (9) on POSIX platforms, making it a self-contained way to force a known errno value.
     */
    public void testSystemErrorReturnsCapturedErrnoAfterFailedCall() throws Throwable {
        if (System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows")) {
            // errno semantics for this symbol are POSIX-specific; skip on Windows.
            return;
        }

        MethodHandle close = LinkerHelper.downcallHandleWithSystemError("close", FunctionDescriptor.of(JAVA_INT, JAVA_INT));
        int result = (int) close.invoke(-1);

        assertEquals("close(-1) must fail", -1, result);
        assertEquals("errno must be EBADF", 9, LinkerHelper.systemError());
    }

    /**
     * On Windows, {@link LinkerHelper#systemError()} returns the Win32 {@code GetLastError} value
     * captured by a {@code downcallHandleWithSystemError} call. {@code CloseHandle} with an obviously
     * invalid handle always fails with {@code ERROR_INVALID_HANDLE} (6), making it a self-contained
     * way to force a known {@code GetLastError} value.
     *
     * <p>{@code Linker.Option.captureCallState("GetLastError")} is only a valid capture state name
     * on Windows, so this test is a no-op everywhere else.
     */
    public void testSystemErrorReturnsCapturedGetLastErrorAfterFailedCall() throws Throwable {
        if (System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows") == false) {
            // GetLastError capture state is Windows-only; skip everywhere else.
            return;
        }

        MethodHandle closeHandle = LinkerHelper.downcallHandleWithSystemError("CloseHandle", FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS));
        boolean result = (boolean) closeHandle.invoke(MemorySegment.ofAddress(0xDEADBEEFL));

        assertFalse("CloseHandle(invalid handle) must fail", result);
        assertEquals("GetLastError must be ERROR_INVALID_HANDLE", 6, LinkerHelper.systemError());
    }
}
