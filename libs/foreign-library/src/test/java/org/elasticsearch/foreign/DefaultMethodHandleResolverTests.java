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
import java.lang.foreign.Linker;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Locale;

/**
 * Tests for {@link DefaultMethodHandleResolver}.
 *
 * <p>These tests use JUnit 3 ({@code junit.framework.TestCase}) directly rather than
 * {@code ESTestCase} to keep the foreign-library module's test dependencies minimal.
 */
public class DefaultMethodHandleResolverTests extends TestCase {

    /**
     * Verifies that {@link DefaultMethodHandleResolver} builds a working {@link MethodHandle}
     * for a process-id symbol available in the default native linker lookup.
     * Uses {@code getpid} on POSIX and {@code GetCurrentProcessId} on Windows — both take no
     * arguments and return a positive int, making them ideal for a self-contained native call.
     */
    public void testDefaultResolverBuildsWorkingMethodHandle() throws Throwable {
        Linker linker = Linker.nativeLinker();
        String symName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows")
            ? "GetCurrentProcessId"
            : "getpid";
        var addr = linker.defaultLookup()
            .find(symName)
            .orElseThrow(() -> new AssertionError(symName + " not found in default linker lookup"));

        ResolvedSymbol symbol = new ResolvedSymbol(symName, addr);
        FunctionDescriptor descriptor = FunctionDescriptor.of(ValueLayout.JAVA_INT);

        DefaultMethodHandleResolver resolver = new DefaultMethodHandleResolver();
        MethodHandle handle = resolver.resolve(symbol, descriptor, linker);

        assertNotNull("resolve() must return a non-null MethodHandle", handle);

        int pid = (int) handle.invoke();
        assertTrue("process id should be positive", pid > 0);
    }

    /**
     * Verifies that {@link DefaultMethodHandleResolver} implements {@link MethodHandleResolver}.
     */
    public void testDefaultResolverImplementsInterface() {
        assertTrue(new DefaultMethodHandleResolver() instanceof MethodHandleResolver);
    }

    /**
     * Verifies that {@link DefaultMethodHandleResolver} has a public no-arg constructor.
     */
    public void testDefaultResolverHasPublicNoArgConstructor() throws Exception {
        var constructor = DefaultMethodHandleResolver.class.getConstructor();
        assertNotNull(constructor);
    }
}
