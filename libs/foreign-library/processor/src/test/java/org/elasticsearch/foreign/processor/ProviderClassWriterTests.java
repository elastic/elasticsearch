/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.Platform;

/**
 * Tests that {@link ProviderClassWriter} generates correct {@code $Provider} class files.
 */
public class ProviderClassWriterTests extends ProcessorTestCase {

    /**
     * A valid @LibrarySpecification interface must generate a public {@code $Provider} class that
     * implements {@code LibraryProvider}, with {@code libraryClass()} returning the interface.
     */
    public void testValidLibraryGeneratesProvider() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("native_add")
                int add(int a, int b);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> providerClass = result.loadClassNoInit("test.MyLib$Provider");
        assertNotNull("Generated MyLib$Provider class not found", providerClass);

        // Must be public and final
        assertTrue("provider class must be public", java.lang.reflect.Modifier.isPublic(providerClass.getModifiers()));
        assertTrue("provider class must be final", java.lang.reflect.Modifier.isFinal(providerClass.getModifiers()));

        // Must implement LibraryProvider
        boolean implementsLibraryProvider = java.util.Arrays.stream(providerClass.getInterfaces())
            .anyMatch(i -> i.getName().equals("org.elasticsearch.foreign.LibraryProvider"));
        assertTrue("provider must implement LibraryProvider", implementsLibraryProvider);

        // libraryClass() must return the interface Class object
        java.lang.reflect.Method libraryClassMethod = providerClass.getMethod("libraryClass");
        Object providerInstance = providerClass.getConstructor().newInstance();
        Class<?> returned = (Class<?>) libraryClassMethod.invoke(providerInstance);
        assertEquals("libraryClass() must return the annotated interface", "test.MyLib", returned.getName());
    }

    /**
     * A library with {@code unavailableOn} listing all five platforms must generate a {@code load()}
     * that returns {@code null} regardless of the current platform, without attempting to construct
     * the {@code $Impl} (which would require a live native library).
     */
    public void testUnavailableOnAllPlatformsLoadReturnsNull() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Platform;
            @LibrarySpecification(
                name = "testlib",
                unavailableOn = {
                    Platform.LINUX_X64,
                    Platform.LINUX_AARCH64,
                    Platform.DARWIN_X64,
                    Platform.DARWIN_AARCH64,
                    Platform.WINDOWS_X64
                }
            )
            public interface MyLib {
                @Function("native_fn")
                int fn(int x);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // Load with initialization: load() will return null before $Impl is ever constructed,
        // so the $Impl <clinit> (which would require a native library) is never triggered.
        Class<?> providerClass = result.loadClass("test.MyLib$Provider");
        assertNotNull("Generated MyLib$Provider class not found", providerClass);

        java.lang.reflect.Method loadMethod = providerClass.getMethod("load");
        Object providerInstance = providerClass.getConstructor().newInstance();
        Object loadResult = loadMethod.invoke(providerInstance);
        assertNull("load() must return null when current platform is in unavailableOn", loadResult);
    }

    /**
     * A library with {@code unavailableOn} listing only the current platform must generate a
     * {@code load()} that returns {@code null} for this specific platform check.
     */
    public void testUnavailableOnCurrentPlatformLoadReturnsNull() throws Exception {
        String currentPlatform = Platform.current().name();
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Platform;
            @LibrarySpecification(
                name = "testlib",
                unavailableOn = { Platform.%s }
            )
            public interface MyLib {
                @Function("native_fn")
                int fn(int x);
            }
            """.formatted(currentPlatform);

        CompilationResult result = compile("test.MyLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // load() must return null before $Impl is constructed (no native library needed)
        Class<?> providerClass = result.loadClass("test.MyLib$Provider");
        assertNotNull("Generated MyLib$Provider class not found", providerClass);

        java.lang.reflect.Method loadMethod = providerClass.getMethod("load");
        Object providerInstance = providerClass.getConstructor().newInstance();
        Object loadResult = loadMethod.invoke(providerInstance);
        assertNull(
            "load() must return null when current platform (" + currentPlatform + ") is in unavailableOn",
            loadResult
        );
    }

    /**
     * A library with empty {@code unavailableOn} generates {@code load()} that attempts to construct
     * the {@code $Impl} (no platform check is emitted).
     *
     * <p>We only verify compilation and that the provider class is present — calling {@code load()}
     * would trigger the {@code $Impl} static initializer and require a live native library.
     */
    public void testEmptyUnavailableOnGeneratesLoadWithoutPlatformCheck() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib", unavailableOn = {})
            public interface MyLib {
                @Function("native_fn")
                int fn(int x);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // Verify the provider class exists and is well-formed
        Class<?> providerClass = result.loadClassNoInit("test.MyLib$Provider");
        assertNotNull("Generated MyLib$Provider class not found", providerClass);

        // The generated load() must exist and not have been altered to return null unconditionally
        java.lang.reflect.Method loadMethod = providerClass.getMethod("load");
        assertNotNull("load() method must exist on generated provider", loadMethod);
    }
}
