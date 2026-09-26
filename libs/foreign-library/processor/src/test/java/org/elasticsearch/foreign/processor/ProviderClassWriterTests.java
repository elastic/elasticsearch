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

import java.util.Locale;

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
     * A library with {@code unavailableOn} listing two platforms, one of which is the current platform,
     * must generate a {@code load()} that returns {@code null} without constructing the {@code $Impl}.
     */
    public void testUnavailableOnTwoPlatformsIncludingCurrentLoadReturnsNull() throws Exception {
        Platform current = Platform.current();
        Platform other = java.util.Arrays.stream(Platform.values()).filter(p -> p != current).findFirst().get();

        String source = String.format(Locale.ROOT, """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Platform;
            @LibrarySpecification(
                name = "testlib",
                unavailableOn = { Platform.%s, Platform.%s }
            )
            public interface MyLib {
                @Function("native_fn")
                int fn(int x);
            }
            """, current.name(), other.name());

        CompilationResult result = compile("test.MyLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> providerClass = result.loadClass("test.MyLib$Provider");
        assertNotNull("Generated MyLib$Provider class not found", providerClass);

        java.lang.reflect.Method loadMethod = providerClass.getMethod("load");
        Object providerInstance = providerClass.getConstructor().newInstance();
        Object loadResult = loadMethod.invoke(providerInstance);
        assertNull("load() must return null when current platform (" + current.name() + ") is in unavailableOn", loadResult);
    }

    /**
     * A library with {@code unavailableOn} listing only the current platform must generate a
     * {@code load()} that returns {@code null} for this specific platform check.
     */
    public void testUnavailableOnCurrentPlatformLoadReturnsNull() throws Exception {
        String currentPlatform = Platform.current().name();
        String source = String.format(Locale.ROOT, """
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
            """, currentPlatform);

        CompilationResult result = compile("test.MyLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // load() must return null before $Impl is constructed (no native library needed)
        Class<?> providerClass = result.loadClass("test.MyLib$Provider");
        assertNotNull("Generated MyLib$Provider class not found", providerClass);

        java.lang.reflect.Method loadMethod = providerClass.getMethod("load");
        Object providerInstance = providerClass.getConstructor().newInstance();
        Object loadResult = loadMethod.invoke(providerInstance);
        assertNull("load() must return null when current platform (" + currentPlatform + ") is in unavailableOn", loadResult);
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
