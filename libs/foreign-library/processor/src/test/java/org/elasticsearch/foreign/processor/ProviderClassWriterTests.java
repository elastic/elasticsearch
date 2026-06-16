/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

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
        Object providerInstance = providerClass.getDeclaredConstructor().newInstance();
        Class<?> returned = (Class<?>) libraryClassMethod.invoke(providerInstance);
        assertEquals("libraryClass() must return the annotated interface", "test.MyLib", returned.getName());
    }
}
