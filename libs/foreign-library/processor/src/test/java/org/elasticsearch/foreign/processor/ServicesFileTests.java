/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Tests that {@link LibraryProcessor} writes a {@code META-INF/services/org.elasticsearch.foreign.LibraryProvider}
 * file listing every generated {@code $Provider} class.
 */
public class ServicesFileTests extends ProcessorTestCase {

    private static final String SERVICES_PATH = "META-INF/services/org.elasticsearch.foreign.LibraryProvider";

    /**
     * A successful compilation of one {@code @LibrarySpecification} interface must write a services file
     * containing exactly that interface's generated {@code $Provider} class name.
     */
    public void testSingleProviderServicesFileGenerated() throws Exception {
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

        String content = result.readResource(SERVICES_PATH);
        assertNotNull("Expected services file at " + SERVICES_PATH, content);

        List<String> entries = Arrays.stream(content.split("\n")).filter(s -> s.isBlank() == false).toList();
        assertEquals("Services file should list exactly one provider, got: " + entries, 1, entries.size());
        assertEquals("test.MyLib$Provider", entries.get(0));
    }

    /**
     * The services file must list providers in sorted order, independent of the order in which javac
     * hands the annotated sources to the processor. That order follows source-file order, which in real
     * builds depends on filesystem iteration order, so relying on it makes the jar non-reproducible.
     */
    public void testProvidersSortedIndependentOfSourceOrder() throws Exception {
        List<String> simpleNames = List.of("Zeta", "Mid", "Alpha");
        Map<String, String> sources = new LinkedHashMap<>();
        for (String simpleName : simpleNames) {
            sources.put("test." + simpleName, String.format(Locale.ROOT, """
                package test;
                import org.elasticsearch.foreign.LibrarySpecification;
                import org.elasticsearch.foreign.Function;
                @LibrarySpecification(name = "testlib")
                public interface %s {
                    @Function("native_add")
                    int add(int a, int b);
                }
                """, simpleName));
        }

        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        String content = result.readResource(SERVICES_PATH);
        assertNotNull("Expected services file at " + SERVICES_PATH, content);
        List<String> entries = Arrays.stream(content.split("\n")).filter(s -> s.isBlank() == false).toList();
        assertEquals(List.of("test.Alpha$Provider", "test.Mid$Provider", "test.Zeta$Provider"), entries);
    }

    /**
     * A failed compilation (the processor emitted errors before generating any provider) must not
     * produce a stray services file.
     */
    public void testNoServicesFileWhenNoProvidersGenerated() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            @LibrarySpecification
            public class NotAnInterface {
            }
            """;

        CompilationResult result = compile("test.NotAnInterface", source);
        assertFalse("Expected compilation to fail", result.success());
        assertNull("No services file should be written when no providers are generated", result.readResource(SERVICES_PATH));
    }
}
