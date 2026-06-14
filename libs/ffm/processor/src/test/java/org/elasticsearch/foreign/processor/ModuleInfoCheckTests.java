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
 * Tests that {@link LibraryProcessor} verifies the enclosing module declares a {@code provides}
 * directive for each generated {@code $Provider} class, and emits a descriptive error when it does not.
 */
public class ModuleInfoCheckTests extends ProcessorTestCase {

    private static final String LIB_SOURCE = """
        package test;
        import org.elasticsearch.foreign.LibrarySpecification;
        import org.elasticsearch.foreign.Function;
        @LibrarySpecification(name = "testlib")
        public interface MyLib {
            @Function("native_add")
            int add(int a, int b);
        }
        """;

    /**
     * A module with the expected {@code provides ... with test.MyLib$Provider} directive must compile cleanly.
     */
    public void testModuleWithProvidesDirectiveSucceeds() {
        String moduleInfo = """
            module test.mod {
                requires org.elasticsearch.foreign;
                provides org.elasticsearch.foreign.LibraryProvider with test.MyLib$Provider;
            }
            """;

        CompilationResult result = compileModule("test.mod", moduleInfo, "test.MyLib", LIB_SOURCE);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
    }

    /**
     * A module that has no {@code provides org.elasticsearch.foreign.LibraryProvider} directive at all
     * must emit an error that names the generated provider and gives the exact directive to add.
     */
    public void testModuleWithoutProvidesDirectiveEmitsError() {
        String moduleInfo = """
            module test.mod {
                requires org.elasticsearch.foreign;
            }
            """;

        CompilationResult result = compileModule("test.mod", moduleInfo, "test.MyLib", LIB_SOURCE);
        assertFalse("Expected compilation to fail due to missing provides directive", result.success());
        assertTrue(
            "Expected error mentioning the missing provides directive and the generated provider, got: " + result.errors(),
            result.errors()
                .stream()
                .anyMatch(
                    msg -> msg.contains("test.MyLib$Provider")
                        && msg.contains("provides org.elasticsearch.foreign.LibraryProvider with test.MyLib$Provider;")
                )
        );
    }

    /**
     * A module that has a {@code provides org.elasticsearch.foreign.LibraryProvider} directive
     * for some <em>other</em> provider must emit an error pointing the developer to add this provider
     * to the existing {@code with} clause.
     */
    public void testModuleWithProvidesForDifferentImplEmitsError() {
        String moduleInfo = """
            module test.mod {
                requires org.elasticsearch.foreign;
                provides org.elasticsearch.foreign.LibraryProvider with test.Other$Provider;
            }
            """;

        CompilationResult result = compileModule("test.mod", moduleInfo, "test.MyLib", LIB_SOURCE);
        // The compiler will reject the bogus test.Other$Provider with its own error, so we can't assert
        // overall success here. We only care that the processor itself emitted its descriptive error.
        assertTrue(
            "Expected error pointing at the existing `provides` directive, got: " + result.errors(),
            result.errors()
                .stream()
                .anyMatch(msg -> msg.contains("test.MyLib$Provider") && msg.contains("existing `provides"))
        );
    }
}
