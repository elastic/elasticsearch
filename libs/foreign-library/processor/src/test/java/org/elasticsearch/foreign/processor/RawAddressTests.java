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
 * Tests compile-time validation for {@code @RawAddress}.
 */
public class RawAddressTests extends ProcessorTestCase {

    public void testRawAddressOnNonMemorySegmentParamFails() {
        String source = """
            package test;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.RawAddress;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@RawAddress int value);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about non-MemorySegment parameter but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@RawAddress may only be applied to MemorySegment parameters"))
        );
    }

    public void testRawAddressWithStringParamCompiles() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.RawAddress;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@RawAddress MemorySegment value, String name);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
    }

    public void testRawAddressOnStructFactoryParamFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.RawAddress;
            import org.elasticsearch.foreign.StructFactory;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @StructSpecification
                interface Point {
                    int x();
                }

                @StructFactory
                Point newPoint(@RawAddress MemorySegment value);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about @StructFactory parameters but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("must not have @RawAddress on any parameter"))
        );
    }

    public void testRawAddressWithUpcallParamFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.RawAddress;
            import org.elasticsearch.foreign.Upcall;
            @Upcall
            @FunctionalInterface
            interface IntCallback {
                int call(int value);
            }
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@RawAddress MemorySegment value, IntCallback callback);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about @Upcall parameters but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("cannot be combined with @Upcall parameters"))
        );
    }
}
