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
 * Tests that {@link LibraryProcessor} emits the correct diagnostics for invalid inputs.
 */
public class LibraryProcessorTests extends ProcessorTestCase {

    /**
     * A @LibrarySpecification interface with a method that has no annotation should emit a Kind.ERROR.
     */
    public void testUnannotatedMethodEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            @LibrarySpecification
            public interface BadLib {
                int unannotated();
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail due to unannotated method", result.success());
        boolean hasProcessorError = result.errors().stream().anyMatch(msg -> msg.contains("unannotated"));
        assertTrue("Expected an error about 'unannotated' method but got: " + result.errors(), hasProcessorError);
    }

    /**
     * Combining {@code @Critical} and {@code @CaptureErrno} on the same method must emit a compile error
     * because critical calls bypass the errno-capture mechanism.
     */
    public void testCriticalAndCaptureErrnoEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.CaptureErrno;
            import org.elasticsearch.foreign.Critical;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("do_thing")
                @Critical
                @CaptureErrno
                int doThing(int x);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail due to @Critical + @CaptureErrno", result.success());
        assertTrue(
            "Expected error about @Critical and @CaptureErrno but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@Critical") && msg.contains("@CaptureErrno"))
        );
    }

    /**
     * Applying {@code @Utf16} to a non-String parameter must emit a compile error.
     */
    public void testUtf16OnNonStringParamEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Utf16;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("do_thing")
                void doThing(@Utf16 int x);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail due to @Utf16 on non-String", result.success());
        assertTrue(
            "Expected error about @Utf16 on non-String but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@Utf16"))
        );
    }

    /**
     * A {@code @LibrarySpecification} annotation on a class (not an interface) should emit an error.
     */
    public void testAnnotationOnClassEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            @LibrarySpecification
            public class NotAnInterface {
            }
            """;

        CompilationResult result = compile("test.NotAnInterface", source);

        assertFalse("Expected compilation to fail", result.success());
        boolean hasProcessorError = result.errors().stream().anyMatch(msg -> msg.contains("@LibrarySpecification must be on an interface"));
        assertTrue("Expected error about @LibrarySpecification on non-interface but got: " + result.errors(), hasProcessorError);
    }
}
