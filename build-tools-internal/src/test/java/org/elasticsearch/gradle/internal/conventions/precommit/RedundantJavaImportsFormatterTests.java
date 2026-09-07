/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.precommit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RedundantJavaImportsFormatterTests {

    @Test
    public void testRemovesRedundantJunitStaticImportsWhenExtendingTestCase() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESTestCase;

            import static org.junit.Assert.assertNotNull;
            import static org.junit.Assert.assertTrue;

            public class ExampleTests extends ESTestCase {
                public void testThing() {
                    assertNotNull("x");
                    assertTrue(true);
                }
            }
            """;
        String expected = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESTestCase;

            public class ExampleTests extends ESTestCase {
                public void testThing() {
                    assertNotNull("x");
                    assertTrue(true);
                }
            }
            """;
        assertEquals(expected, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testKeepsJunitStaticImportsWhenNotExtendingTestCase() {
        String input = """
            package org.elasticsearch.example;

            import static org.junit.Assert.assertEquals;

            public class Example {
                public void testThing() {
                    assertEquals(1, 1);
                }
            }
            """;
        assertEquals(input, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testRemovesRandomizedTestStaticImportsWhenExtendingTestCase() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESAllocationTestCase;

            import static com.carrotsearch.randomizedtesting.RandomizedTest.rarely;

            public class ExampleTests extends ESAllocationTestCase {
                public void testThing() {
                    if (rarely()) {
                        return;
                    }
                }
            }
            """;
        String expected = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESAllocationTestCase;

            public class ExampleTests extends ESAllocationTestCase {
                public void testThing() {
                    if (rarely()) {
                        return;
                    }
                }
            }
            """;
        assertEquals(expected, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testRemovesUnusedStaticMethodImportThatCollidesWithAVariable() {
        String input = """
            package org.elasticsearch.example;

            import java.io.InputStream;

            import static org.hamcrest.Matchers.in;

            public class Example {
                public void testThing(InputStream in) {
                    in.hashCode();
                }
            }
            """;
        String expected = """
            package org.elasticsearch.example;

            import java.io.InputStream;

            public class Example {
                public void testThing(InputStream in) {
                    in.hashCode();
                }
            }
            """;
        assertEquals(expected, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testKeepsStaticMethodImportUsedAsAMethodCall() {
        String input = """
            package org.elasticsearch.example;

            import static org.hamcrest.Matchers.in;

            public class Example {
                public Object testThing() {
                    return in(java.util.List.of(1));
                }
            }
            """;
        assertEquals(input, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testRemovesUnusedStringFormatImportThatCollidesWithAField() {
        String input = """
            package org.elasticsearch.example;

            import static java.lang.String.format;

            public class Example {
                private String format;

                public String name() {
                    return format;
                }
            }
            """;
        String expected = """
            package org.elasticsearch.example;

            public class Example {
                private String format;

                public String name() {
                    return format;
                }
            }
            """;
        assertEquals(expected, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testRemovesNestedTypeImportWhenSubclassExtendsEnclosingType() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.example.Outer;
            import org.elasticsearch.example.Outer.Inner;

            public class Example extends Outer {
                public void testThing(Inner inner) {
                    inner.hashCode();
                }
            }
            """;
        String expected = """
            package org.elasticsearch.example;

            import org.elasticsearch.example.Outer;

            public class Example extends Outer {
                public void testThing(Inner inner) {
                    inner.hashCode();
                }
            }
            """;
        assertEquals(expected, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testKeepsNestedTypeImportWhenUsedOutsideInheritingType() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.example.Outer.Inner;

            public abstract class Example {
                public abstract static class Loader implements Outer {
                    public Inner inner() {
                        return null;
                    }
                }

                public static class Holder {
                    public Inner inner() {
                        return null;
                    }
                }
            }
            """;
        assertEquals(input, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testRemovesNestedTypeImportWhenOnlyUsedInsideImplementingNestedClass() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.example.Outer.Inner;

            public abstract class Example {
                public abstract static class Loader implements Outer {
                    public Inner inner() {
                        return null;
                    }
                }
            }
            """;
        String expected = """
            package org.elasticsearch.example;

            public abstract class Example {
                public abstract static class Loader implements Outer {
                    public Inner inner() {
                        return null;
                    }
                }
            }
            """;
        assertEquals(expected, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testRemovesJunitStaticImportsWhenExtendingRandomizedTest() {
        String input = """
            package org.elasticsearch.client;

            import com.carrotsearch.randomizedtesting.RandomizedTest;

            import static org.junit.Assert.assertEquals;

            public abstract class RestClientTestCase extends RandomizedTest {
                public void testThing() {
                    assertEquals(1, 1);
                }
            }
            """;
        String expected = """
            package org.elasticsearch.client;

            import com.carrotsearch.randomizedtesting.RandomizedTest;

            public abstract class RestClientTestCase extends RandomizedTest {
                public void testThing() {
                    assertEquals(1, 1);
                }
            }
            """;
        assertEquals(expected, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testLeavesSourceUnchangedWhenThereAreNoImports() {
        String input = """
            package org.elasticsearch.example;

            public class Example {}
            """;
        assertEquals(input, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testRemovesUnusedStaticMethodImportThatCollidesWithATryWithResourcesVariable() {
        String input = """
            package org.elasticsearch.example;

            import java.io.InputStream;

            import static org.hamcrest.Matchers.in;

            public class Example {
                public void testThing() throws Exception {
                    try (InputStream in = null) {
                        in.hashCode();
                    }
                }
            }
            """;
        String expected = """
            package org.elasticsearch.example;

            import java.io.InputStream;

            public class Example {
                public void testThing() throws Exception {
                    try (InputStream in = null) {
                        in.hashCode();
                    }
                }
            }
            """;
        assertEquals(expected, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testKeepsStaticFieldImportUsedAsAnIdentifier() {
        String input = """
            package org.elasticsearch.example;

            import static org.elasticsearch.example.Holder.logger;

            public class Example {
                public void log() {
                    logger.info("x");
                }

                static class Holder {
                    static Logger logger = null;
                }
            }
            """;
        assertEquals(input, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testKeepsNestedTypeImportUsedOnlyInJavadoc() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.example.Outer.Inner;

            /**
             * See {@link Inner#foo()}.
             */
            public class Example {
            }
            """;
        assertEquals(input, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testKeepsNestedTypeImportUsedOnlyAsAnnotationWhenNotInheriting() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.SkipUnavailableRule;
            import org.elasticsearch.test.SkipUnavailableRule.NotSkipped;

            public class Example {
                @NotSkipped(aliases = { "remote" })
                public void testThing() {}
            }
            """;
        assertEquals(input, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testRemovesNestedTypeImportUsedOnlyAsAnnotationWhenSubclassExtendsEnclosingType() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESIntegTestCase;
            import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

            @ClusterScope(supportsDedicatedMasters = false)
            public class ExampleTests extends ESIntegTestCase {
            }
            """;
        String expected = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESIntegTestCase;

            @ClusterScope(supportsDedicatedMasters = false)
            public class ExampleTests extends ESIntegTestCase {
            }
            """;
        assertEquals(expected, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testKeepsJupiterStaticImportsWhenExtendingTestCase() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESTestCase;

            import static org.junit.jupiter.api.Assertions.assertThrows;

            public class ExampleTests extends ESTestCase {
                public void testThing() {
                    assertThrows(IllegalStateException.class, () -> {
                        throw new IllegalStateException();
                    });
                }
            }
            """;
        assertEquals(input, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testKeepsJunitStaticImportWhenAlsoUsedInNonTestTypeInSameFile() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESTestCase;

            import static org.junit.Assert.assertEquals;

            public class ExampleTests extends ESTestCase {
                public void testThing() {
                    assertEquals(1, 1);
                }
            }

            class ExampleHelper {
                static void check() {
                    assertEquals(1, 1);
                }
            }
            """;
        assertEquals(input, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testRemovesJunitStaticImportWhenOnlyUsedInTestTypeInSameFile() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESTestCase;

            import static org.junit.Assert.assertEquals;

            public class ExampleTests extends ESTestCase {
                public void testThing() {
                    assertEquals(1, 1);
                }
            }

            class ExampleHelper {
                static void check() {}
            }
            """;
        String expected = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESTestCase;

            public class ExampleTests extends ESTestCase {
                public void testThing() {
                    assertEquals(1, 1);
                }
            }

            class ExampleHelper {
                static void check() {}
            }
            """;
        assertEquals(expected, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testRemovesJunitAsteriskStaticImportWhenExtendingTestCase() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESTestCase;

            import static org.junit.Assert.*;

            public class ExampleTests extends ESTestCase {
                public void testThing() {
                    assertEquals(1, 1);
                }
            }
            """;
        String expected = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESTestCase;

            public class ExampleTests extends ESTestCase {
                public void testThing() {
                    assertEquals(1, 1);
                }
            }
            """;
        assertEquals(expected, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testKeepsJunitAsteriskStaticImportWhenUsedInNonTestTypeInSameFile() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.test.ESTestCase;

            import static org.junit.Assert.*;

            public class ExampleTests extends ESTestCase {
                public void testThing() {
                    assertEquals(1, 1);
                }
            }

            class ExampleHelper {
                static void check() {
                    assertTrue(true);
                }
            }
            """;
        assertEquals(input, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testKeepsJunitAsteriskStaticImportWhenNotExtendingTestCase() {
        String input = """
            package org.elasticsearch.example;

            import static org.junit.Assert.*;

            public class Example {
                public void testThing() {
                    assertEquals(1, 1);
                }
            }
            """;
        assertEquals(input, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testKeepsNestedTypeImportUsedAsMethodReferenceWhenNotInheriting() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.example.Outer.Inner;

            public class Example {
                public Runnable run() {
                    return Inner::new;
                }
            }
            """;
        assertEquals(input, RedundantJavaImportsFormatter.format(input));
    }

    @Test
    public void testRemovesNestedTypeImportUsedAsMethodReferenceWhenSubclassExtendsEnclosingType() {
        String input = """
            package org.elasticsearch.example;

            import org.elasticsearch.example.Outer;
            import org.elasticsearch.example.Outer.Inner;

            public class Example extends Outer {
                public Runnable run() {
                    return Inner::new;
                }
            }
            """;
        String expected = """
            package org.elasticsearch.example;

            import org.elasticsearch.example.Outer;

            public class Example extends Outer {
                public Runnable run() {
                    return Inner::new;
                }
            }
            """;
        assertEquals(expected, RedundantJavaImportsFormatter.format(input));
    }
}
