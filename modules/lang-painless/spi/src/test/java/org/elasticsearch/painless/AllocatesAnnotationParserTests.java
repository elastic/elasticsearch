/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.annotation.AllocatesAnnotation;
import org.elasticsearch.painless.spi.annotation.AllocatesAnnotationParser;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

/** Unit tests for the {@code @allocates[class=…, method=…]} allowlist annotation parser. */
public class AllocatesAnnotationParserTests extends ESTestCase {

    private static final AllocatesAnnotationParser ALLOCATES = AllocatesAnnotationParser.INSTANCE;

    private static AllocatesAnnotation parse(Map<String, String> arguments) {
        return (AllocatesAnnotation) ALLOCATES.parse(arguments);
    }

    public void testParsesClassAndMethod() {
        AllocatesAnnotation annotation = parse(
            Map.of(AllocatesAnnotationParser.CLASS, "com.example.Foo", AllocatesAnnotationParser.METHOD, "estimate")
        );
        assertEquals("com.example.Foo", annotation.estimatorClassName());
        assertEquals("estimate", annotation.estimatorMethodName());
    }

    public void testAcceptsInnerClassDollarForm() {
        assertEquals(
            "com.example.Outer$Inner",
            parse(Map.of(AllocatesAnnotationParser.CLASS, "com.example.Outer$Inner", AllocatesAnnotationParser.METHOD, "estimate"))
                .estimatorClassName()
        );
    }

    public void testRejectsMissingClass() {
        expectThrows(IllegalArgumentException.class, () -> parse(Map.of(AllocatesAnnotationParser.METHOD, "estimate")));
    }

    public void testRejectsMissingMethod() {
        expectThrows(IllegalArgumentException.class, () -> parse(Map.of(AllocatesAnnotationParser.CLASS, "com.example.Foo")));
    }

    public void testRejectsMissingArguments() {
        expectThrows(IllegalArgumentException.class, () -> parse(Map.of()));
    }

    public void testRejectsEmptyValues() {
        expectThrows(
            IllegalArgumentException.class,
            () -> parse(Map.of(AllocatesAnnotationParser.CLASS, " ", AllocatesAnnotationParser.METHOD, "estimate"))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> parse(Map.of(AllocatesAnnotationParser.CLASS, "com.example.Foo", AllocatesAnnotationParser.METHOD, " "))
        );
    }

    public void testRejectsUnknownArgument() {
        expectThrows(
            IllegalArgumentException.class,
            () -> parse(
                Map.of(AllocatesAnnotationParser.CLASS, "com.example.Foo", AllocatesAnnotationParser.METHOD, "estimate", "extra", "nope")
            )
        );
    }
}
