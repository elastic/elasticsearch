/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.annotation.AllocatesConstantAnnotation;
import org.elasticsearch.painless.spi.annotation.AllocatesConstantAnnotationParser;
import org.elasticsearch.painless.spi.annotation.AllocatesDynamicAnnotation;
import org.elasticsearch.painless.spi.annotation.AllocatesDynamicAnnotationParser;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

/** Unit tests for the {@code @allocates_constant} and {@code @allocates_dynamic} allowlist annotation parsers. */
public class AllocatesAnnotationParserTests extends ESTestCase {

    private static final AllocatesConstantAnnotationParser ALLOCATES = AllocatesConstantAnnotationParser.INSTANCE;
    private static final AllocatesDynamicAnnotationParser DYNAMIC = AllocatesDynamicAnnotationParser.INSTANCE;

    public void testAllocatesParsesBytes() {
        AllocatesConstantAnnotation annotation = (AllocatesConstantAnnotation) ALLOCATES.parse(
            Map.of(AllocatesConstantAnnotationParser.BYTES, "40b")
        );
        assertEquals(40L, annotation.bytes());
    }

    public void testAllocatesParsesUnits() {
        assertEquals(
            1024L,
            ((AllocatesConstantAnnotation) ALLOCATES.parse(Map.of(AllocatesConstantAnnotationParser.BYTES, "1kb"))).bytes()
        );
    }

    public void testAllocatesAcceptsZero() {
        // @allocates_constant[bytes="0"] is a valid no-op ("audited: does not allocate").
        assertEquals(0L, ((AllocatesConstantAnnotation) ALLOCATES.parse(Map.of(AllocatesConstantAnnotationParser.BYTES, "0"))).bytes());
    }

    public void testAllocatesRejectsMissingUnits() {
        // ByteSizeValue requires a unit for anything but "0"; a bare number must not parse.
        expectThrows(IllegalArgumentException.class, () -> ALLOCATES.parse(Map.of(AllocatesConstantAnnotationParser.BYTES, "40")));
    }

    public void testAllocatesRejectsNegative() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ALLOCATES.parse(Map.of(AllocatesConstantAnnotationParser.BYTES, "-1"))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("must not be negative"));
    }

    public void testAllocatesRejectsNonNumeric() {
        expectThrows(IllegalArgumentException.class, () -> ALLOCATES.parse(Map.of(AllocatesConstantAnnotationParser.BYTES, "big")));
    }

    public void testAllocatesRejectsMissingArgument() {
        expectThrows(IllegalArgumentException.class, () -> ALLOCATES.parse(Map.of()));
    }

    public void testAllocatesRejectsUnknownArgument() {
        expectThrows(IllegalArgumentException.class, () -> ALLOCATES.parse(Map.of("size", "40")));
    }

    public void testDynamicParsesClassAndMethod() {
        AllocatesDynamicAnnotation annotation = (AllocatesDynamicAnnotation) DYNAMIC.parse(
            Map.of(AllocatesDynamicAnnotationParser.CLASS, "com.example.Foo", AllocatesDynamicAnnotationParser.METHOD, "estimate")
        );
        assertEquals("com.example.Foo", annotation.estimatorClassName());
        assertEquals("estimate", annotation.estimatorMethodName());
    }

    public void testDynamicAcceptsInnerClassDollarForm() {
        AllocatesDynamicAnnotation annotation = (AllocatesDynamicAnnotation) DYNAMIC.parse(
            Map.of(AllocatesDynamicAnnotationParser.CLASS, "com.example.Outer$Inner", AllocatesDynamicAnnotationParser.METHOD, "estimate")
        );
        assertEquals("com.example.Outer$Inner", annotation.estimatorClassName());
        assertEquals("estimate", annotation.estimatorMethodName());
    }

    public void testDynamicRejectsMissingClass() {
        expectThrows(IllegalArgumentException.class, () -> DYNAMIC.parse(Map.of(AllocatesDynamicAnnotationParser.METHOD, "estimate")));
    }

    public void testDynamicRejectsMissingMethod() {
        expectThrows(
            IllegalArgumentException.class,
            () -> DYNAMIC.parse(Map.of(AllocatesDynamicAnnotationParser.CLASS, "com.example.Foo"))
        );
    }

    public void testDynamicRejectsEmptyValues() {
        expectThrows(
            IllegalArgumentException.class,
            () -> DYNAMIC.parse(Map.of(AllocatesDynamicAnnotationParser.CLASS, " ", AllocatesDynamicAnnotationParser.METHOD, "estimate"))
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> DYNAMIC.parse(
                Map.of(AllocatesDynamicAnnotationParser.CLASS, "com.example.Foo", AllocatesDynamicAnnotationParser.METHOD, " ")
            )
        );
    }

    public void testDynamicRejectsUnknownArgument() {
        expectThrows(
            IllegalArgumentException.class,
            () -> DYNAMIC.parse(
                Map.of(
                    AllocatesDynamicAnnotationParser.CLASS,
                    "com.example.Foo",
                    AllocatesDynamicAnnotationParser.METHOD,
                    "estimate",
                    "extra",
                    "nope"
                )
            )
        );
    }

    public void testDynamicRejectsMissingArguments() {
        expectThrows(IllegalArgumentException.class, () -> DYNAMIC.parse(Map.of()));
    }
}
