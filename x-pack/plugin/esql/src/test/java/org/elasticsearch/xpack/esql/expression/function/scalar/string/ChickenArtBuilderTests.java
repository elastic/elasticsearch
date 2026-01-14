/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for {@link Chicken} ASCII art and text wrapping builder methods.
 */
public class ChickenArtBuilderTests extends ESTestCase {

    public void testBuildChickenSay() {
        try (BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(newLimitedBreaker(ByteSizeValue.ofKb(1)), "test")) {
            Chicken.buildChickenSay(scratch, "Hello!", 40);
            String result = scratch.bytesRefView().utf8ToString();

            assertThat(result, startsWith(" _"));
            assertTrue(result.contains("< Hello!"));
            assertTrue(result.contains("__//"));
        }
    }

    public void testBuildChickenSayMultiLine() {
        try (BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(newLimitedBreaker(ByteSizeValue.ofKb(2)), "test")) {
            Chicken.buildChickenSay(scratch, "This is a longer message that wraps", 20);
            String result = scratch.bytesRefView().utf8ToString();

            assertThat(result, startsWith(" _"));
            assertTrue(result.contains("/ "));  // Multi-line start
            assertTrue(result.contains("\\ ")); // Multi-line end
            assertTrue(result.contains("__//"));
        }
    }

    public void testWrapTextSingleLine() {
        List<String> lines = Chicken.wrapText("Hello world", 40);
        assertEquals(1, lines.size());
        assertEquals("Hello world", lines.get(0));
    }

    public void testWrapTextMultiLine() {
        List<String> lines = Chicken.wrapText("This is a longer message that needs wrapping", 20);
        assertTrue(lines.size() > 1);
        for (String line : lines) {
            assertTrue("Line too long: " + line, line.length() <= 20);
        }
    }

    public void testWrapTextEmpty() {
        List<String> lines = Chicken.wrapText("", 40);
        assertEquals(1, lines.size());
        assertEquals("", lines.get(0));
    }

    public void testWrapTextNull() {
        List<String> lines = Chicken.wrapText(null, 40);
        assertEquals(1, lines.size());
        assertEquals("", lines.get(0));
    }

    public void testChickenArtContainsExpectedElements() {
        try (BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(newLimitedBreaker(ByteSizeValue.ofKb(1)), "test")) {
            Chicken.buildChickenSay(scratch, "Test", 40);
            String result = scratch.bytesRefView().utf8ToString();

            // Verify chicken art is present
            assertTrue(result.contains("\\__//"));
            assertTrue(result.contains("_____/"));
            assertTrue(result.contains("\" \""));
        }
    }
}
