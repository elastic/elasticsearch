/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;

/**
 * Parameterized tests for {@link ChickenArtBuilder} - runs each test for every chicken type.
 */
public class ChickenArtBuilderTests extends ESTestCase {

    private final ChickenArtBuilder chicken;

    public ChickenArtBuilderTests(@Name("chicken") ChickenArtBuilder chicken) {
        this.chicken = chicken;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(ChickenArtBuilder.values()).map(c -> new Object[] { c }).collect(Collectors.toList());
    }

    public void testBuildChickenSaySingleLine() {
        try (BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(newLimitedBreaker(ByteSizeValue.ofKb(2)), "test")) {
            chicken.buildChickenSay(scratch, "Hello!");
            String result = scratch.bytesRefView().utf8ToString();

            assertThat(result, startsWith(" _"));
            assertThat(result, containsString("< Hello!"));
            assertThat(result, containsString(" >"));
        }
    }

    public void testBuildChickenSayMultiLine() {
        try (BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(newLimitedBreaker(ByteSizeValue.ofKb(2)), "test")) {
            chicken.buildChickenSay(scratch, "This is a longer message that wraps across lines", 20);
            String result = scratch.bytesRefView().utf8ToString();

            assertThat(result, startsWith(" _"));
            assertThat(result, containsString("/ "));  // Multi-line start
            assertThat(result, containsString(" /"));  // Multi-line end
        }
    }

    public void testBuildChickenSayProducesOutput() {
        try (BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(newLimitedBreaker(ByteSizeValue.ofKb(2)), "test")) {
            chicken.buildChickenSay(scratch, "Test message");
            String result = scratch.bytesRefView().utf8ToString();

            assertThat("Should produce output", result.length(), greaterThan(0));
            assertThat("Should have top border", result, startsWith(" _"));
            assertThat("Should contain message", result, containsString("Test message"));
            assertThat("Should have bottom border", result, containsString(" -"));
        }
    }

    public void testBuildChickenSayEmpty() {
        try (BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(newLimitedBreaker(ByteSizeValue.ofKb(2)), "test")) {
            chicken.buildChickenSay(scratch, "");
            String result = scratch.bytesRefView().utf8ToString();

            assertThat(result, startsWith(" _"));
            // Empty message should still have bubble structure
            assertThat(result, containsString("<"));
            assertThat(result, containsString(">"));
        }
    }

    public void testWrapTextSingleLine() {
        List<String> lines = ChickenArtBuilder.wrapText("Hello world", 40);
        assertEquals(1, lines.size());
        assertEquals("Hello world", lines.get(0));
    }

    public void testWrapTextMultiLine() {
        List<String> lines = ChickenArtBuilder.wrapText("This is a longer message that needs wrapping", 20);
        assertThat(lines.size(), greaterThan(1));
        for (String line : lines) {
            assertTrue("Line too long: " + line, line.length() <= 20);
        }
    }

    public void testWrapTextEmpty() {
        List<String> lines = ChickenArtBuilder.wrapText("", 40);
        assertEquals(1, lines.size());
        assertEquals("", lines.get(0));
    }

    public void testWrapTextNull() {
        List<String> lines = ChickenArtBuilder.wrapText(null, 40);
        assertEquals(1, lines.size());
        assertEquals("", lines.get(0));
    }
}
