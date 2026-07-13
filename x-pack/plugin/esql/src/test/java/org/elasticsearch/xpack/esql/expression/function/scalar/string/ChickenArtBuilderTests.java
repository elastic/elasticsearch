/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
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
            chicken.buildChickenSay(scratch, new BytesRef("Hello!"));
            String result = scratch.bytesRefView().utf8ToString();

            assertThat(result, startsWith(" _"));
            assertThat(result, containsString("< Hello!"));
            assertThat(result, containsString(" >"));
        }
    }

    public void testBuildChickenSayMultiLine() {
        try (BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(newLimitedBreaker(ByteSizeValue.ofKb(2)), "test")) {
            chicken.buildChickenSay(scratch, new BytesRef("This is a longer message that wraps across lines"), 20);
            String result = scratch.bytesRefView().utf8ToString();

            assertThat(result, startsWith(" _"));
            assertThat(result, containsString("/ "));  // Multi-line start
            assertThat(result, containsString(" /"));  // Multi-line end
        }
    }

    public void testBuildChickenSayProducesOutput() {
        try (BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(newLimitedBreaker(ByteSizeValue.ofKb(2)), "test")) {
            chicken.buildChickenSay(scratch, new BytesRef("Test message"));
            String result = scratch.bytesRefView().utf8ToString();

            assertThat("Should produce output", result.length(), greaterThan(0));
            assertThat("Should have top border", result, startsWith(" _"));
            assertThat("Should contain message", result, containsString("Test message"));
            assertThat("Should have bottom border", result, containsString(" -"));
        }
    }

    public void testBuildChickenSayEmpty() {
        try (BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(newLimitedBreaker(ByteSizeValue.ofKb(2)), "test")) {
            chicken.buildChickenSay(scratch, new BytesRef(""));
            String result = scratch.bytesRefView().utf8ToString();

            assertThat(result, startsWith(" _"));
            // Empty message should still have bubble structure
            assertThat(result, containsString("<"));
            assertThat(result, containsString(">"));
        }
    }

    public void testWrapBytesSingleLine() {
        BytesRef message = new BytesRef("Hello world");
        List<ChickenArtBuilder.LineRange> lines = ChickenArtBuilder.wrapBytes(message, 40);
        assertEquals(1, lines.size());
        assertEquals("Hello world", extractLine(message, lines.get(0)));
    }

    public void testWrapBytesMultiLine() {
        BytesRef message = new BytesRef("This is a longer message that needs wrapping");
        List<ChickenArtBuilder.LineRange> lines = ChickenArtBuilder.wrapBytes(message, 20);
        assertThat(lines.size(), greaterThan(1));
        for (ChickenArtBuilder.LineRange line : lines) {
            assertTrue("Display width too large: " + line.displayWidth(), line.displayWidth() <= 20);
        }
    }

    public void testWrapBytesEmpty() {
        BytesRef message = new BytesRef("");
        List<ChickenArtBuilder.LineRange> lines = ChickenArtBuilder.wrapBytes(message, 40);
        assertEquals(1, lines.size());
        assertEquals(0, lines.get(0).displayWidth());
    }

    public void testWrapBytesWithUtf8() {
        // Test with multi-byte UTF-8 characters (emoji is 4 bytes but 1 display char)
        BytesRef message = new BytesRef("Hello 🐔 world");
        List<ChickenArtBuilder.LineRange> lines = ChickenArtBuilder.wrapBytes(message, 40);
        assertEquals(1, lines.size());
        assertEquals("Hello 🐔 world", extractLine(message, lines.get(0)));
    }

    public void testFromNameExact() {
        // Test that fromName returns the correct chicken for each enum value
        assertEquals(chicken, ChickenArtBuilder.fromName(chicken.name().toLowerCase(Locale.ROOT)));
    }

    public void testFromNameCaseInsensitive() {
        // Test case insensitivity
        assertEquals(chicken, ChickenArtBuilder.fromName(chicken.name().toUpperCase(Locale.ROOT)));
        assertEquals(chicken, ChickenArtBuilder.fromName(chicken.name().toLowerCase(Locale.ROOT)));
    }

    public void testFromNameBytesRef() {
        // Test BytesRef variant
        assertEquals(chicken, ChickenArtBuilder.fromName(new BytesRef(chicken.name().toLowerCase(Locale.ROOT))));
    }

    public void testFromNameNull() {
        assertNull(ChickenArtBuilder.fromName((String) null));
        assertNull(ChickenArtBuilder.fromName((BytesRef) null));
    }

    public void testFromNameUnknown() {
        assertNull(ChickenArtBuilder.fromName("unknown_style"));
        assertNull(ChickenArtBuilder.fromName(""));
    }

    public void testAvailableStylesContainsChicken() {
        String styles = ChickenArtBuilder.availableStyles();
        assertTrue("Should contain current chicken style", styles.contains(chicken.name().toLowerCase(Locale.ROOT)));
    }

    public void testRandomReturnsValidChicken() {
        ChickenArtBuilder randomChicken = ChickenArtBuilder.random();
        assertNotNull(randomChicken);
        // Verify it can be looked up by name
        assertEquals(randomChicken, ChickenArtBuilder.fromName(randomChicken.name()));
    }

    /**
     * Helper method to extract a line from the original message using a LineRange.
     */
    private static String extractLine(BytesRef message, ChickenArtBuilder.LineRange range) {
        return new BytesRef(message.bytes, range.startOffset(), range.endOffset() - range.startOffset()).utf8ToString();
    }
}
