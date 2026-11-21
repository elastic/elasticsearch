/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public class ErrorExcerptsTests extends ESTestCase {

    public void testShortValuePassesThrough() {
        assertEquals("hello", ErrorExcerpts.summarize("hello"));
        assertEquals("null", ErrorExcerpts.summarize(null));
        String atCap = "x".repeat(ErrorExcerpts.MAX_EXCERPT_CHARS);
        assertSame(atCap, ErrorExcerpts.summarize(atCap));
    }

    public void testLongValueIsCappedKeepingBothEnds() {
        String huge = "H" + "x".repeat(5_000) + "T";
        String summarized = ErrorExcerpts.summarize(huge);
        assertThat(summarized.length(), lessThanOrEqualTo(ErrorExcerpts.MAX_EXCERPT_CHARS));
        assertThat(summarized, containsString("(truncated, 5002 chars total)"));
        assertThat(summarized, startsWith("H"));
        assertThat(summarized, endsWith("T"));
    }

    public void testOneCharOverCapIsCapped() {
        String overCap = "x".repeat(ErrorExcerpts.MAX_EXCERPT_CHARS + 1);
        String summarized = ErrorExcerpts.summarize(overCap);
        assertThat(summarized.length(), lessThanOrEqualTo(ErrorExcerpts.MAX_EXCERPT_CHARS));
        assertThat(summarized, containsString("(truncated, " + overCap.length() + " chars total)"));
    }

    public void testEscapedSummaryKeepsAShortValueWhole() {
        assertEquals("null", ErrorExcerpts.summarizeEscaped(null));
        assertEquals("plain", ErrorExcerpts.summarizeEscaped("plain"));
        assertEquals("a\\u005D b", ErrorExcerpts.summarizeEscaped("a] b"));
    }

    /**
     * A value short enough for the cap can still escape past it -- six characters out for every bracket or
     * control character in -- so the cap has to be applied to the escape rather than to what it was before.
     */
    public void testAValueTheEscapeGrowsPastTheCapIsCut() {
        String brackets = "[".repeat(ErrorExcerpts.MAX_EXCERPT_CHARS / 2);
        String summarized = ErrorExcerpts.summarizeEscaped(brackets);
        assertThat(summarized.length(), lessThanOrEqualTo(ErrorExcerpts.MAX_EXCERPT_CHARS));
        assertThat(summarized, containsString("(truncated, " + brackets.length() + " chars total)"));
    }

    /**
     * The value is a CSV field, which a dialect lets reach {@code max_field_size} -- ten megabytes by default,
     * unbounded when the option is zero. Escaping it whole to then cut 256 characters out of the result would
     * build six characters for every one of those, so the cut is taken on the value and only the two kept
     * windows are escaped. What that choice is visible as, and what this pins: the count names the value's own
     * length rather than the length of an escape nothing ever built.
     */
    public void testALargeValueIsNeverEscapedWhole() {
        String large = "[".repeat(2_000_000);
        String summarized = ErrorExcerpts.summarizeEscaped(large);
        assertThat(summarized.length(), lessThanOrEqualTo(ErrorExcerpts.MAX_EXCERPT_CHARS));
        assertThat(summarized, containsString("(truncated, 2000000 chars total)"));
        assertThat(summarized, startsWith("\\u005B"));
        assertThat(summarized, endsWith("\\u005B"));
    }

    /**
     * Cutting the value rather than its escape is what keeps a half-written escape out of the message: every
     * escape in the result was produced from a character wholly inside a kept window, so no stump of one can
     * reach the reader. Cutting the escaped text instead could end a window inside {@code u005D}, which spells
     * nothing and would be the only part of the name the reader is shown at that end.
     */
    public void testTheCutNeverSplitsAnEscape() {
        for (String value : List.of(
            "[".repeat(1_000),
            "]".repeat(997),
            "a[b]c".repeat(400),
            "\t".repeat(500),
            "x".repeat(300) + "[" + "y".repeat(300)
        )) {
            String summarized = ErrorExcerpts.summarizeEscaped(value);
            assertThat(summarized.length(), lessThanOrEqualTo(ErrorExcerpts.MAX_EXCERPT_CHARS));
            Matcher stump = Pattern.compile("\\\\u(?![0-9A-F]{4})").matcher(summarized);
            assertFalse("a cut left a half-written escape in [" + summarized + "]", stump.find());
        }
    }

    /**
     * A window chosen by escaped width can still land between the halves of a surrogate pair, and a lone
     * surrogate encodes to no UTF-8 sequence -- so the orphan is given up rather than kept, which is what
     * {@code RowLocation.shorten} does with the same problem.
     */
    public void testNoUnpairedSurrogateSurvivesTheCut() {
        String emoji = "\uD83D\uDE00";
        String value = emoji.repeat(1_000);
        String summarized = ErrorExcerpts.summarizeEscaped(value);
        assertThat(summarized.length(), lessThanOrEqualTo(ErrorExcerpts.MAX_EXCERPT_CHARS));
        for (int i = 0; i < summarized.length(); i++) {
            char c = summarized.charAt(i);
            if (Character.isHighSurrogate(c)) {
                assertTrue(
                    "unpaired high surrogate at " + i,
                    i + 1 < summarized.length() && Character.isLowSurrogate(summarized.charAt(i + 1))
                );
            } else if (Character.isLowSurrogate(c)) {
                assertTrue("unpaired low surrogate at " + i, i > 0 && Character.isHighSurrogate(summarized.charAt(i - 1)));
            }
        }
    }
}
