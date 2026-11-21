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
import static org.hamcrest.Matchers.greaterThan;
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
     * windows are escaped.
     * <p>
     * That the whole value is never escaped is a property of the work, not of the result: escaping first and
     * cutting at escape boundaries would produce this same string, because the escape is a per-character map and
     * both orders budget it the same way. Nothing a caller can observe separates them, and a timing or heap
     * assertion would pin the machine rather than the method, so what is asserted here is the output -- bounded,
     * counted by the value's own length, and whole escapes at both ends. The bounded work is held by
     * {@code charsEscapingWithin}, which walks from one end and stops at the budget, and by the two substrings
     * that bound what {@code escapedForDisplay} is ever handed.
     */
    public void testALargeValueIsCutRatherThanGrown() {
        String large = "[".repeat(2_000_000);
        String summarized = ErrorExcerpts.summarizeEscaped(large);
        assertThat(summarized.length(), lessThanOrEqualTo(ErrorExcerpts.MAX_EXCERPT_CHARS));
        String marker = CUT_MARKER_HEAD + "2000000" + CUT_MARKER_TAIL;
        int at = summarized.indexOf(marker);
        assertThat("the marker counts the value, not the escape", at, greaterThan(0));
        // Stripping every complete escape must leave nothing: a cut inside one leaves a stump at the end of the
        // head or a bare "005B" at the start of the tail. Each window is pinned present first, because stripping
        // the escapes out of an empty one also leaves nothing -- the strip alone passes on a window that is gone.
        String head = summarized.substring(0, at);
        String tail = summarized.substring(at + marker.length());
        assertThat("the head is kept", head, startsWith("\\u005B"));
        assertThat("the tail is kept", tail, endsWith("\\u005B"));
        assertEquals("the head is whole escapes", "", head.replace("\\u005B", ""));
        assertEquals("the tail is whole escapes", "", tail.replace("\\u005B", ""));
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
        // The scan above is anchored on the backslash, so it sees a head that ends inside an escape and not a
        // tail that begins inside one: a window starting at the "005D" of a lost "\\u005D" carries no backslash
        // to match. These three values escape every character they have, so both windows must strip to nothing --
        // and must be there to strip, which an empty window would satisfy without being anything a reader sees.
        for (String value : List.of("[".repeat(1_000), "]".repeat(997), "\t".repeat(500))) {
            String escape = RowLocation.escapedForDisplay(value.substring(0, 1));
            String summarized = ErrorExcerpts.summarizeEscaped(value);
            int at = summarized.indexOf(CUT_MARKER_HEAD);
            assertThat("the value was cut", at, greaterThan(0));
            int tailAt = summarized.indexOf(CUT_MARKER_TAIL, at) + CUT_MARKER_TAIL.length();
            String head = summarized.substring(0, at);
            String tail = summarized.substring(tailAt);
            assertThat("the head is kept", head, startsWith(escape));
            assertThat("the tail is kept", tail, endsWith(escape));
            assertEquals("the head is whole escapes", "", head.replace(escape, ""));
            assertEquals("the tail is whole escapes", "", tail.replace(escape, ""));
        }
    }

    /** The two halves of {@code ErrorExcerpts}'s truncation marker, around the count it carries. */
    private static final String CUT_MARKER_HEAD = "\u2026 (truncated, ";
    private static final String CUT_MARKER_TAIL = " chars total) \u2026";

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
