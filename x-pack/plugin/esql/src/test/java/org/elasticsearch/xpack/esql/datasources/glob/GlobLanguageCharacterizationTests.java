/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.PatternSyntaxException;

/**
 * An exhaustive, labelled record of what the glob language does TODAY, written before the matcher is rewritten.
 *
 * <p>This is not a statement that today's behaviour is correct. Every case carries a verdict: {@link Verdict#KEEP}
 * means the rewrite must reproduce it exactly, {@link Verdict#FLIP} means the rewrite deliberately changes it and
 * the case records both the current result and the intended one. Before the rewrite every case asserts the CURRENT
 * result, so this suite is green against the shipping code. The rewrite then flips exactly the {@code FLIP} rows,
 * which makes the behavioural diff one reviewable list instead of scattered edits across test files.
 *
 * <p>The point is that {@code GlobMatcher} is on the path of every object every dataset lists, and it had nine
 * distinct patterns under test. A defect here is silent wrong rows, so the rewrite needs a baseline it cannot
 * quietly drift from.
 */
public class GlobLanguageCharacterizationTests extends ESTestCase {

    private enum Verdict {
        /** Correct today; the rewrite must preserve it. */
        KEEP,
        /** Wrong today; the rewrite changes it. {@code intended} records what it becomes. */
        FLIP
    }

    private record Case(String glob, String path, Boolean today, Verdict verdict, Boolean intended, String note) {}

    private static final List<Case> CASES = new ArrayList<>();

    private static void keep(String glob, String path, boolean today, String note) {
        CASES.add(new Case(glob, path, today, Verdict.KEEP, today, note));
    }

    private static void flip(String glob, String path, boolean today, boolean intended, String note) {
        CASES.add(new Case(glob, path, today, Verdict.FLIP, intended, note));
    }

    static {
        // -- `*` : one segment, never crosses a separator. Correct, and agrees with git and ClickHouse.
        keep("*.parquet", "file.parquet", true, "star matches within a segment");
        keep("*.parquet", "dir/file.parquet", false, "star does not cross a separator");
        keep("*", "", true, "star matches the empty string");
        keep("a*b", "ab", true, "star matches zero characters");
        keep("data-*-out.csv", "data-2024-out.csv", true, "star in the middle of a segment");
        keep("data-*-out.csv", "data-a/b-out.csv", false, "star in the middle still does not cross");

        // -- `?` : exactly one character, never a separator.
        keep("file?.parquet", "file1.parquet", true, "question matches one character");
        keep("file?.parquet", "file.parquet", false, "question does not match zero characters");
        keep("a?b", "a/b", false, "question does not match a separator");

        // -- `**` as a whole segment. The count is right; the BOUNDARY is the defect.
        keep("**/*.parquet", "a/b/x.parquet", true, "recursive form, the documented idiom");
        keep("**/*.parquet", "x.parquet", true, "leading ** allows zero directories");
        keep("**", "a/b/x.parquet", true, "bare ** crosses separators (matches ClickHouse)");
        keep("a/**", "a/b/c.csv", true, "trailing ** is recursive below the anchor");
        keep("a/**" + "/b", "a/x/b", true, "middle ** spans a directory");
        keep("a/**" + "/b", "a/b", true, "middle ** allows zero directories");
        flip("**/events.csv", "old_events.csv", true, false, "DEFECT: leading ** compiles unanchored, so it can stop mid-name");
        flip("**/events.csv", "xevents.csv", true, false, "DEFECT: same, any filename ending in the literal");
        keep("**/events.csv", "events.csv", true, "must still match at depth zero after the fix");
        keep("**/events.csv", "a/b/events.csv", true, "must still match at depth after the fix");

        // -- character classes. Ours, not ClickHouse's; a deliberate documented extension.
        keep("file[123].parquet", "file2.parquet", true, "class matches one of the set");
        keep("file[123].parquet", "file4.parquet", false, "class rejects outside the set");
        keep("part-[0-9].csv", "part-7.csv", true, "class ranges");
        keep("file[!0-9].txt", "filea.txt", true, "negated class matches outside the set");
        keep("file[!0-9].txt", "file1.txt", false, "negated class rejects inside the set");
        flip("x[!a]y", "x/y", true, false, "DEFECT: a negated class matches the separator, silently crossing a segment");

        // -- brace alternation.
        keep("*.{parquet,csv}", "a.csv", true, "alternation picks either branch");
        keep("*.{parquet,csv}", "a.json", false, "alternation rejects a third value");
        flip("{a*,b}.csv", "ax.csv", false, true, "DEFECT: a metacharacter inside an alternative leaks as raw regex");
        flip("{a*,b}.csv", ".csv", true, false, "DEFECT: the same leak makes the empty string match");

        // -- numeric ranges: the two engines disagree with each other today.
        flip("{1..3}.csv", "2.csv", false, true, "DEFECT: GlobMatcher reads the range literally; the brace fast path expands it");
        flip("{1..3}.csv", "1..3.csv", true, false, "DEFECT: the same, from the other side");

        // -- literals and the absence of an escape character.
        keep("a.b", "a.b", true, "dot is a literal, not a regex wildcard");
        keep("a.b", "axb", false, "dot really is escaped in the emitted regex");
        keep("a\\b", "a\\b", true, "backslash is a literal, matching ClickHouse; there is no escape character");
        keep("a+b", "a+b", true, "regex metacharacters outside the glob vocabulary are literals");
        keep("a$b", "a$b", true, "same for dollar");
        keep("(a)", "(a)", true, "same for parentheses");
    }

    /** Every labelled case, asserted against the CURRENT behaviour. Green on the shipping code by construction. */
    public void testCharacterizedBehaviourMatchesToday() {
        List<String> failures = new ArrayList<>();
        for (Case c : CASES) {
            boolean actual;
            try {
                actual = new GlobMatcher(c.glob()).matches(c.path());
            } catch (RuntimeException e) {
                failures.add(c.glob() + " vs " + c.path() + " threw " + e.getClass().getSimpleName() + " (" + c.note() + ")");
                continue;
            }
            if (actual != c.today()) {
                failures.add(
                    "["
                        + c.verdict()
                        + "] "
                        + c.glob()
                        + " vs "
                        + c.path()
                        + ": recorded "
                        + c.today()
                        + ", got "
                        + actual
                        + " — "
                        + c.note()
                );
            }
        }
        assertTrue("characterized behaviour drifted:\n  " + String.join("\n  ", failures), failures.isEmpty());
    }

    /** The ledger the rewrite is measured against: what changes, and what must not. */
    public void testEveryCaseCarriesAVerdict() {
        for (Case c : CASES) {
            assertNotNull(c.glob() + ": every case needs a verdict", c.verdict());
            assertNotNull(c.glob() + ": every case needs an intended result", c.intended());
            if (c.verdict() == Verdict.KEEP) {
                assertEquals("a KEEP case must not change: " + c.glob(), c.today(), c.intended());
            } else {
                assertNotEquals("a FLIP case must actually change: " + c.glob(), c.today(), c.intended());
                assertTrue("a FLIP case must say why: " + c.glob(), c.note().startsWith("DEFECT"));
            }
        }
        assertFalse("the ledger must not be empty", CASES.isEmpty());
    }

    // -- inputs that throw today. Each becomes either a working pattern or a deliberate validation error.

    public void testUnterminatedClassIsSilentlyAutoClosedToday() {
        // DEFECT: a typo becomes a different pattern rather than an error.
        assertTrue(new GlobMatcher("file[abc").matches("filea"));
    }

    public void testLiteralOpenBracketFailsToCompileToday() {
        // DEFECT, and self-inflicted: escapeGlobMeta emits exactly this shape to escape a literal '[' in a
        // partition value, so hint-driven rewriting can generate a pattern this matcher rejects.
        expectThrows(PatternSyntaxException.class, () -> new GlobMatcher("x[[]y"));
    }

    public void testPosixClassSyntaxSilentlyMatchesTheWrongCharactersToday() {
        // DEFECT: [[:digit:]] is read as a class of the characters in ":digt", not as digits.
        GlobMatcher m = new GlobMatcher("[[:digit:]]");
        assertFalse("a digit does not match", m.matches("5"));
        assertTrue("but a letter from the class name does", m.matches("d"));
    }

    public void testClassIntersectionLeaksFromJavaRegexToday() {
        // DEFECT: '&&' is Java's set-intersection operator, so this silently matches nothing.
        GlobMatcher m = new GlobMatcher("[a&&b]");
        assertFalse(m.matches("a"));
        assertFalse(m.matches("b"));
        assertFalse(m.matches("&"));
    }

    // -- the two engines. A brace-only pattern takes BraceExpander's exists()-based fast path instead of the
    // matcher, so the language a dataset actually speaks depends on which path its pattern happens to take.
    // Any disagreement here is a defect by definition: one pattern, two meanings.

    public void testTheTwoEnginesAgreeOnPlainAlternation() {
        assertTrue(BraceExpander.isBraceOnly("{a,b}.csv"));
        List<String> expanded = BraceExpander.expand("{a,b}.csv", 100);
        assertNotNull("brace-only patterns must expand", expanded);
        for (String candidate : expanded) {
            assertTrue("the matcher must accept what the expander produced: " + candidate, new GlobMatcher("{a,b}.csv").matches(candidate));
        }
    }

    /**
     * DEFECT: they do NOT agree on numeric ranges. The expander produces 1.csv, 2.csv, 3.csv; the matcher reads
     * the pattern as the literal text "1..3". So {@code {1..3}.csv} and {@code {1..3}*.csv} mean different things
     * today purely because the second is not brace-only and takes the other engine.
     */
    public void testTheTwoEnginesDisagreeOnNumericRanges() {
        List<String> expanded = BraceExpander.expand("{1..3}.csv", 100);
        assertEquals(List.of("1.csv", "2.csv", "3.csv"), expanded);
        GlobMatcher matcher = new GlobMatcher("{1..3}.csv");
        for (String candidate : expanded) {
            assertFalse("the matcher rejects what the expander produced: " + candidate, matcher.matches(candidate));
        }
        assertTrue("the matcher instead matches the literal text", matcher.matches("1..3.csv"));
    }

    // -- fuzz. A fixed alphabet over the characters the language actually branches on, so the rewrite has a
    // corpus it must reproduce or explain. Failures print the pattern, which is what makes them actionable.

    public void testFuzzedPatternsEitherMatchDeterministicallyOrFailToCompile() {
        int compiled = 0;
        int threw = 0;
        for (int i = 0; i < 2000; i++) {
            String glob = randomGlob();
            GlobMatcher m;
            try {
                m = new GlobMatcher(glob);
                compiled++;
            } catch (RuntimeException e) {
                // Recorded, not asserted away: which inputs throw is part of today's contract and several of
                // these become deliberate validation errors rather than crashes.
                threw++;
                continue;
            }
            String path = randomPath();
            boolean first = m.matches(path);
            assertEquals("matching must be deterministic for [" + glob + "] vs [" + path + "]", first, m.matches(path));
            assertEquals("a fresh matcher must agree with the first for [" + glob + "]", first, new GlobMatcher(glob).matches(path));
        }
        assertTrue("the fuzz alphabet must actually produce compilable patterns", compiled > 0);
        assertTrue("and must actually reach the throwing cases, or it is not exercising the parser", threw > 0);
    }

    private String randomGlob() {
        int len = randomIntBetween(1, 10);
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(randomFrom('*', '?', '[', ']', '{', '}', ',', '/', '!', '^', '-', '.', 'a', 'b', '0', '9', '=', '_'));
        }
        return sb.toString();
    }

    private String randomPath() {
        int segments = randomIntBetween(1, 3);
        StringBuilder sb = new StringBuilder();
        for (int s = 0; s < segments; s++) {
            if (s > 0) {
                sb.append('/');
            }
            int len = randomIntBetween(0, 5);
            for (int i = 0; i < len; i++) {
                sb.append(randomFrom('a', 'b', '0', '9', '.', '_', '=', '-', '[', ']'));
            }
        }
        return sb.toString();
    }

    /** {@code needsRecursion} drives whether the listing is recursive, so it is part of the contract. */
    public void testNeedsRecursionIsDrivenByTheDoubleStar() {
        assertTrue(new GlobMatcher("**/*.parquet").needsRecursion());
        assertTrue(new GlobMatcher("a/**").needsRecursion());
        assertFalse(new GlobMatcher("*.parquet").needsRecursion());
        assertFalse(new GlobMatcher("a/*/b").needsRecursion());
    }
}
