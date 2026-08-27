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

/**
 * An exhaustive, labelled record of what the glob language does TODAY, written before the matcher is rewritten.
 *
 * <p>This is not a statement that today's behaviour is correct. Every case carries a verdict: {@link Verdict#KEEP}
 * means the rewrite must reproduce it exactly, {@link Verdict#FLIP} means the rewrite deliberately changes it and
 * the case records both the result it had before the rewrite and the intended one. The rewrite flipped exactly
 * the {@code FLIP} rows and no others — which is the property this file exists to make checkable, since the
 * behavioural diff of a matcher rewrite is otherwise scattered across every test that touches it.
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

    /**
     * Every labelled case, asserted against the INTENDED behaviour — for a {@code KEEP} row what it always did,
     * for a {@code FLIP} row the corrected result. The {@code today} column stays as the record of what each case
     * used to do, so the rewrite's behavioural diff reads as one list.
     */
    public void testLanguageMatchesTheIntendedBehaviour() {
        List<String> failures = new ArrayList<>();
        for (Case c : CASES) {
            boolean actual;
            try {
                actual = new GlobMatcher(c.glob()).matches(c.path());
            } catch (RuntimeException e) {
                failures.add(c.glob() + " vs " + c.path() + " threw " + e.getClass().getSimpleName() + " (" + c.note() + ")");
                continue;
            }
            if (actual != c.intended()) {
                failures.add(
                    "["
                        + c.verdict()
                        + "] "
                        + c.glob()
                        + " vs "
                        + c.path()
                        + ": intended "
                        + c.intended()
                        + ", got "
                        + actual
                        + " — "
                        + c.note()
                );
            }
        }
        assertTrue("behaviour diverged from the ledger:\n  " + String.join("\n  ", failures), failures.isEmpty());
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

    /** Was silently auto-closed into a different pattern, so a typo changed what a dataset read. Now it is named. */
    public void testUnterminatedClassIsRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GlobMatcher("file[abc"));
        assertTrue(e.getMessage(), e.getMessage().contains("unterminated character class"));
    }

    public void testUnterminatedBraceGroupIsRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GlobMatcher("file{a,b"));
        assertTrue(e.getMessage(), e.getMessage().contains("unterminated brace group"));
    }

    /**
     * Used to fail to compile, which mattered because {@code GlobExpander.escapeGlobMeta} emits exactly this shape
     * to neutralise a literal {@code [} in a partition value — hint-driven rewriting could generate a pattern this
     * matcher then rejected. A one-character class is also the only way to match a literal metacharacter at all,
     * since backslash is a literal in this language rather than an escape.
     */
    public void testOneCharacterClassMatchesALiteralMetacharacter() {
        assertTrue(new GlobMatcher("x[[]y").matches("x[y"));
        assertFalse(new GlobMatcher("x[[]y").matches("xy"));
        assertTrue(new GlobMatcher("a[*]b").matches("a*b"));
        assertFalse("the star is a literal here, not a wildcard", new GlobMatcher("a[*]b").matches("axb"));
        assertTrue(new GlobMatcher("a[?]b").matches("a?b"));
        assertTrue(new GlobMatcher("a[{]b").matches("a{b"));
    }

    /** Used to silently match the characters of the class name rather than digits. Unsupported syntax is now named. */
    public void testPosixClassSyntaxIsRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GlobMatcher("[[:digit:]]"));
        assertTrue(e.getMessage(), e.getMessage().contains("POSIX character classes"));
    }

    /** {@code &&} used to become Java's set-intersection operator and silently match nothing. It is three characters. */
    public void testClassIntersectionSyntaxIsNotInheritedFromJavaRegex() {
        GlobMatcher m = new GlobMatcher("[a&&b]");
        assertTrue(m.matches("a"));
        assertTrue(m.matches("b"));
        assertTrue(m.matches("&"));
        assertFalse(m.matches("c"));
    }

    // -- enumeration and matching must agree. A pattern naming a finite set of keys is resolved by probing those
    // keys with exists() rather than by listing; matching then never runs on them. So the two answers have to be
    // the same answer, or the strategy silently decides what a pattern means.

    public void testEveryEnumeratedKeyMatchesThePatternThatProducedIt() {
        for (String glob : List.of(
            "{a,b}.csv",
            "data/{a,b}.csv",
            "{a,b}/{x,y}.csv",
            "file-{1..5}.parquet",
            "file-{000..003}.parquet",
            "plain.csv",
            "{a,b}{c,d}.csv"
        )) {
            List<String> keys = new GlobMatcher(glob).enumerateKeys(100);
            assertNotNull(glob + " should enumerate", keys);
            GlobMatcher matcher = new GlobMatcher(glob);
            for (String key : keys) {
                assertTrue("enumerated [" + key + "] but the same pattern does not match it: " + glob, matcher.matches(key));
            }
        }
    }

    public void testPatternsHoldingAWildcardRefuseToEnumerate() {
        for (String glob : List.of("*.csv", "a?.csv", "[ab].csv", "{a*,b}.csv", "**", "a/**" + "/b", "{a,b}*.csv")) {
            assertNull("must be listed rather than probed: " + glob, new GlobMatcher(glob).enumerateKeys(100));
        }
    }

    /** A range wide enough to be worth listing instead degrades rather than materialising a huge candidate list. */
    public void testAnOverWideEnumerationDegradesToListing() {
        assertNotNull(new GlobMatcher("f{1..8}.csv").enumerateKeys(10));
        assertNull(new GlobMatcher("f{1..500}.csv").enumerateKeys(10));
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

    /**
     * The regex translation this replaced compiled repeated {@code **} into nested optional {@code .*} groups,
     * which backtrack exponentially in the number of globstars, and worsen with path length: ten against a
     * twenty-five segment path took seconds, and fifteen did not finish. The
     * matcher runs once per discovered object on the coordinator thread, and nothing validates a dataset's
     * resource pattern before then, so a pattern like this was a way to hang a coordinator. Matching is now a
     * memoised walk over (pattern segment, path segment), so the cost is bounded by their product.
     */
    public void testRepeatedGlobstarsDoNotBlowUp() {
        StringBuilder glob = new StringBuilder("a");
        for (int i = 0; i < 40; i++) {
            glob.append("/**");
        }
        glob.append("/X");
        StringBuilder path = new StringBuilder("a");
        for (int i = 0; i < 40; i++) {
            path.append("/b");
        }
        path.append("/Y");

        GlobMatcher matcher = new GlobMatcher(glob.toString());
        long start = System.nanoTime();
        assertFalse("the path genuinely does not match, which is the expensive case", matcher.matches(path.toString()));
        long millis = (System.nanoTime() - start) / 1_000_000;
        assertTrue("40 globstars took " + millis + "ms; this used to not terminate", millis < 1_000);
    }

    /** {@code needsRecursion} drives whether the listing is recursive, so it is part of the contract. */
    public void testNeedsRecursionIsDrivenByTheDoubleStar() {
        assertTrue(new GlobMatcher("**/*.parquet").needsRecursion());
        assertTrue(new GlobMatcher("a/**").needsRecursion());
        assertFalse(new GlobMatcher("*.parquet").needsRecursion());
        // Was false, which was the defect: a non-recursive listing returns only the prefix's immediate children,
        // so a multi-segment glob saw the directories and never the files inside them.
        assertTrue(new GlobMatcher("a/*/b").needsRecursion());
    }
}
