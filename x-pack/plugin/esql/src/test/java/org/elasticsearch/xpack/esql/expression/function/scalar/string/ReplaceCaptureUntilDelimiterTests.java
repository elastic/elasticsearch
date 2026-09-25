/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end and white-box tests for {@link ReplaceCaptureUntilDelimiter}, the idiom-detected byte-scan
 * fast path for {@link Replace} (see that class's javadoc). End-to-end tests come first since they're
 * more readily understood; the white-box tests further down exercise {@link ReplaceCaptureUntilDelimiter}'s
 * internals directly.
 */
@com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 10)
public class ReplaceCaptureUntilDelimiterTests extends ComputeTestCase {

    // The line terminators recognized by java.util.regex.Pattern's default (non-UNIX_LINES) mode.
    private static final String[] LINE_TERMINATORS = { "\n", "\r", "\u0085", "\u2028", "\u2029" };

    private static final String URL_REGEX = "^https?://(?:www\\.)?([^/]+)/.*$";
    // Two independent optional prefix segments: scheme and www. can each be present or absent.
    private static final String URL_OPTIONAL_SCHEME_REGEX = "^(?:https://)?(?:www\\.)?([^/]+)/.*$";

    private record CommonPatternCase(String regex, String newStr, String input, String expected, String description) {}

    /**
     * Common real-world patterns of the recipe: a fixed (possibly-optional) literal prefix, one capture
     * up to the first occurrence of a single-byte delimiter, and anything after. Shared by the
     * evaluator-selection test below and the direct fast-path-vs-real-regex-engine differential test.
     */
    private static List<CommonPatternCase> commonPatternCases() {
        // tag::noformat
        return List.of(
            new CommonPatternCase(URL_REGEX,                        "$1",     "http://example.com/a",        "example.com",    "URL host, http(s) + optional www."),
            new CommonPatternCase(URL_REGEX,                        "$1",     "https://www.example.com/x/y", "example.com",    "URL host, with www. prefix present"),
            new CommonPatternCase(URL_REGEX,                        "$1",     "https://wwwexample.com/x/y",  "wwwexample.com", "URL hostname with www prefix"),
            new CommonPatternCase(URL_REGEX,                        "$1",     "https://example.com/",        "example.com",    "URL host, bare trailing slash"),
            new CommonPatternCase(URL_OPTIONAL_SCHEME_REGEX,        "$1",     "https://www.example.com/x",   "example.com",    "URL host, scheme and www. both present"),
            new CommonPatternCase(URL_OPTIONAL_SCHEME_REGEX,        "$1",     "example.com/x",               "example.com",    "URL host, scheme and www. both omitted"),
            new CommonPatternCase("^([^@]+)@.*$",                   "$1",     "alice@example.com",           "alice",          "email username"),
            new CommonPatternCase("^([^=]+)=.*$",                   "$1",     "retries=3",                   "retries",        "key from a key=value pair"),
            new CommonPatternCase("^([^,]+),.*$",                   "$1",     "a,b,c",                       "a",              "first CSV field"),
            new CommonPatternCase("^([^\\.]+)\\..*$",               "$1.bak", "archive.tar.gz",              "archive.bak",    "base filename, literal suffix"),
            new CommonPatternCase("^JSESSIONID=([^;]+);.*$",        "sid=$1!", "JSESSIONID=abc123; Path=/",  "sid=abc123!",    "cookie value with literal prefix and suffix"),
            new CommonPatternCase("^/api/v1/(?:beta/)?([^/]+)/.*$", "$1",     "/api/v1/beta/svc/x",          "svc",            "path segment, skips optional version prefix"),
            // Scheme required, but written as a non-capturing group instead of a plain literal.
            new CommonPatternCase("^(?:https://)([^/]+)/.*$",       "$1",     "https://example.com/x",       "example.com",    "required (non-optional) (?:...) prefix segment"),
            new CommonPatternCase("^Mozilla/5\\.0 \\(([^;]+);.*$",  "$1",     "Mozilla/5.0 (X11; Linux)",    "X11",            "user-agent token inside parens"),
            // Same pattern, anchored with \A instead of ^.
            new CommonPatternCase("\\A([^/]+)/.*$",                 "$1",     "host/path-tail",              "host",           "\\A start anchor instead of ^"),
            // A literal prefix containing dots, quoted instead of individually escaped.
            new CommonPatternCase("^\\Qapi.v1.\\E([^/]+)/.*$",      "$1",     "api.v1.svc/tail",             "svc",            "\\Q...\\E quoted literal prefix"),
            // An empty quoted section, equivalent to having no prefix at all.
            new CommonPatternCase("^\\Q\\E([^/]+)/.*$",             "$1",     "host/tail",                   "host",           "empty \\Q\\E section"),
            // A literal prefix character outside the ASCII range.
            new CommonPatternCase("^\uD83D\uDE00([^/]+)/.*$",       "$1",     "\uD83D\uDE00host/tail",       "host",           "supplementary-plane literal (surrogate pair) prefix char"),
            // A non-greedy quantifier on the capture group itself; it still has only one valid split point.
            new CommonPatternCase("^([^/]+?)/.*$",                  "$1",     "hostname/tail",               "hostname",       "lazy quantifier on the capture group"),
            // Exactly MAX_OPTIONAL_PREFIX_PARTS independent optional segments, all present.
            new CommonPatternCase("^a?b?c?d?([^/]+)/.*$",           "$1",     "abcdhost/tail",               "host",           "four independent optional prefix segments")
        );
        // end::noformat
    }

    public void testExtractsCommonPatterns() {
        for (CommonPatternCase c : commonPatternCases()) {
            forEachTailAnchoring(
                c.regex(),
                regex -> assertThat(c.description(), processConstantRegexAndNewStr(c.input(), regex, c.newStr()), equalTo(c.expected()))
            );
        }

        // No match cases -- REPLACE returns the input unchanged, exactly like the regex path.
        forEachTailAnchoring(URL_REGEX, regex -> {
            assertThat(processConstantRegexAndNewStr("ftp://example.com/a", regex, "$1"), equalTo("ftp://example.com/a"));
            assertThat(processConstantRegexAndNewStr("http://example.com", regex, "$1"), equalTo("http://example.com"));
            assertThat(processConstantRegexAndNewStr("http:///x", regex, "$1"), equalTo("http:///x"));
            assertThat(processConstantRegexAndNewStr("", regex, "$1"), equalTo(""));
        });
    }

    public void testProcessReturnsNullForNullInput() {
        ReplaceCaptureUntilDelimiter.Idiom idiom = ReplaceCaptureUntilDelimiter.extract(Pattern.compile(URL_REGEX), new BytesRef("$1"));
        assertNotNull(idiom);
        assertNull(ReplaceCaptureUntilDelimiter.process(null, idiom));
    }

    /**
     * Checks that the byte-scan fast path produces the same result as the real regex engine, for each of
     * {@link #commonPatternCases}, by calling {@link ReplaceCaptureUntilDelimiter#process} and
     * {@link Replace#safeReplace} directly and comparing their output.
     */
    public void testFastPathAgreesWithRealRegexEngineForCommonPatterns() {
        for (CommonPatternCase c : commonPatternCases()) {
            Pattern pattern = Pattern.compile(c.regex());
            BytesRef newStrRef = new BytesRef(c.newStr());
            ReplaceCaptureUntilDelimiter.Idiom idiom = ReplaceCaptureUntilDelimiter.extract(pattern, newStrRef);
            assertNotNull(c.description(), idiom);
            BytesRef input = new BytesRef(c.input());
            BytesRef groundTruth = Replace.safeReplace(input, pattern, newStrRef);
            assertThat(c.description(), ReplaceCaptureUntilDelimiter.process(input, idiom), equalTo(groundTruth));
        }
    }

    /**
     * Checks that the byte-scan fast path produces the same result as the real regex engine, for many
     * random inputs against several representative idiom shapes, by calling
     * {@link ReplaceCaptureUntilDelimiter#process} and {@link Replace#safeReplace} directly and comparing
     * their output.
     */
    public void testFastPathAgreesWithRealRegexEngineOnRandomInput() {
        record Shape(String regex, int flags, String newStr, Supplier<String> randomInput) {
            Shape(String regex, String newStr, Supplier<String> randomInput) {
                this(regex, 0, newStr, randomInput);
            }
        }
        List<Shape> shapes = List.of(
            new Shape(URL_REGEX, "$1", ReplaceCaptureUntilDelimiterTests::randomUrlInput),
            new Shape("^([^,]+),.*$", "name=$1!", ReplaceCaptureUntilDelimiterTests::randomLiteralAroundGroupInput),
            // Multiple independent optional prefix segments, which can require backtracking across more
            // than one at once.
            new Shape("^(?:a)?(?:b)?(?:c)?([^/]+)/.*$", "$1", ReplaceCaptureUntilDelimiterTests::randomMultiOptionalPrefixInput),
            // DOTALL enabled: embedded newlines must still match correctly.
            new Shape("^([^/]+)/.*$", Pattern.DOTALL, "$1", ReplaceCaptureUntilDelimiterTests::randomHostSlashTailInput),
            // No trailing $ anchor.
            new Shape("^([^/]+)/.*", "$1", ReplaceCaptureUntilDelimiterTests::randomHostSlashTailInput)
        );
        int iterationsPerShape = atLeast(200);
        for (Shape shape : shapes) {
            Pattern pattern = Pattern.compile(shape.regex(), shape.flags());
            BytesRef newStrRef = new BytesRef(shape.newStr());
            ReplaceCaptureUntilDelimiter.Idiom idiom = ReplaceCaptureUntilDelimiter.extract(pattern, newStrRef);
            assertNotNull(shape.regex(), idiom);
            for (int i = 0; i < iterationsPerShape; i++) {
                BytesRef input = new BytesRef(shape.randomInput().get());
                BytesRef groundTruth = Replace.safeReplace(input, pattern, newStrRef);
                assertThat(
                    shape.regex() + " / " + input.utf8ToString(),
                    ReplaceCaptureUntilDelimiter.process(input, idiom),
                    equalTo(groundTruth)
                );
            }
        }
    }

    private static String randomUrlInput() {
        // Includes schemes the regex doesn't match, for a no-match case too.
        String scheme = randomFrom("http", "https", "ftp", "ftps");
        String www = randomBoolean() ? "www." : "";
        // Occasionally empty, so an empty host right after "www." forces backtracking (see
        // testBacktracksOptionalPrefix).
        String host = randomBoolean() ? "" : randomAlphaOfLengthBetween(1, 12);
        // Occasionally omit the delimiter, for a no-match case too.
        return randomBoolean() ? scheme + "://" + www + host + "/" + randomTailContent() : scheme + "://" + www + host;
    }

    private static String randomMultiOptionalPrefixInput() {
        String prefix = "";
        for (String segment : new String[] { "a", "b", "c" }) {
            if (randomBoolean()) {
                prefix += segment;
            }
        }
        // Occasionally empty, so an empty host forces backtracking across more than one optional segment.
        String host = randomBoolean() ? "" : randomAlphaOfLengthBetween(1, 12);
        // Occasionally omit the delimiter, for a no-match case too.
        return randomBoolean() ? prefix + host + "/" + randomTailContent() : prefix + host;
    }

    private static String randomHostSlashTailInput() {
        String host = randomBoolean() ? "" : randomAlphaOfLengthBetween(1, 12);
        // Occasionally omit the delimiter, for a no-match case too.
        return randomBoolean() ? host + "/" + randomTailContent() : host;
    }

    private static String randomLiteralAroundGroupInput() {
        String body = randomAlphaOfLengthBetween(0, 12);
        // Occasionally omit the delimiter, for a no-match case too.
        return randomBoolean() ? body + "," + randomTailContent() : body;
    }

    /** Random tail content that occasionally embeds a line terminator, to exercise that fallback too. */
    private static String randomTailContent() {
        String base = randomAlphaOfLengthBetween(0, 20);
        if (rarely()) {
            int pos = randomIntBetween(0, base.length());
            base = base.substring(0, pos) + randomFrom(LINE_TERMINATORS) + base.substring(pos);
        }
        return base;
    }

    public void testBacktracksOptionalPrefix() {
        // Greedily consuming the optional "www." would leave an empty capture ("www./x" -> host starts
        // right at the delimiter), which isn't a valid match; the real regex backtracks to capture
        // "www." itself instead.
        forEachTailAnchoring("^(?:www\\.)?([^/]+)/.*$", regex -> {
            assertThat(processConstantRegexAndNewStr("www./x", regex, "$1"), equalTo("www."));
            assertThat(processConstantRegexAndNewStr("www.example.com/x", regex, "$1"), equalTo("example.com"));
            // Same backtracked match, but with a literal prefix and suffix around $1 too.
            assertThat(processConstantRegexAndNewStr("www./x", regex, "host=$1!"), equalTo("host=www.!"));
        });
    }

    public void testLiteralAroundGroup() {
        forEachTailAnchoring("^([^,]+),.*$", regex -> {
            assertThat(processConstantRegexAndNewStr("alice,30,nyc", regex, "name=$1!"), equalTo("name=alice!"));
            assertThat(processConstantRegexAndNewStr("chris,dub", regex, "name=$1"), equalTo("name=chris"));
            assertThat(processConstantRegexAndNewStr("bob,50", regex, "$1!"), equalTo("bob!"));
            assertThat(processConstantRegexAndNewStr("no-comma-here", regex, "name=$1!"), equalTo("no-comma-here"));
            assertThat(processConstantRegexAndNewStr("chris", regex, "name=!!!$1!!!"), equalTo("chris"));
        });
    }

    public void testUnicodeCapturedContent() {
        // Only the literal prefix/delimiter must be ASCII -- the captured segment itself can be
        // arbitrary UTF-8 (including multi-byte and surrogate-pair code points).
        forEachTailAnchoring("^([^/]+)/.*$", regex -> {
            assertThat(processConstantRegexAndNewStr("caf\u00e9/x", regex, "$1"), equalTo("caf\u00e9"));
            assertThat(processConstantRegexAndNewStr("caf\u00e9/", regex, "$1"), equalTo("caf\u00e9"));
            // No delimiter present -- not a capture case at all, just the no-match passthrough
            assertThat(processConstantRegexAndNewStr("caf\u00e9", regex, "$1"), equalTo("caf\u00e9"));
            assertThat(processConstantRegexAndNewStr("a\ud83d\udc05b/x", regex, "$1"), equalTo("a\ud83d\udc05b"));
            // 3-byte BMP code points (CJK).
            assertThat(processConstantRegexAndNewStr("\u4e0a\u6d77/x", regex, "$1"), equalTo("\u4e0a\u6d77"));
            // 2-byte BMP code points, right-to-left script (Hebrew).
            assertThat(processConstantRegexAndNewStr("\u05e9\u05dc\u05d5\u05dd/x", regex, "$1"), equalTo("\u05e9\u05dc\u05d5\u05dd"));
            // Decomposed (NFD) grapheme cluster -- a base letter followed by a combining diacritic,
            // i.e. two separate code points forming what looks like one character.
            assertThat(processConstantRegexAndNewStr("cafe\u0301/x", regex, "$1"), equalTo("cafe\u0301"));
            // Multiple adjacent surrogate pairs (two 4-byte code points back to back).
            assertThat(processConstantRegexAndNewStr("\ud83d\udc05\ud83d\udc06/x", regex, "$1"), equalTo("\ud83d\udc05\ud83d\udc06"));
            // ZWJ emoji sequence -- mixes 4-byte emoji code points with a 3-byte zero-width joiner
            // within a single captured grapheme cluster (family: man + ZWJ + woman + ZWJ + girl).
            assertThat(
                processConstantRegexAndNewStr("\ud83d\udc68\u200d\ud83d\udc69\u200d\ud83d\udc67/x", regex, "$1"),
                equalTo("\ud83d\udc68\u200d\ud83d\udc69\u200d\ud83d\udc67")
            );
        });
    }

    public void testFallsBackOnEmbeddedNewline() {
        // `.*$` (no DOTALL) can't cross an embedded, non-trailing newline
        final String regex = "^([^/]+)/.*$";
        final String withEmbeddedNewline = "host/pa\nth-tail";
        // A newline embedded in the *captured* segment itself (before the delimiter)
        final String newlineInCapturedSegment = "ho\nst/path-tail";
        forEachTailAnchoring(regex, r -> {
            for (String input : List.of(withEmbeddedNewline, newlineInCapturedSegment)) {
                assertThat(processConstantRegexAndNewStr(input, r, "$1"), equalTo(input.replaceAll(r, "$1")));
            }
        });

        // With DOTALL enabled.
        Pattern dotAllPattern = Pattern.compile(regex, Pattern.DOTALL);
        BytesRef dotAllNewStr = new BytesRef("$1");
        ReplaceCaptureUntilDelimiter.Idiom dotAllIdiom = ReplaceCaptureUntilDelimiter.extract(dotAllPattern, dotAllNewStr);
        assertNotNull(dotAllIdiom);
        BytesRef dotAllInput = new BytesRef(withEmbeddedNewline);
        assertThat(
            ReplaceCaptureUntilDelimiter.process(dotAllInput, dotAllIdiom),
            equalTo(Replace.safeReplace(dotAllInput, dotAllPattern, dotAllNewStr))
        );
    }

    public void testLineTerminatorScanWordBoundaries() {
        // Every line terminator, at every position and tail length, cross-checked against Java's own regex.
        final String regex = "^([^/]+)/.*$";
        final String newStr = "$1";
        for (String term : LINE_TERMINATORS) {
            for (int tailLen = 1; tailLen <= 20; tailLen++) {
                for (int pos = 0; pos < tailLen; pos++) {
                    StringBuilder tail = new StringBuilder();
                    for (int i = 0; i < tailLen; i++) {
                        tail.append(i == pos ? term : "x");
                    }
                    String input = "host/" + tail;
                    String expected = input.replaceAll(regex, newStr);
                    assertThat(
                        "term=U+" + Integer.toHexString(term.codePointAt(0)) + " tailLen=" + tailLen + " pos=" + pos,
                        processConstantRegexAndNewStr(input, regex, newStr),
                        equalTo(expected)
                    );
                }
            }
        }
        // No terminator at all, at every length -- must not false-positive.
        for (int tailLen = 0; tailLen <= 20; tailLen++) {
            String input = "host/" + "x".repeat(tailLen);
            assertThat("tailLen=" + tailLen, processConstantRegexAndNewStr(input, regex, newStr), equalTo("host"));
        }
    }

    public void testCaptureUntilDelimiterEvaluatorNotUsedWhenIdiomDoesNotMatch() {
        // 0 or multiple capturing groups -- must fall back to the existing evaluator.
        assertEvaluatorToStringContains("^abc/.*$", "$0", "ReplaceConstantOrdinalEvaluator");
        assertEvaluatorToStringContains("^(a)([^/]+)/.*$", "$2", "ReplaceConstantOrdinalEvaluator");
        assertEvaluatorToStringContains("^(a)(b)([^/]+)/.*$", "$3", "ReplaceConstantOrdinalEvaluator");
    }

    public void testEmptyRegex() {
        // Empty regex ("") is valid Pattern syntax but not our shape -- extract() bails on it directly.
        assertNull(ReplaceCaptureUntilDelimiter.extract(Pattern.compile(""), new BytesRef("-")));
        // Falls back to the existing evaluator, which must still match real empty-pattern semantics.
        assertThat(processViaOrdinaryEvaluator("abc", "", "-"), equalTo("abc".replaceAll("", "-")));
    }

    public void testEmptyNewStr() {
        // Empty newStr has no "$1" group reference at all, so extract() bails regardless of the regex.
        String regex = "^([^/]+)/.*$";
        assertNull(ReplaceCaptureUntilDelimiter.extract(Pattern.compile(regex), new BytesRef("")));
        // Falls back to the existing evaluator, which must still discard the whole match as expected.
        String text = "host/path-tail";
        assertThat(processViaOrdinaryEvaluator(text, regex, ""), equalTo(text.replaceAll(regex, "")));
    }

    // --- detection (white-box) ---

    public void testCaptureUntilDelimiterIdiomDetectsMotivatingPattern() {
        // Common pattern, e.g. `REPLACE(Referer, "^https?://(?:www\.)?([^/]+)/.*$", "$1")` -- extract URL host
        var idiom = ReplaceCaptureUntilDelimiter.extract(Pattern.compile("^https?://(?:www\\.)?([^/]+)/.*$"), new BytesRef("$1"));
        assertNotNull(idiom);
        assertThat(idiom.delimiter(), equalTo((byte) '/'));
        assertThat(idiom.originalNewStr(), equalTo(new BytesRef("$1")));
        assertFalse(idiom.prefix()[0].optional());
        assertThat(idiom.prefix()[1].literal(), equalTo("s".getBytes(UTF_8)));
        assertTrue(idiom.prefix()[1].optional());
        assertThat(idiom.prefix()[2].literal(), equalTo("://".getBytes(UTF_8)));
        assertFalse(idiom.prefix()[2].optional());
        assertThat(idiom.prefix()[3].literal(), equalTo("www.".getBytes(UTF_8)));
        assertTrue(idiom.prefix()[3].optional());

        idiom = ReplaceCaptureUntilDelimiter.extract(Pattern.compile("^https://([^/]+)/.*$"), new BytesRef("https://$1x"));
        assertNotNull(idiom);
        assertThat(idiom.delimiter(), equalTo((byte) '/'));
        assertThat(idiom.originalNewStr(), equalTo(new BytesRef("https://$1x")));
        assertThat(idiom.prefix(), arrayWithSize(1));
        assertFalse(idiom.prefix()[0].optional());
        assertThat(idiom.prefix()[0].literal(), equalTo("https://".getBytes(UTF_8)));
        assertThat(idiom.replacementPrefix(), equalTo("https://".getBytes(UTF_8)));
        assertThat(idiom.replacementSuffix(), equalTo("x".getBytes(UTF_8)));
    }

    /**
     * A trailing '?' right after a \Q...\E section must split the prefix at the \Q boundary, not pull in
     * whatever character preceded \Q too -- even in the pathological case of a lone UTF-16 surrogate
     * immediately before \Q that would otherwise combine with the first (also lone) surrogate inside the
     * quoted section into what looks like one codepoint.
     */
    public void testExtractQuotedSectionLastAtomNeverCrossesChunkBoundary() {
        String regex = "^" + '\uD83D' + "\\Q" + '\uDE00' + "\\E?([^/]+)/.*$";
        var idiom = ReplaceCaptureUntilDelimiter.extract(Pattern.compile(regex), new BytesRef("$1"));
        assertNotNull(idiom);
        // Two separate prefix parts, split right at the \Q boundary: the lone high surrogate is required,
        // the lone low surrogate from inside \Q...\E is optional. Each encodes to the JDK's UTF-8
        // substitution byte ('?', 0x3F).
        assertThat(idiom.prefix(), arrayWithSize(2));
        assertFalse(idiom.prefix()[0].optional());
        assertThat(idiom.prefix()[0].literal(), equalTo(new byte[] { '?' }));
        assertTrue(idiom.prefix()[1].optional());
        assertThat(idiom.prefix()[1].literal(), equalTo(new byte[] { '?' }));
    }

    public void testExtractAcceptsValidShapeVariants() {
        record Case(String regex, String newStr, String description) {}
        List<Case> cases = List.of(
            new Case("^([^/]+)/.*", "$1", "bare trailing wildcard, no $ anchor"),
            new Case("\\A([^/]+)/.*$", "$1", "\\A start anchor instead of ^"),
            new Case("^([^,]+),.*$", "name=$1!", "literal text before and after the group"),
            // Other common real-world patterns of the same recipe: a fixed (possibly-optional) literal
            // prefix, one capture up to the first occurrence of a single-byte delimiter, and anything after.
            new Case("^([^@]+)@.*$", "$1", "email username"),
            new Case("^([^=]+)=.*$", "$1", "key from a key=value pair"),
            new Case("^([^,]+),.*$", "$1", "first CSV field"),
            new Case("^([^\\.]+)\\..*$", "$1", "base filename, strip extension (escaped '.' delimiter)"),
            new Case("^JSESSIONID=([^;]+);.*$", "sid=$1", "cookie value with a literal name prefix"),
            new Case("^/api/v1/(?:beta/)?([^/]+)/.*$", "$1", "path segment, skipping an optional version prefix"),
            new Case("^Mozilla/5\\.0 \\(([^;]+);.*$", "$1", "user-agent token inside parens")
        );
        for (Case c : cases) {
            assertNotNull(c.description(), ReplaceCaptureUntilDelimiter.extract(Pattern.compile(c.regex()), new BytesRef(c.newStr())));
        }
    }

    /**
     * extract() is conservative by design (see its javadoc): every one of these is a different reason it
     * bails ({@code null}) rather than risk approximating a shape it can't fully verify.
     */
    public void testExtractBailsOnUnsupportedShapes() {
        record Case(String regex, int flags, String newStr, String description) {
            Case(String regex, String newStr, String description) {
                this(regex, 0, newStr, description);
            }
        }
        List<Case> cases = List.of(
            new Case("([^/]+)/.*$", "$1", "missing ^ / \\A anchor"),
            new Case("^", "$1", "bare anchor, nothing else"),
            new Case("^a|([^/]+)/.*$", "$1", "top-level alternation invalidates anchoring"),
            new Case("^(?:a|b)([^/]+)/.*$", "$1", "alternation inside a group also bails"),
            new Case("^([^/]+)/.*$", Pattern.CASE_INSENSITIVE, "$1", "CASE_INSENSITIVE flag"),
            new Case("^([^/]+)/.*$", Pattern.MULTILINE, "$1", "MULTILINE flag"),
            new Case("^([^/]+)/.*$", Pattern.UNIX_LINES, "$1", "UNIX_LINES flag"),
            new Case("^(?d)([^/]+)/.*$", "$1", "inline (?d) UNIX_LINES modifier"),
            new Case("^([^/]+)/.*$", Pattern.UNICODE_CASE, "$1", "UNICODE_CASE flag"),
            new Case("^([^/]+)/.*$", Pattern.COMMENTS, "$1", "COMMENTS flag"),
            new Case("^([^/]+)/.*$", Pattern.CANON_EQ, "$1", "CANON_EQ flag"),
            new Case("^([^/]+)/.*$", Pattern.LITERAL, "$1", "LITERAL flag"),
            new Case("^(?:ab*)([^/]+)/.*$", "$1", "non-literal content inside a (?:...) prefix group"),
            new Case("^(?:a[(]b)([^/]+)/.*$", "$1", "paren hidden inside a character class confuses matchingParen"),
            new Case("^\\Qabc", "$1", "unterminated \\Q...\\E with no capturing group after it"),
            new Case("^\\Q", "$1", "unterminated \\Q with nothing after it at all"),
            new Case("^\\d([^/]+)/.*$", "$1", "regex shorthand class (\\d) in the prefix"),
            new Case("^a??([^/]+)/.*$", "$1", "second '?' with no preceding atom (lazy quantifier)"),
            new Case("^a*([^/]+)/.*$", "$1", "unsupported prefix quantifier: *"),
            new Case("^a+([^/]+)/.*$", "$1", "unsupported prefix quantifier: +"),
            new Case("^a{1,2}([^/]+)/.*$", "$1", "unsupported prefix quantifier: {1,2}"),
            new Case("^abc/.*$", "$1", "no capturing group at all"),
            new Case("^(a)([^/]+)/.*$", "$2", "more than one capturing group"),
            new Case("^(a)(b)([^/]+)/.*$", "$3", "three capturing groups"),
            new Case("^(a)(b)(c)([^/]+)/.*$", "$4", "four capturing groups"),
            new Case("^(?<host>[^/]+)/.*$", "$1", "named group instead of a plain capturing group"),
            new Case("^(abc)/.*$", "$1", "capturing group isn't a negated character class"),
            new Case("^()", "$1", "empty capturing group"),
            new Case("^([^.]+).*$", "$1", "unescaped meta char as the delimiter"),
            new Case("^([^\\d]+)/.*$", "$1", "regex shorthand class (\\d) as the delimiter"),
            new Case("^([^ab]+)a.*$", "$1", "more than one char in the negated class"),
            new Case("^([^/])/.*$", "$1", "missing + quantifier after the negated class"),
            new Case("^([^/]+[^,]+)/.*$", "$1", "extra content inside the group before the closing paren"),
            new Case("^([^\u00e9]+)\u00e9.*$", "$1", "non-ASCII delimiter"),
            new Case("^([^/]+),.*$", "$1", "delimiter mismatch after group"),
            new Case("^([^/]+).*$", "$1", "no literal delimiter repeat immediately after the group"),
            new Case("^([^/]+)\\d.*$", "$1", "regex shorthand class (\\d) as the literal repeat after the group"),
            new Case("^([^/]+)/x.*$", "$1", "content between the delimiter repeat and .*"),
            new Case("^([^/]+)/.*x$", "$1", "trailing content after tail wildcard"),
            new Case("^a?b?c?d?e?([^/]+)/.*$", "$1", "too many optional prefix segments"),
            new Case("^([^/]+)/.*$", "$10", "ambiguous replacement: $10 could be group 10"),
            new Case("^([^/]+)/.*$", "$1$1", "ambiguous replacement: more than one group reference"),
            new Case("^([^/]+)/.*$", "\\$1", "ambiguous replacement: backslash escape"),
            new Case("^([^/]+)/.*$", "no group ref", "ambiguous replacement: no group reference")
        );
        for (Case c : cases) {
            assertNull(
                c.description(),
                ReplaceCaptureUntilDelimiter.extract(Pattern.compile(c.regex(), c.flags()), new BytesRef(c.newStr()))
            );
        }
    }

    // --- internal parsing helpers (white-box) ---

    public void testMatchingParenFindsCorrectClose() {
        assertMatchingParenBody("abc", "tail");
        assertMatchingParenBody("", "tail");
        // Nested parens must not make matchingParen stop early at an inner one.
        assertMatchingParenBody("a(b)c", "tail");
        assertMatchingParenBody("(a)(b)", "tail");
        // An escaped, or \Q...\E-quoted, ')' must not be mistaken for the real closing paren.
        assertMatchingParenBody("a\\)b", "tail");
        assertMatchingParenBody("\\Q)(\\Eb", "tail");
    }

    public void testMatchingParenUnterminated() {
        assertEquals(-1, ReplaceCaptureUntilDelimiter.matchingParen("(?:abc", 3));
        assertEquals(-1, ReplaceCaptureUntilDelimiter.matchingParen("(?:a(b)c", 3)); // nested paren also unbalanced
    }

    /** Builds {@code "(?:" + body + ")" + trailing} and asserts matchingParen finds body's exact end. */
    private static void assertMatchingParenBody(String body, String trailing) {
        String regex = "(?:" + body + ")" + trailing;
        int from = 3; // right after "(?:"
        assertEquals("body=[" + body + "]", from + body.length(), ReplaceCaptureUntilDelimiter.matchingParen(regex, from));
    }

    public void testParsePureLiteralParsesLiteralContent() {
        record Case(String regex, String expected, String description) {}
        List<Case> cases = List.of(
            new Case("abc", "abc", "plain literal"),
            new Case("a\\.c", "a.c", "escaped meta char"),
            new Case("\\Qa.b\\Ec", "a.bc", "\\Q...\\E quoted section"),
            new Case("\\Qa.b", "a.b", "unterminated \\Q...\\E runs to end")
        );
        for (Case c : cases) {
            assertArrayEquals(
                c.description(),
                c.expected().getBytes(UTF_8),
                ReplaceCaptureUntilDelimiter.parsePureLiteral(c.regex(), 0, c.regex().length())
            );
        }
    }

    public void testParsePureLiteralBailsOnNonLiteralContent() {
        record Case(String regex, String description) {}
        List<Case> cases = List.of(
            new Case("a.c", "unescaped meta char"),
            new Case("a\\d", "disallowed escape (\\d is a class, not a literal escape)"),
            new Case("", "empty range")
        );
        for (Case c : cases) {
            assertNull(c.description(), ReplaceCaptureUntilDelimiter.parsePureLiteral(c.regex(), 0, c.regex().length()));
        }
    }

    public void testExtractLiteralReplacementPartsParsesValidShapes() {
        record Case(String newStr, String expectedPrefix, String expectedSuffix, String description) {}
        List<Case> cases = List.of(
            new Case("name=$1!", "name=", "!", "literal prefix and suffix around $1"),
            new Case("$1", "", "", "bare group reference"),
            new Case("sid=$1", "sid=", "", "literal prefix only, no suffix")
        );
        for (Case c : cases) {
            byte[][] parts = ReplaceCaptureUntilDelimiter.extractLiteralReplacementParts(new BytesRef(c.newStr()));
            assertArrayEquals(c.description(), c.expectedPrefix().getBytes(UTF_8), parts[0]);
            assertArrayEquals(c.description(), c.expectedSuffix().getBytes(UTF_8), parts[1]);
        }
    }

    public void testExtractLiteralReplacementPartsBailsOnUnsupportedShapes() {
        record Case(String newStr, String description) {}
        List<Case> cases = List.of(
            new Case("no group ref", "no group reference"),
            new Case("$2", "wrong group number ($2)"),
            new Case("$10", "ambiguous digit ($10 could be group 10)"),
            new Case("$1$1", "more than one group reference"),
            new Case("\\$1", "backslash escape"),
            new Case("$", "single dollar $)")
        );
        for (Case c : cases) {
            assertNull(c.description(), ReplaceCaptureUntilDelimiter.extractLiteralReplacementParts(new BytesRef(c.newStr())));
        }
    }

    public void testHasLineTerminatorFindsEachTerminator() {
        for (String term : LINE_TERMINATORS) {
            byte[] b = ("host/tail" + term + "more").getBytes(UTF_8);
            assertTrue(term, ReplaceCaptureUntilDelimiter.hasLineTerminator(b, 0, 0, b.length));
        }
    }

    public void testHasLineTerminatorFalseForNonTerminatorContent() {
        // No terminator-like bytes at all.
        byte[] plain = "host/tail-with-no-terminator".getBytes(UTF_8);
        assertFalse(ReplaceCaptureUntilDelimiter.hasLineTerminator(plain, 0, 0, plain.length));
        // U+2014 EM DASH looks similar in UTF-8 to U+2028/U+2029 but isn't itself a terminator.
        byte[] emDash = "before\u2014after".getBytes(UTF_8);
        assertFalse(ReplaceCaptureUntilDelimiter.hasLineTerminator(emDash, 0, 0, emDash.length));
    }

    public void testHasLineTerminatorResumesScanPastFalsePositive() {
        // A look-alike (EM DASH) followed later by a real terminator -- the scan must not give up early.
        byte[] b = "before\u2014middle\u2028after".getBytes(UTF_8);
        assertTrue(ReplaceCaptureUntilDelimiter.hasLineTerminator(b, 0, 0, b.length));
    }

    public void testHasLineTerminatorRespectsFromAndLenBounds() {
        byte[] b = "\nhost/tail".getBytes(UTF_8);
        assertFalse(ReplaceCaptureUntilDelimiter.hasLineTerminator(b, 0, 1, b.length)); // terminator before `from`
        byte[] b2 = "host/tail\n".getBytes(UTF_8);
        assertFalse(ReplaceCaptureUntilDelimiter.hasLineTerminator(b2, 0, 0, b2.length - 1)); // terminator excluded by `len`
        assertTrue(ReplaceCaptureUntilDelimiter.hasLineTerminator(b2, 0, 0, b2.length));
    }

    public void testHasLineTerminatorHonorsNonZeroOffset() {
        byte[] padded = "XXXXXhost/tail\nmore".getBytes(UTF_8);
        int off = 5;
        assertTrue(ReplaceCaptureUntilDelimiter.hasLineTerminator(padded, off, 0, padded.length - off));
    }

    public void testLineTerminatorLengthAtEachTerminator() {
        record Case(String term, int expectedLen) {}
        List<Case> cases = List.of(
            new Case("\n", 1),
            new Case("\r", 1),
            new Case("\u0085", 2),
            new Case("\u2028", 3),
            new Case("\u2029", 3)
        );
        for (Case c : cases) {
            byte[] b = (c.term() + "tail").getBytes(UTF_8);
            assertEquals(c.term(), c.expectedLen(), ReplaceCaptureUntilDelimiter.lineTerminatorLengthAt(b, 0, 0, b.length));
        }
    }

    public void testLineTerminatorLengthAtZeroForNonTerminatorBytes() {
        byte[] ordinary = "a".getBytes(UTF_8);
        assertEquals(0, ReplaceCaptureUntilDelimiter.lineTerminatorLengthAt(ordinary, 0, 0, ordinary.length));
        // A truncated multi-byte terminator, cut off at the end of the scanned range -- not a terminator.
        byte[] truncatedC2 = { (byte) 0xC2 };
        assertEquals(0, ReplaceCaptureUntilDelimiter.lineTerminatorLengthAt(truncatedC2, 0, 0, truncatedC2.length));
        byte[] truncatedE2 = { (byte) 0xE2, (byte) 0x80 };
        assertEquals(0, ReplaceCaptureUntilDelimiter.lineTerminatorLengthAt(truncatedE2, 0, 0, truncatedE2.length));
    }

    /**
     * Runs {@code check} once with {@code regexEndingInDollar} as given and once with its trailing
     * {@code $} stripped; both are valid shapes and must produce identical results. Don't use this for
     * tests where a trailing {@code $} genuinely changes the outcome, e.g. embedded line terminators.
     */
    private static void forEachTailAnchoring(String regexEndingInDollar, Consumer<String> check) {
        assertThat(regexEndingInDollar, endsWith("$"));
        check.accept(regexEndingInDollar);
        check.accept(regexEndingInDollar.substring(0, regexEndingInDollar.length() - 1));
    }

    private void assertEvaluatorToStringContains(String regex, String newStr, String expectedSubstring) {
        try (var eval = constantRegexAndNewStrEvaluator(regex, newStr).get(driverContext())) {
            assertThat(eval.toString(), containsString(expectedSubstring));
        }
    }

    /**
     * Runs {@code text} through the real evaluator-selection path (fast path or not, whichever
     * {@link Replace#toEvaluator} picks for this {@code regex}/{@code newStr}), and asserts:
     * <ul>
     * <li>the fast path was actually selected -- every caller of this helper is testing
     * {@link ReplaceCaptureUntilDelimiter}, so a silent fallback to the ordinary evaluator would mean the
     * test isn't exercising what it thinks it is (see {@link #testCaptureUntilDelimiterEvaluatorNotUsedWhenIdiomDoesNotMatch}
     * for the one place that deliberately checks the opposite).
     * <li>the result always agrees with {@link Replace#safeReplace} -- the real regex engine, which the
     * fast path must be byte-for-byte indistinguishable from.
     * </ul>
     * Both are a blanket safety net on top of whatever the caller separately asserts (e.g. a hand-computed
     * {@code expected} string): every call site gets them for free, so a wrong hand-computed value can't
     * silently mask a real disagreement between the two engines.
     */
    private String processConstantRegexAndNewStr(String text, String regex, String newStr) {
        String result;
        try (
            var eval = constantRegexAndNewStrEvaluator(regex, newStr).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(text))))
        ) {
            assertThat(regex + " / " + newStr, eval.toString(), containsString("ReplaceCaptureUntilDelimiterEvaluator"));
            result = block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
        BytesRef groundTruth = Replace.safeReplace(new BytesRef(text), Pattern.compile(regex), new BytesRef(newStr));
        assertThat(regex + " / " + newStr + " / " + text, result, equalTo(groundTruth.utf8ToString()));
        return result;
    }

    /**
     * The mirror image of {@link #processConstantRegexAndNewStr}, for shapes that deliberately do NOT
     * match {@link ReplaceCaptureUntilDelimiter}'s idiom (see
     * {@link #testCaptureUntilDelimiterEvaluatorNotUsedWhenIdiomDoesNotMatch}): asserts the fast path was
     * NOT selected, and that the ordinary evaluator's result still agrees with the real regex engine.
     */
    private String processViaOrdinaryEvaluator(String text, String regex, String newStr) {
        String result;
        try (
            var eval = constantRegexAndNewStrEvaluator(regex, newStr).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(text))))
        ) {
            assertThat(regex + " / " + newStr, eval.toString(), not(containsString("ReplaceCaptureUntilDelimiterEvaluator")));
            result = block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
        BytesRef groundTruth = Replace.safeReplace(new BytesRef(text), Pattern.compile(regex), new BytesRef(newStr));
        assertThat(regex + " / " + newStr + " / " + text, result, equalTo(groundTruth.utf8ToString()));
        return result;
    }

    private ExpressionEvaluator.Factory constantRegexAndNewStrEvaluator(String regex, String newStr) {
        return AbstractScalarFunctionTestCase.evaluator(
            new Replace(
                Source.EMPTY,
                field("text", DataType.KEYWORD),
                new Literal(Source.EMPTY, new BytesRef(regex), DataType.KEYWORD),
                new Literal(Source.EMPTY, new BytesRef(newStr), DataType.KEYWORD)
            )
        );
    }

    private static Page row(List<Object> values) {
        return new Page(1, BlockUtils.fromListRow(TestBlockFactory.getNonBreakingInstance(), values));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    /**
     * A {@link DriverContext} backed by {@link #blockFactory()}, whose circuit-breaker/block-release
     * checks are asserted automatically by {@link ComputeTestCase#allBreakersEmpty}.
     */
    private DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }
}
