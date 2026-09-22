/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.junit.After;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * These tests create rows that are 1MB in size. Test classes
 * which extend AbstractScalarFunctionTestCase rerun test cases with
 * many randomized inputs. Unfortunately, tests are run with
 * limited memory, and instantiating many copies of these
 * tests with large rows causes out of memory.
 */
public class ReplaceStaticTests extends ESTestCase {

    /**
     * Stack size for catastrophic-regex evaluation. Small enough that {@code Matcher.find()}
     * overflows independently of the test JVM {@code -Xss}, large enough for evaluator setup.
     */
    private static final long CATASTROPHIC_REGEX_STACK_SIZE = 256 * 1024;

    public void testLimit() {
        int textLength = (int) ScalarFunction.MAX_BYTES_REF_RESULT_SIZE / 10;
        String text = randomAlphaOfLength((int) ScalarFunction.MAX_BYTES_REF_RESULT_SIZE / 10);
        String regex = "^(.+)$";

        // 10 times the original text + the remainder
        String extraString = "a".repeat((int) ScalarFunction.MAX_BYTES_REF_RESULT_SIZE % 10);
        assert textLength * 10 + extraString.length() == ScalarFunction.MAX_BYTES_REF_RESULT_SIZE;
        String newStr = "$0$0$0$0$0$0$0$0$0$0" + extraString;

        String result = process(text, regex, newStr);
        assertThat(result, equalTo(newStr.replaceAll("\\$\\d", text)));
    }

    /**
     * A catastrophic regex like {@code (a|aa)+$} against a long run of {@code a}s
     * used to throw {@link StackOverflowError} from {@code Matcher.find()}, which
     * is a fatal JVM error and killed the node. It must be converted to
     * {@link IllegalArgumentException} so the evaluator can warn and return null.
     * <p>
     * Evaluation runs on a child thread with a fixed stack so the overflow does not
     * depend on the test JVM's {@code -Xss}.
     */
    public void testCatastrophicRegexDoesNotThrowStackOverflowError() {
        String text = "a".repeat(10000);
        String regex = "(a|aa)+$";
        assertNull(processOnSmallStack(text, regex, "x"));
        assertDriverWarnings(
            "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalArgumentException: Pattern nesting is too deep to evaluate"
        );
    }

    /**
     * Same payload as {@link #testCatastrophicRegexDoesNotThrowStackOverflowError} but through
     * the constant-regex evaluator, which is the path taken by
     * {@code EVAL r = REPLACE(f, "(a|aa)+$", "x")}.
     */
    public void testCatastrophicConstantRegexDoesNotThrowStackOverflowError() {
        String text = "a".repeat(10000);
        String regex = "(a|aa)+$";
        assertNull(processConstantRegexOnSmallStack(text, regex, "x"));
        assertDriverWarnings(
            "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalArgumentException: Pattern nesting is too deep to evaluate"
        );
    }

    public void testInvalidConstantRegexWarnsAndReturnsNull() {
        PatternSyntaxException pse = expectThrows(PatternSyntaxException.class, () -> Pattern.compile("["));
        try (
            var eval = AbstractScalarFunctionTestCase.evaluator(
                new Replace(
                    Source.EMPTY,
                    field("text", DataType.KEYWORD),
                    new Literal(Source.EMPTY, new BytesRef("["), DataType.KEYWORD),
                    new Literal(Source.EMPTY, new BytesRef("x"), DataType.KEYWORD)
                )
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef("a"))));
        ) {
            assertTrue(block.isNull(0));
        }
        assertDriverWarnings(
            "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: " + pse.getClass().getName() + ": " + pse.getMessage()
        );
    }

    public void testTooBig() {
        String textAndNewStr = randomAlphaOfLength((int) (ScalarFunction.MAX_BYTES_REF_RESULT_SIZE / 10));
        String regex = ".";

        String result = process(textAndNewStr, regex, textAndNewStr);
        assertNull(result);
        assertDriverWarnings(
            "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalArgumentException: "
                + "Creating strings with more than ["
                + ScalarFunction.MAX_BYTES_REF_RESULT_SIZE
                + "] bytes is not supported"
        );
    }

    public void testTooBigWithGroups() {
        int textLength = (int) ScalarFunction.MAX_BYTES_REF_RESULT_SIZE / 10;
        String text = randomAlphaOfLength(textLength);
        String regex = "(.+)";

        // 10 times the original text + the remainder + 1
        String extraString = "a".repeat(1 + (int) ScalarFunction.MAX_BYTES_REF_RESULT_SIZE % 10);
        assert textLength * 10 + extraString.length() == ScalarFunction.MAX_BYTES_REF_RESULT_SIZE + 1;
        String newStr = "$0$1$0$1$0$1$0$1$0$1" + extraString;

        String result = process(text, regex, newStr);
        assertNull(result);
        assertDriverWarnings(
            "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalArgumentException: "
                + "Creating strings with more than ["
                + ScalarFunction.MAX_BYTES_REF_RESULT_SIZE
                + "] bytes is not supported"
        );
    }

    public void testLiteralPrefixSimpleAnchor() {
        assertPrefix("^http", "http");
        assertPrefix("^abc", "abc");
        assertPrefix("\\Aabc", "abc");
    }

    public void testLiteralPrefixInputAnchorEquivalentToCaret() {
        // \A is the start-of-input anchor and must behave identically to ^ for all of our walker logic,
        // including the quantifier handling that drops the preceding literal on `?` / `*` and keeps it on `+`.
        assertPrefix("\\Ahttps?://", "http");
        assertPrefix("\\Afoob+", "foob");
        assertPrefix("\\Afoob{0,3}", "foo");
    }

    public void testLiteralPrefixOptionalChar() {
        // `s?` makes the preceding `s` optional, so the prefix is only `http`.
        assertPrefix("^https?://", "http");
        // `*` is the same kind of quantifier.
        assertPrefix("^foob*ar", "foo");
        // `{0,3}` makes the preceding char optional.
        assertPrefix("^foob{0,3}", "foo");
        // `{2,3}` does NOT make it optional, but we are conservative and drop it anyway.
        assertPrefix("^foob{2,3}", "foo");
    }

    public void testLiteralPrefixOneOrMore() {
        // `+` is one-or-more, so the preceding `b` is required at that position, but any literal
        // after the quantifier lives at an unknown offset, so we stop the walk there.
        assertPrefix("^foob+", "foob");
        assertPrefix("^foob+ar", "foob");
    }

    public void testLiteralPrefixEscapedSpecials() {
        assertPrefix("^www\\.example\\.com", "www.example.com");
        assertPrefix("^\\[special", "[special");
    }

    public void testLiteralPrefixQuotedSection() {
        assertPrefix("^\\Q.*[(?)\\Etail", ".*[(?)tail");
        // Unterminated \Q runs to end of pattern.
        assertPrefix("^abc\\Q.*[(?)", "abc.*[(?)");
        // Quantifier after \E drops only the last literal of the quoted chunk.
        assertPrefix("^\\Qabc\\Ed?", "abc");
    }

    public void testLiteralPrefixNonAscii() {
        // Non-ASCII literal — should encode as UTF-8 bytes.
        byte[] expected = "café".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expected, Replace.extractLiteralPrefix(Pattern.compile("^café")));
    }

    public void testLiteralPrefixUnicodeSurrogate() {
        // Supplementary code point (tiger emoji) as a surrogate pair in the regex source.
        String tiger = "\ud83d\udc05";
        byte[] expected = ("a" + tiger).getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expected, Replace.extractLiteralPrefix(Pattern.compile("^a" + tiger)));
    }

    public void testLiteralPrefixNoAnchor() {
        // Pattern must start with ^ or \A.
        assertNoPrefix("http");
        assertNoPrefix("abc^");
        assertNoPrefix("");
    }

    public void testLiteralPrefixBailsOnAlternation() {
        // Top-level alternation invalidates anchoring.
        assertNoPrefix("^a|b");
        // Alternation inside a group also bails — conservative simplification.
        assertNoPrefix("^a(b|c)");
    }

    public void testLiteralPrefixBailsOnLeadingMeta() {
        assertNoPrefix("^[a-z]+");
        assertNoPrefix("^(group)");
        assertNoPrefix("^.foo");
        assertNoPrefix("^\\d+");
    }

    public void testLiteralPrefixBailsOnLeadingQuantifier() {
        // No literal to attach to: `?` at start of body means we have no prefix.
        assertNoPrefix("^?abc");
        assertNoPrefix("^*abc");
    }

    public void testLiteralPrefixBailsOnMultiline() {
        // With (?m), `^` matches after newlines and a byte-level prefix check would be wrong.
        assertArrayEquals(Replace.NO_LITERAL_PREFIX, Replace.extractLiteralPrefix(Pattern.compile("^abc", Pattern.MULTILINE)));
        // Same via inline flag.
        assertNoPrefix("(?m)^abc");
    }

    public void testLiteralPrefixBailsOnCaseInsensitive() {
        assertArrayEquals(Replace.NO_LITERAL_PREFIX, Replace.extractLiteralPrefix(Pattern.compile("^abc", Pattern.CASE_INSENSITIVE)));
        assertNoPrefix("(?i)^abc");
    }

    public void testLiteralPrefixBailsOnComments() {
        // (?x) / Pattern.COMMENTS makes whitespace and `#…` in the pattern source non-literal.
        assertArrayEquals(Replace.NO_LITERAL_PREFIX, Replace.extractLiteralPrefix(Pattern.compile("^abc", Pattern.COMMENTS)));
        assertNoPrefix("(?x)^abc");
    }

    public void testLiteralPrefixBailsOnCanonEq() {
        assertArrayEquals(Replace.NO_LITERAL_PREFIX, Replace.extractLiteralPrefix(Pattern.compile("^abc", Pattern.CANON_EQ)));
    }

    public void testLiteralPrefixHexEscapesAreLiteral() {
        // Java regex hex / unicode / control escapes always produce a literal character — they cannot encode a
        // meta `|`. So `^a\x7cb` matches the literal string `a|b`, not `^a|b`, and we can safely emit `a` as the
        // prefix (the walker stops at the unrecognized escape).
        assertPrefix("^a\\x7cb", "a");
    }

    public void testEndToEndHexEscapeIsLiteral() {
        // Sanity: `^a\x7cb` should match the literal "a|b" through the constant evaluator (with the prefix
        // fast path active). Strings that don't start with `a` are safely rejected.
        assertThat(processConstantRegex("a|b cat", "^a\\x7cb", "X"), equalTo("X cat"));
        assertThat(processConstantRegex("baz", "^a\\x7cb", "X"), equalTo("baz"));
    }

    public void testLiteralPrefixStopsAtMeta() {
        // For the q28 motivating example.
        assertPrefix("^https?://(?:www\\.)?([^/]+)/.*$", "http");
        // Exact-match pattern stops at `$`.
        assertPrefix("^exact_match$", "exact_match");
    }

    /**
     * End-to-end check that the prefix-rejection fast path does not change observable behavior:
     * inputs that match must still be replaced, and inputs that don't (because they fail the prefix
     * check) must come back unchanged.
     */
    public void testEndToEndAnchoredReplaceRespectsPrefixFastPath() {
        // Regex with extractable prefix "http" — common motivating shape.
        String regex = "^https?://(?:www\\.)?([^/]+)/.*$";
        String newStr = "$1";

        // Matches.
        assertThat(processConstantRegex("http://example.com/a", regex, newStr), equalTo("example.com"));
        assertThat(processConstantRegex("https://www.example.com/x/y", regex, newStr), equalTo("example.com"));

        // Does NOT start with the extracted prefix — must be returned unchanged.
        assertThat(processConstantRegex("ftp://example.com/a", regex, newStr), equalTo("ftp://example.com/a"));
        assertThat(processConstantRegex("", regex, newStr), equalTo(""));
        assertThat(processConstantRegex("htt", regex, newStr), equalTo("htt"));

        // Starts with prefix but doesn't fully match — must be returned unchanged (regex falls through).
        assertThat(processConstantRegex("httpbin", regex, newStr), equalTo("httpbin"));
    }

    // --- CaptureUntilDelimiterIdiom: detection (white-box) ---

    public void testCaptureUntilDelimiterIdiomDetectsMotivatingPattern() {
        // ClickBench's `REPLACE(Referer, "^https?://(?:www\.)?([^/]+)/.*$", "$1")` -- extract URL host.
        var idiom = Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^https?://(?:www\\.)?([^/]+)/.*$"), new BytesRef("$1"));
        assertNotNull(idiom);
        assertThat(idiom.delimiter(), equalTo((byte) '/'));
    }

    public void testCaptureUntilDelimiterIdiomAcceptsBareTrailingWildcard() {
        // No `$` anchor: `.*` always succeeds trivially, so no per-row DOTALL/newline safety net is needed.
        assertNotNull(Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^([^/]+)/.*"), new BytesRef("$1")));
    }

    public void testCaptureUntilDelimiterIdiomAcceptsLiteralAroundGroup() {
        assertNotNull(Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^([^,]+),.*$"), new BytesRef("name=$1!")));
    }

    public void testCaptureUntilDelimiterIdiomBailsWithoutAnchor() {
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("([^/]+)/.*$"), new BytesRef("$1")));
    }

    public void testCaptureUntilDelimiterIdiomBailsOnMultipleGroups() {
        // `$1` would be ambiguous / not necessarily "the" extracted segment once other groups exist.
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^(a)([^/]+)/.*$"), new BytesRef("$2")));
    }

    public void testCaptureUntilDelimiterIdiomBailsOnDisqualifyingFlags() {
        assertNull(
            Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^([^/]+)/.*$", Pattern.CASE_INSENSITIVE), new BytesRef("$1"))
        );
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^([^/]+)/.*$", Pattern.MULTILINE), new BytesRef("$1")));
    }

    public void testCaptureUntilDelimiterIdiomBailsOnNonAsciiDelimiter() {
        // The byte scan only supports a single-UTF-8-byte delimiter.
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^([^\u00e9]+)\u00e9.*$"), new BytesRef("$1")));
    }

    public void testCaptureUntilDelimiterIdiomBailsOnDelimiterMismatch() {
        // `[^/]+` followed by a DIFFERENT literal (",") -- the unique-split-point guarantee doesn't hold.
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^([^/]+),.*$"), new BytesRef("$1")));
    }

    public void testCaptureUntilDelimiterIdiomBailsOnAmbiguousReplacement() {
        Pattern p = Pattern.compile("^([^/]+)/.*$");
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(p, new BytesRef("$10"))); // could be group 10
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(p, new BytesRef("$1$1"))); // more than one ref
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(p, new BytesRef("\\$1"))); // backslash escape
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(p, new BytesRef("no group ref")));
    }

    public void testCaptureUntilDelimiterIdiomBailsOnUnsupportedPrefixQuantifiers() {
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^a*([^/]+)/.*$"), new BytesRef("$1")));
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^a+([^/]+)/.*$"), new BytesRef("$1")));
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^a{1,2}([^/]+)/.*$"), new BytesRef("$1")));
    }

    public void testCaptureUntilDelimiterIdiomBailsOnTrailingContent() {
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^([^/]+)/.*x$"), new BytesRef("$1")));
    }

    public void testCaptureUntilDelimiterIdiomBailsOnTooManyOptionalSegments() {
        // 5 independent optional segments exceeds MAX_OPTIONAL_PREFIX_PARTS (4).
        assertNull(Replace.extractCaptureUntilDelimiterIdiom(Pattern.compile("^a?b?c?d?e?([^/]+)/.*$"), new BytesRef("$1")));
    }

    // --- CaptureUntilDelimiterIdiom: end-to-end (evaluator selection + byte-scan correctness) ---

    public void testEndToEndCaptureUntilDelimiterIdiomExtractsHost() {
        String regex = "^https?://(?:www\\.)?([^/]+)/.*$";
        String newStr = "$1";
        assertEvaluatorToStringContains(regex, newStr, "ReplaceCaptureUntilDelimiterEvaluator");

        assertThat(processConstantRegexAndNewStr("http://example.com/a", regex, newStr), equalTo("example.com"));
        assertThat(processConstantRegexAndNewStr("https://www.example.com/x/y", regex, newStr), equalTo("example.com"));
        assertThat(processConstantRegexAndNewStr("https://example.com/", regex, newStr), equalTo("example.com"));

        // No match cases -- REPLACE returns the input unchanged, exactly like the regex path.
        assertThat(processConstantRegexAndNewStr("ftp://example.com/a", regex, newStr), equalTo("ftp://example.com/a"));
        assertThat(processConstantRegexAndNewStr("http://example.com", regex, newStr), equalTo("http://example.com"));
        assertThat(processConstantRegexAndNewStr("http:///x", regex, newStr), equalTo("http:///x"));
        assertThat(processConstantRegexAndNewStr("", regex, newStr), equalTo(""));
    }

    public void testEndToEndCaptureUntilDelimiterIdiomBacktracksOptionalPrefix() {
        // Regression case: greedily consuming the optional "www." would leave an empty capture
        // ("www./x" -> host starts right at the delimiter). Real regex backtracks to NOT taking the
        // optional group, capturing "www." itself (up to the next '/'); the byte scan must match.
        String regex = "^(?:www\\.)?([^/]+)/.*$";
        String newStr = "$1";
        assertThat(processConstantRegexAndNewStr("www./x", regex, newStr), equalTo("www."));
        assertThat(processConstantRegexAndNewStr("www.example.com/x", regex, newStr), equalTo("example.com"));
    }

    public void testEndToEndCaptureUntilDelimiterIdiomWithLiteralAroundGroup() {
        String regex = "^([^,]+),.*$";
        assertThat(processConstantRegexAndNewStr("alice,30,nyc", regex, "name=$1!"), equalTo("name=alice!"));
        assertThat(processConstantRegexAndNewStr("no-comma-here", regex, "name=$1!"), equalTo("no-comma-here"));
    }

    public void testEndToEndCaptureUntilDelimiterIdiomMatchesUnicodeCapturedContent() {
        // Only the literal prefix/delimiter must be ASCII -- the captured segment itself can be
        // arbitrary UTF-8 (including multi-byte and surrogate-pair code points).
        String regex = "^([^/]+)/.*$";
        assertThat(processConstantRegexAndNewStr("caf\u00e9/x", regex, "$1"), equalTo("caf\u00e9"));
        assertThat(processConstantRegexAndNewStr("a\ud83d\udc05b/x", regex, "$1"), equalTo("a\ud83d\udc05b"));
    }

    public void testEndToEndCaptureUntilDelimiterIdiomFallsBackOnEmbeddedNewline() {
        // `.*$` (no DOTALL) can't cross an embedded, non-trailing newline -- the byte scan must defer to
        // the real regex engine for that specific row rather than risk an incorrect result. Compare
        // directly against Java's own regex behavior for the same input.
        String regex = "^([^/]+)/.*$";
        assertEvaluatorToStringContains(regex, "$1", "ReplaceCaptureUntilDelimiterEvaluator");
        String withEmbeddedNewline = "host/pa\nth-tail";
        assertThat(processConstantRegexAndNewStr(withEmbeddedNewline, regex, "$1"), equalTo(withEmbeddedNewline.replaceAll(regex, "$1")));
    }

    public void testCaptureUntilDelimiterEvaluatorNotUsedWhenIdiomDoesNotMatch() {
        // Multiple capturing groups -- must fall back to the existing dictionary/ordinal evaluator.
        assertEvaluatorToStringContains("^(a)([^/]+)/.*$", "$2", "ReplaceConstantOrdinalEvaluator");
    }

    private void assertEvaluatorToStringContains(String regex, String newStr, String expectedSubstring) {
        try (var eval = constantRegexAndNewStrEvaluator(regex, newStr).get(driverContext())) {
            assertThat(eval.toString(), containsString(expectedSubstring));
        }
    }

    private String processConstantRegexAndNewStr(String text, String regex, String newStr) {
        try (
            var eval = constantRegexAndNewStrEvaluator(regex, newStr).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(text))))
        ) {
            return block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
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

    private String processConstantRegex(String text, String regex, String newStr) {
        try (
            var eval = AbstractScalarFunctionTestCase.evaluator(
                new Replace(
                    Source.EMPTY,
                    field("text", DataType.KEYWORD),
                    new Literal(Source.EMPTY, new BytesRef(regex), DataType.KEYWORD),
                    field("newStr", DataType.KEYWORD)
                )
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(text), new BytesRef(newStr))));
        ) {
            return block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
    }

    private static void assertPrefix(String regex, String expected) {
        byte[] actual = Replace.extractLiteralPrefix(Pattern.compile(regex));
        byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);
        assertArrayEquals("for regex " + regex, expectedBytes, actual);
    }

    private static void assertNoPrefix(String regex) {
        assertArrayEquals(
            "expected NO_LITERAL_PREFIX for regex " + regex,
            Replace.NO_LITERAL_PREFIX,
            Replace.extractLiteralPrefix(Pattern.compile(regex))
        );
    }

    public String process(String text, String regex, String newStr) {
        try (
            var eval = AbstractScalarFunctionTestCase.evaluator(
                new Replace(
                    Source.EMPTY,
                    field("text", DataType.KEYWORD),
                    field("regex", DataType.KEYWORD),
                    field("newStr", DataType.KEYWORD)
                )
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(text), new BytesRef(regex), new BytesRef(newStr))));
        ) {
            return block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
    }

    private String processOnSmallStack(String text, String regex, String newStr) {
        try (
            var eval = AbstractScalarFunctionTestCase.evaluator(
                new Replace(
                    Source.EMPTY,
                    field("text", DataType.KEYWORD),
                    field("regex", DataType.KEYWORD),
                    field("newStr", DataType.KEYWORD)
                )
            ).get(driverContext())
        ) {
            return evalOnSmallStack(eval, row(List.of(new BytesRef(text), new BytesRef(regex), new BytesRef(newStr))));
        }
    }

    private String processConstantRegexOnSmallStack(String text, String regex, String newStr) {
        try (
            var eval = AbstractScalarFunctionTestCase.evaluator(
                new Replace(
                    Source.EMPTY,
                    field("text", DataType.KEYWORD),
                    new Literal(Source.EMPTY, new BytesRef(regex), DataType.KEYWORD),
                    field("newStr", DataType.KEYWORD)
                )
            ).get(driverContext())
        ) {
            return evalOnSmallStack(eval, row(List.of(new BytesRef(text), new BytesRef(newStr))));
        }
    }

    private static String evalOnSmallStack(ExpressionEvaluator eval, Page page) {
        AtomicReference<String> result = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread thread = new Thread(Thread.currentThread().getThreadGroup(), () -> {
            try (Block block = eval.eval(page)) {
                result.set(block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString());
            } catch (Throwable t) {
                error.set(t);
            }
        }, "replace-catastrophic-regex", CATASTROPHIC_REGEX_STACK_SIZE);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for small-stack replace", e);
        }
        if (error.get() != null) {
            throw new AssertionError(error.get());
        }
        return result.get();
    }

    /**
     * The following fields and methods were borrowed from AbstractScalarFunctionTestCase
     */
    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());
    private final List<DriverContext> driverContexts = Collections.synchronizedList(new ArrayList<>());

    /**
     * Asserts the warnings accumulated across every {@link DriverContext} this test built.
     * If you need the old style thread-local warnings from {@link HeaderWarning}, use
     * {@link #takeResponseWarnings}.
     */
    private void assertDriverWarnings(String... expected) {
        Set<String> collected = new LinkedHashSet<>();
        for (DriverContext ctx : driverContexts) {
            if (ctx.isFinished() == false) {
                ctx.finish();
            }
            collected.addAll(ctx.warnings());
        }
        assertThat(collected, containsInAnyOrder(expected));
    }

    private static Page row(List<Object> values) {
        return new Page(1, BlockUtils.fromListRow(TestBlockFactory.getNonBreakingInstance(), values));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        DriverContext driverContext = new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
        driverContexts.add(driverContext);
        return driverContext;
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}
