/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;

/**
 * These tests create rows that are 1MB in size. Test classes
 * which extend AbstractScalarFunctionTestCase rerun test cases with
 * many randomized inputs. Unfortunately, tests are run with
 * limited memory, and instantiating many copies of these
 * tests with large rows causes out of memory.
 */
public class ReplaceStaticTests extends ESTestCase {

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

    public void testTooBig() {
        String textAndNewStr = randomAlphaOfLength((int) (ScalarFunction.MAX_BYTES_REF_RESULT_SIZE / 10));
        String regex = ".";

        String result = process(textAndNewStr, regex, textAndNewStr);
        assertNull(result);
        assertWarnings(
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
        assertWarnings(
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

    /**
     * The following fields and methods were borrowed from AbstractScalarFunctionTestCase
     */
    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    private static Page row(List<Object> values) {
        return new Page(1, BlockUtils.fromListRow(TestBlockFactory.getNonBreakingInstance(), values));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        return new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}
