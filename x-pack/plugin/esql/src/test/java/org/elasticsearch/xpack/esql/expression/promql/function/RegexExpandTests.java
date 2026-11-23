/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.junit.After;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link RegexExpand}, the value-derivation scalar behind PromQL {@code label_replace}. The pure
 * {@code expand}/match semantics (Prometheus/Go parity) are asserted directly; the three-outcome null/empty/value encoding
 * is asserted through a real evaluator so the builder-arg null handling is exercised end to end.
 */
public class RegexExpandTests extends ESTestCase {

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    // --- Go-style Expand + full anchoring, matching Prometheus funcLabelReplace ---

    public void testWholeMatchGroupZero() {
        assertThat(replace("a(b)c", "$0", "abc"), equalTo("abc"));
    }

    public void testBracedWholeMatchGroupZero() {
        // The braced form "${0}" resolves to the same whole-match group as "$0".
        assertThat(replace("a(b)c", "${0}", "abc"), equalTo("abc"));
    }

    public void testEmptyTemplateExpandsToEmpty() {
        // A template with no bytes has nothing to append: on a match it expands to the empty string.
        assertThat(replace(".*", "", "abc"), equalTo(""));
    }

    public void testNumberedGroup() {
        assertThat(replace("(.*)", "$1-x", "foo"), equalTo("foo-x"));
    }

    public void testBracedNumberedGroupSeparatesTrailingText() {
        // "$1x" reads the group name "1x" (unknown) -> empty; "${1}x" references group 1 then literal 'x'.
        assertThat(replace("(.)(.*)", "$1x", "ab"), equalTo(""));
        assertThat(replace("(.)(.*)", "${1}x", "ab"), equalTo("ax"));
    }

    public void testNamedGroup() {
        assertThat(replace("(?P<x>.*)", "${x}", "bar"), equalTo("bar"));
    }

    public void testEscapedDollarIsLiteral() {
        assertThat(replace(".*", "a$$b", "z"), equalTo("a$b"));
    }

    public void testUnknownAndOutOfRangeGroupExpandsToEmpty() {
        assertThat(replace("(.*)", "$2", "z"), equalTo(""));
        assertThat(replace("(.*)", "${nope}", "z"), equalTo(""));
    }

    public void testLeadingZeroGroupRefIsNamedNotIndexed() {
        // Go/Prometheus (Regexp.Expand -> extract) disallow leading zeros in a numeric capture reference: a multi-digit
        // name starting with '0' is not an index, so it is looked up as a named group, misses, and expands to "".
        assertThat(replace("(.)(.*)", "$01", "ab"), equalTo(""));
        assertThat(replace("(.)(.*)", "${01}", "ab"), equalTo(""));
        assertThat(replace("a(b)c", "$00", "abc"), equalTo(""));
        // Control: a single digit stays numeric ("$0" is the whole match, "$1" is group 1).
        assertThat(replace("a(b)c", "$0", "abc"), equalTo("abc"));
        assertThat(replace("(.)(.*)", "$1", "ab"), equalTo("a"));
    }

    public void testDotMatchesNewline() {
        // Prometheus anchors as ^(?s:regex)$, so '.' spans newlines.
        assertThat(replace(".*", "$0", "a\nb"), equalTo("a\nb"));
    }

    public void testFullAnchoringRequiresWholeStringMatch() {
        // A partial match must not count: the regex is anchored at both ends.
        assertThat(replace("\\d+", "$0", "abc123"), nullValue());
    }

    public void testNoMatchIsNoOp() {
        assertThat(replace("\\d+", "$0", "abc"), nullValue());
    }

    public void testEmptyRegexMatchesEmptyString() {
        assertThat(replace("", "hi", ""), equalTo("hi"));
    }

    public void testUnterminatedBraceReferenceIsLiteralDollar() {
        // Go's extract returns ok=false for a "${" with no closing brace, so the '$' is emitted as raw text and the rest
        // of the template ("{1") follows verbatim.
        assertThat(replace("(.*)", "a${1", "z"), equalTo("a${1"));
    }

    public void testTrailingDollarIsLiteral() {
        // A '$' at the very end of the template has no reference to expand and is emitted literally.
        assertThat(replace("(.*)", "a$", "z"), equalTo("a$"));
    }

    public void testNumericReferenceOverflowIsNamedLookup() {
        // Go caps numeric-index accumulation at 1e8: a longer digit run is treated as a (named) lookup, which misses and
        // expands to "". This also guards the reference resolution against integer overflow.
        assertThat(replace("(.*)", "$1000000000", "z"), equalTo(""));
    }

    public void testNamedGroupWithDigitsAndUnderscore() {
        // Group names may contain digits and underscores; both the braced and unbraced forms resolve to the capture.
        assertThat(replace("(?P<foo_1>.*)", "${foo_1}", "hi"), equalTo("hi"));
        assertThat(replace("(?P<foo_1>.*)", "$foo_1", "hi"), equalTo("hi"));
    }

    // --- UTF-8 / byte-offset correctness: matching is on raw bytes and groups are sliced by byte offset ---

    public void testGroupAfterMultiByteRegionUsesByteOffsets() {
        // 'é' is two UTF-8 bytes, so group 2 begins at byte 5, not char 4. A char-offset slice would split the 'é'.
        assertThat(replace("(caf.)(.*)", "$2", "café!"), equalTo("!"));
    }

    public void testGroupCapturesMultiByteChar() {
        assertThat(replace("(caf.)", "$1", "café"), equalTo("café"));
    }

    public void testGroupCapturesAstralChar() {
        // A 4-byte astral rune (grinning face) is a single RE2 '.'; group 2 is the trailing "x", group 1 the rune itself.
        assertThat(replace("(.)(.*)", "$2", "😀x"), equalTo("x"));
        assertThat(replace("(.)(.*)", "$1", "😀x"), equalTo("😀"));
    }

    public void testMultiByteLiteralInReplacement() {
        // Literal runs in the template are appended as their UTF-8 bytes verbatim.
        assertThat(replace(".*", "→${0}←", "hi"), equalTo("→hi←"));
    }

    public void testMultiByteOnBothSides() {
        assertThat(replace("(.*)", "«$1»", "naïve"), equalTo("«naïve»"));
    }

    public void testGroupBoundaryAfterMultiByteClass() {
        // "café" (5 bytes) precedes the digit group; a byte-offset slice yields "42", a char-offset one would not.
        assertThat(replace("([^0-9]+)([0-9]+)", "$2", "café42"), equalTo("42"));
    }

    // --- Capture-group structure: unmatched-optional vs zero-width, and interleaving ---

    public void testUnmatchedOptionalGroupContributesNothing() {
        // Group 2 is optional and does not participate; its reference (start < 0) expands to nothing (never to src).
        assertThat(replace("(a)(b)?", "$1|$2", "a"), equalTo("a|"));
    }

    public void testZeroWidthGroupIsEmpty() {
        // Group 1 matches an empty string (a zero-width but present capture): its braced reference contributes "", while the
        // surrounding literal 'x' and group 2 still expand. (Braced so the reference is "1", not the name run "1x".)
        assertThat(replace("(a*)(b)", "${1}x$2", "b"), equalTo("xb"));
    }

    public void testMultipleGroupsReordered() {
        assertThat(replace("(.)(.)(.)", "$3$2$1", "abc"), equalTo("cba"));
    }

    public void testAdjacentBracedGroups() {
        assertThat(replace("(.)(.)", "${1}-${2}", "ab"), equalTo("a-b"));
    }

    public void testAdjacentNumberedGroups() {
        assertThat(replace("(.)(.)", "$1$2", "ab"), equalTo("ab"));
    }

    // --- More Go-Expand grammar edge cases ---

    public void testDollarBeforeNonReferenceIsLiteral() {
        // '$' followed by a non-name, non-'{', non-'$' character is emitted literally, then the rest follows verbatim.
        assertThat(replace(".*", "a$.b", "z"), equalTo("a$.b"));
    }

    public void testEmptyBracesAreLiteral() {
        // "${}" has an empty name, so extract fails: the '$' is literal and "{}b" follows verbatim.
        assertThat(replace(".*", "a${}b", "z"), equalTo("a${}b"));
    }

    public void testEscapedDollarBeforeReference() {
        // "$$" collapses to one literal '$', then "$1" expands group 1.
        assertThat(replace("(.*)", "$$$1", "x"), equalTo("$x"));
    }

    // --- RE2 matching-feature parity: the regex is compiled and matched by RE2/J, groups sliced from the matched bytes ---

    public void testCaseInsensitiveFlag() {
        // Inline (?i) applies RE2 case-insensitive matching; the captured group is the actual (original-case) input bytes.
        assertThat(replace("(?i)foo(.*)", "$1", "FOObar"), equalTo("bar"));
    }

    public void testAlternation() {
        assertThat(replace("(cat|dog)s?", "$1", "dogs"), equalTo("dog"));
    }

    public void testNonGreedyQuantifier() {
        // Non-greedy (.*?) takes as little as possible under full anchoring: group 1 is "aa", not the greedy "aab".
        assertThat(replace("(.*?)(b+)", "$1|$2", "aabb"), equalTo("aa|bb"));
    }

    public void testCharacterClasses() {
        assertThat(replace("([a-z]+)([0-9]+)", "$2$1", "abc123"), equalTo("123abc"));
    }

    public void testUnicodeCharacterClass() {
        // RE2 Unicode property class \p{L} matches multi-byte letters; the group is sliced back out by byte offset intact.
        assertThat(replace("(\\p{L}+)", "$1", "café"), equalTo("café"));
    }

    public void testGroupSpanningNewlineUnderDotall() {
        // Prometheus anchors as ^(?s:regex)$, so a captured '.' group spans the newline.
        assertThat(replace("(.*)", "[$1]", "a\nb"), equalTo("[a\nb]"));
    }

    // --- Analysis-time regex validation: compiles exactly as the evaluator does, so invalid patterns are rejected early ---

    public void testValidateRegexAcceptsValidPattern() {
        assertThat(RegexExpand.validateRegex("source-value-(.*)"), nullValue());
    }

    public void testValidateRegexRejectsInvalidPattern() {
        assertThat(RegexExpand.validateRegex("("), notNullValue());
    }

    // --- Evaluator: null (no-op) / empty (delete) / value (set) and absent-source-reads-as-empty ---

    public void testEvaluatorSet() {
        assertThat(eval("(.*)", "$1!", "foo"), equalTo("foo!"));
    }

    public void testEvaluatorDeleteOnMatchedEmpty() {
        assertThat(eval("(.*)", "$2", "foo"), equalTo(""));
    }

    public void testEvaluatorDeleteOnEmptyTemplate() {
        // An empty template on a match yields the empty string (the delete outcome), not a no-op null.
        assertThat(eval(".*", "", "foo"), equalTo(""));
    }

    public void testEvaluatorNoOpOnNoMatch() {
        assertThat(eval("\\d+", "x", "foo"), nullValue());
    }

    /**
     * An absent source label is coalesced to {@code ""} upstream (during translation), so at the evaluator it matches an
     * empty regex. A genuinely {@code null} position is short-circuited by the evaluator and never reaches
     * {@link RegexExpand#process}, which is why the coalesce is the caller's responsibility.
     */
    public void testEvaluatorEmptySourceMatchesEmptyRegex() {
        assertThat(eval("", "hi", ""), equalTo("hi"));
    }

    public void testEvaluatorNullSourceIsShortCircuitedToNull() {
        assertThat(eval("", "hi", null), nullValue());
    }

    public void testEvaluatorMultiByteValue() {
        assertThat(eval("(.*)", "[$1]", "naïve"), equalTo("[naïve]"));
    }

    public void testEvaluatorMultiValueSourceWarnsAndIsNull() {
        Source source = Source.EMPTY;
        RegexExpand function = new RegexExpand(
            source,
            field("src", DataType.KEYWORD),
            new Literal(source, new BytesRef("(.*)"), DataType.KEYWORD),
            new Literal(source, new BytesRef("$1"), DataType.KEYWORD)
        );
        DriverContext context = driverContext();
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(1)) {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("a"));
            builder.appendBytesRef(new BytesRef("b"));
            builder.endPositionEntry();
            try (
                var evaluator = AbstractScalarFunctionTestCase.evaluator(function).get(context);
                Block block = evaluator.eval(new Page(builder.build()))
            ) {
                assertTrue(block.isNull(0));
            }
        }
        context.finish();
        assertThat(context.warnings(), hasItem(containsString("single-value function encountered multi-value")));
    }

    // --- Evaluator across multiple rows: the Matcher and output buffers are reused per driver, so verify no cross-row bleed ---

    public void testEvaluatorReusesOutputBufferAcrossRows() {
        // A long output followed by a short one must not leak the tail of the previous row (length vs. buffer capacity).
        assertThat(evalMany("(.*)", "$1", "aaaa", "b"), equalTo(List.of("aaaa", "b")));
    }

    public void testEvaluatorInterleavesOutcomesAcrossRows() {
        // set / delete (matched-empty) / no-op (null) / set, all through the shared Matcher and output buffers.
        assertThat(evalMany("(a*)", "$1", "aaa", "", "b", "a"), equalTo(Arrays.asList("aaa", "", null, "a")));
    }

    public void testEvaluatorConstantTemplateReusedAcrossRows() {
        // A reference-free template is re-emitted every row; the shared buffer must yield the same value each time.
        assertThat(evalMany(".*", "static", "a", "bb", "ccc"), equalTo(List.of("static", "static", "static")));
    }

    public void testEvaluatorBufferGrowsThenReusedForSmallRow() {
        // Force the output buffer to grow well past its default, then reuse it for a tiny row.
        String big = "x".repeat(5000);
        assertThat(evalMany("(.*)", "$1", big, "y"), equalTo(List.of(big, "y")));
    }

    /**
     * Mirrors {@link RegexExpand}'s match/expand contract: {@code null} on no match (no-op), otherwise the expansion
     * (which may be the empty string, the delete sentinel).
     */
    private static String replace(String regex, String replacement, String src) {
        Pattern pattern = Pattern.compile("^(?s:" + regex + ")$");
        // Match the raw UTF-8 bytes, mirroring RegexExpand#process: capture-group offsets are then byte offsets, which
        // is what expand() slices against.
        byte[] input = src.getBytes(StandardCharsets.UTF_8);
        Matcher matcher = pattern.matcher(input);
        if (matcher.matches() == false) {
            return null;
        }
        return RegexExpand.Replacement.of(replacement, pattern)
            .expand(matcher, input, new BytesRefBuilder(), new BytesRef())
            .utf8ToString();
    }

    private String eval(String regex, String replacement, String src) {
        Source source = Source.EMPTY;
        RegexExpand function = new RegexExpand(
            source,
            field("src", DataType.KEYWORD),
            new Literal(source, new BytesRef(regex), DataType.KEYWORD),
            new Literal(source, new BytesRef(replacement), DataType.KEYWORD)
        );
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(1)) {
            if (src == null) {
                builder.appendNull();
            } else {
                builder.appendBytesRef(new BytesRef(src));
            }
            try (
                var evaluator = AbstractScalarFunctionTestCase.evaluator(function).get(driverContext());
                Block block = evaluator.eval(new Page(builder.build()))
            ) {
                if (block.isNull(0)) {
                    return null;
                }
                return ((BytesRefBlock) block).getBytesRef(0, new BytesRef()).utf8ToString();
            }
        }
    }

    /**
     * Evaluates {@code srcs} as a single multi-position block, returning one result per position ({@code null} for a no-op or a
     * {@code null} source). Exercises the per-driver reuse of the {@code Matcher} and the output buffers across rows.
     */
    private List<String> evalMany(String regex, String replacement, String... srcs) {
        Source source = Source.EMPTY;
        RegexExpand function = new RegexExpand(
            source,
            field("src", DataType.KEYWORD),
            new Literal(source, new BytesRef(regex), DataType.KEYWORD),
            new Literal(source, new BytesRef(replacement), DataType.KEYWORD)
        );
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(srcs.length)) {
            for (String src : srcs) {
                if (src == null) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(new BytesRef(src));
                }
            }
            try (
                var evaluator = AbstractScalarFunctionTestCase.evaluator(function).get(driverContext());
                Block block = evaluator.eval(new Page(builder.build()))
            ) {
                BytesRefBlock bytesRefBlock = (BytesRefBlock) block;
                BytesRef scratch = new BytesRef();
                List<String> results = new ArrayList<>(srcs.length);
                for (int p = 0; p < srcs.length; p++) {
                    results.add(
                        block.isNull(p) ? null : bytesRefBlock.getBytesRef(bytesRefBlock.getFirstValueIndex(p), scratch).utf8ToString()
                    );
                }
                return results;
            }
        }
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
    public void allBreakersEmpty() {
        for (CircuitBreaker breaker : breakers) {
            assertThat("Breaker not empty: " + breaker.getName(), breaker.getUsed(), equalTo(0L));
        }
    }
}
