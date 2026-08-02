/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.promql;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;

import org.apache.lucene.util.BytesRef;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link PromqlRegexExtract}, the value-derivation scalar behind PromQL {@code label_replace}. The pure
 * {@code expand}/match semantics (Prometheus/Go parity) are asserted directly; the three-outcome null/empty/value encoding
 * is asserted through a real evaluator so the builder-arg null handling is exercised end to end.
 */
public class PromqlRegexExtractTests extends ESTestCase {

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    // --- Go-style Expand + full anchoring, matching Prometheus funcLabelReplace ---

    public void testWholeMatchGroupZero() {
        assertThat(replace("a(b)c", "$0", "abc"), equalTo("abc"));
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

    // --- Evaluator: null (no-op) / empty (delete) / value (set) and absent-source-reads-as-empty ---

    public void testEvaluatorSet() {
        assertThat(eval("(.*)", "$1!", "foo"), equalTo("foo!"));
    }

    public void testEvaluatorDeleteOnMatchedEmpty() {
        assertThat(eval("(.*)", "$2", "foo"), equalTo(""));
    }

    public void testEvaluatorNoOpOnNoMatch() {
        assertThat(eval("\\d+", "x", "foo"), nullValue());
    }

    /**
     * An absent source label is coalesced to {@code ""} upstream (during translation), so at the evaluator it matches an
     * empty regex. A genuinely {@code null} position is short-circuited by the generated evaluator and never reaches
     * {@link PromqlRegexExtract#process}, which is why the coalesce is the caller's responsibility.
     */
    public void testEvaluatorEmptySourceMatchesEmptyRegex() {
        assertThat(eval("", "hi", ""), equalTo("hi"));
    }

    public void testEvaluatorNullSourceIsShortCircuitedToNull() {
        assertThat(eval("", "hi", null), nullValue());
    }

    /**
     * Mirrors {@link PromqlRegexExtract}'s match/expand contract: {@code null} on no match (no-op), otherwise the expansion
     * (which may be the empty string, the delete sentinel).
     */
    private static String replace(String regex, String replacement, String src) {
        Pattern pattern = Pattern.compile("^(?s:" + regex + ")$");
        Matcher matcher = pattern.matcher(src);
        if (matcher.matches() == false) {
            return null;
        }
        return PromqlRegexExtract.expand(replacement, matcher, pattern.namedGroups());
    }

    private String eval(String regex, String replacement, String src) {
        Source source = Source.EMPTY;
        PromqlRegexExtract function = new PromqlRegexExtract(
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
