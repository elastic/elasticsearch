/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IllformedLocaleException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class HighlightOptionsTests extends ESTestCase {

    public void testNullOptionsUsesDefaults() {
        HighlightOptions options = HighlightOptions.from(null, FoldContext.small());
        assertThat(options.preTag(), equalTo(HighlightOptions.DEFAULT_PRE_TAG));
        assertThat(options.postTag(), equalTo(HighlightOptions.DEFAULT_POST_TAG));
        assertThat(options.encoder(), equalTo(HighlightOptions.DEFAULT_ENCODER));
        assertThat(options.numberOfFragments(), equalTo(HighlightOptions.DEFAULT_NUMBER_OF_FRAGMENTS));
        assertThat(options.fragmentSize(), equalTo(HighlightOptions.DEFAULT_FRAGMENT_SIZE));
        assertThat(options.noMatchSize(), equalTo(HighlightOptions.DEFAULT_NO_MATCH_SIZE));
        assertThat(options.boundaryScanner(), equalTo(HighlightOptions.DEFAULT_BOUNDARY_SCANNER));
        assertThat(options.boundaryScannerLocale(), equalTo(HighlightOptions.DEFAULT_BOUNDARY_SCANNER_LOCALE));
        assertThat(options.order(), equalTo(HighlightOptions.DEFAULT_ORDER));
        assertThat(options.maxAnalyzedOffset(), equalTo(HighlightOptions.DEFAULT_MAX_ANALYZED_OFFSET));
    }

    public void testBoundaryAndOrderOptionsAreParsed() {
        MapExpression map = map(
            Highlight.BOUNDARY_SCANNER,
            Literal.keyword(Source.EMPTY, HighlightOptions.BOUNDARY_SCANNER_WORD),
            Highlight.BOUNDARY_SCANNER_LOCALE,
            Literal.keyword(Source.EMPTY, "en-US"),
            Highlight.ORDER,
            Literal.keyword(Source.EMPTY, HighlightOptions.ORDER_SCORE)
        );
        HighlightOptions options = HighlightOptions.from(map, FoldContext.small());
        assertThat(options.boundaryScanner(), equalTo(HighlightOptions.BOUNDARY_SCANNER_WORD));
        assertThat(options.boundaryScannerLocale(), equalTo(Locale.forLanguageTag("en-US")));
        assertThat(options.order(), equalTo(HighlightOptions.ORDER_SCORE));
    }

    public void testBoundaryScannerLocaleRejectsMalformedTag() {
        // The malformed tag is normalized into a stable HIGHLIGHT IllegalArgumentException rather than leaking the
        // JDK-controlled IllformedLocaleException (whose message wording is not stable across runtimes).
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightOptions.from(map(Highlight.BOUNDARY_SCANNER_LOCALE, Literal.keyword(Source.EMPTY, "en_US")), FoldContext.small())
        );
        assertThat(e.getMessage(), containsString("Option [boundary_scanner_locale] has invalid language tag"));
        assertThat(e.getMessage(), containsString("[en_US] is not a valid language tag"));
        assertThat(e.getCause(), instanceOf(IllformedLocaleException.class));
    }

    public void testBoundaryAndOrderOptionsAreNormalizedToLowerCase() {
        MapExpression map = map(
            Highlight.BOUNDARY_SCANNER,
            Literal.keyword(Source.EMPTY, "WORD"),
            Highlight.ORDER,
            Literal.keyword(Source.EMPTY, "Score")
        );
        HighlightOptions options = HighlightOptions.from(map, FoldContext.small());
        assertThat(options.boundaryScanner(), equalTo(HighlightOptions.BOUNDARY_SCANNER_WORD));
        assertThat(options.order(), equalTo(HighlightOptions.ORDER_SCORE));
    }

    public void testMaxAnalyzedOffsetIsParsed() {
        MapExpression map = map(
            Highlight.MAX_ANALYZED_OFFSET,
            Literal.integer(Source.EMPTY, 500),
            Highlight.PHRASE_LIMIT,
            Literal.integer(Source.EMPTY, 64)
        );
        HighlightOptions options = HighlightOptions.from(map, FoldContext.small());
        assertThat(options.maxAnalyzedOffset(), equalTo(500));
    }

    public void testMaxAnalyzedOffsetAllowsMinusOne() {
        HighlightOptions options = HighlightOptions.from(
            map(Highlight.MAX_ANALYZED_OFFSET, Literal.integer(Source.EMPTY, -1)),
            FoldContext.small()
        );
        assertThat(options.maxAnalyzedOffset(), equalTo(-1));
    }

    public void testMaxAnalyzedOffsetRejectsZero() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightOptions.from(map(Highlight.MAX_ANALYZED_OFFSET, Literal.integer(Source.EMPTY, 0)), FoldContext.small())
        );
        assertThat(e.getMessage(), containsString("Option [max_analyzed_offset] must be a positive integer, or -1"));
    }

    public void testMaxAnalyzedOffsetRejectsBelowMinusOne() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightOptions.from(map(Highlight.MAX_ANALYZED_OFFSET, Literal.integer(Source.EMPTY, -2)), FoldContext.small())
        );
        assertThat(e.getMessage(), containsString("Option [max_analyzed_offset] must be a positive integer, or -1"));
    }

    public void testPhraseLimitIsAcceptedButIgnored() {
        HighlightOptions options = HighlightOptions.from(
            map(Highlight.PHRASE_LIMIT, Literal.integer(Source.EMPTY, -1), Highlight.NUMBER_OF_FRAGMENTS, Literal.integer(Source.EMPTY, 3)),
            FoldContext.small()
        );
        assertThat(options.numberOfFragments(), equalTo(3));
    }

    public void testTagAsScalarString() {
        HighlightOptions options = HighlightOptions.from(
            map(Highlight.PRE_TAGS, Literal.keyword(Source.EMPTY, "<b>")),
            FoldContext.small()
        );
        assertThat(options.preTag(), equalTo("<b>"));
    }

    public void testTagAsSingleElementList() {
        HighlightOptions options = HighlightOptions.from(map(Highlight.PRE_TAGS, keywordList("<b>")), FoldContext.small());
        assertThat(options.preTag(), equalTo("<b>"));
    }

    public void testMultipleStringValuesAreRejected() {
        IllegalArgumentException tags = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightOptions.from(map(Highlight.PRE_TAGS, keywordList("<b>", "<i>")), FoldContext.small())
        );
        assertThat(tags.getMessage(), containsString("Option [pre_tags] expects a single string value, found [2] values"));

        IllegalArgumentException encoder = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightOptions.from(map(Highlight.ENCODER, keywordList("default", "html")), FoldContext.small())
        );
        assertThat(encoder.getMessage(), containsString("Option [encoder] expects a single string value, found [2] values"));
    }

    public void testHtmlEncoder() {
        HighlightOptions options = HighlightOptions.from(
            map(Highlight.ENCODER, Literal.keyword(Source.EMPTY, HighlightOptions.HTML_ENCODER)),
            FoldContext.small()
        );
        assertThat(options.encoder(), equalTo(HighlightOptions.HTML_ENCODER));
    }

    public void testIntegerOptionsAreParsed() {
        MapExpression map = map(
            Highlight.NUMBER_OF_FRAGMENTS,
            Literal.integer(Source.EMPTY, 3),
            Highlight.FRAGMENT_SIZE,
            Literal.integer(Source.EMPTY, 120),
            Highlight.NO_MATCH_SIZE,
            Literal.integer(Source.EMPTY, 50)
        );
        HighlightOptions options = HighlightOptions.from(map, FoldContext.small());
        assertThat(options.numberOfFragments(), equalTo(3));
        assertThat(options.fragmentSize(), equalTo(120));
        assertThat(options.noMatchSize(), equalTo(50));
    }

    public void testNegativeIntegerIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightOptions.from(map(Highlight.NUMBER_OF_FRAGMENTS, Literal.integer(Source.EMPTY, -1)), FoldContext.small())
        );
        assertThat(e.getMessage(), containsString("Option [number_of_fragments] must be >= 0"));
    }

    public void testNonNumericIntegerIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightOptions.from(map(Highlight.FRAGMENT_SIZE, Literal.keyword(Source.EMPTY, "big")), FoldContext.small())
        );
        assertThat(e.getMessage(), containsString("Option [fragment_size] must be numeric"));
    }

    public void testDecimalIntegerOptionsAreRejected() {
        for (String name : List.of(Highlight.NUMBER_OF_FRAGMENTS, Highlight.FRAGMENT_SIZE, Highlight.NO_MATCH_SIZE)) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> HighlightOptions.from(map(name, Literal.fromDouble(Source.EMPTY, 0.9)), FoldContext.small())
            );
            assertThat(e.getMessage(), containsString("Option [" + name + "] must be an integer"));
        }
    }

    public void testDecimalMaxAnalyzedOffsetIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightOptions.from(map(Highlight.MAX_ANALYZED_OFFSET, Literal.fromDouble(Source.EMPTY, 10.9)), FoldContext.small())
        );
        assertThat(e.getMessage(), containsString("Option [max_analyzed_offset] must be an integer"));
    }

    public void testWholeDoubleIsAcceptedForIntegerOptions() {
        HighlightOptions options = HighlightOptions.from(
            map(Highlight.NUMBER_OF_FRAGMENTS, Literal.fromDouble(Source.EMPTY, 3.0)),
            FoldContext.small()
        );
        assertThat(options.numberOfFragments(), equalTo(3));
    }

    public void testNonStringTagIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightOptions.from(map(Highlight.PRE_TAGS, Literal.integer(Source.EMPTY, 123)), FoldContext.small())
        );
        assertThat(e.getMessage(), containsString("Option [pre_tags] must be a string"));
    }

    private static MapExpression map(Object... keyValues) {
        List<Expression> entries = new ArrayList<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            entries.add(Literal.keyword(Source.EMPTY, (String) keyValues[i]));
            entries.add((Expression) keyValues[i + 1]);
        }
        return new MapExpression(Source.EMPTY, entries);
    }

    private static Literal keywordList(String... values) {
        return new Literal(Source.EMPTY, Arrays.stream(values).map(BytesRefs::toBytesRef).toList(), KEYWORD);
    }

}
