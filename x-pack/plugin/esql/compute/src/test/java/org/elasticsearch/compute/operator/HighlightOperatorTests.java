/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.BytesRefBlockSourceOperator;
import org.elasticsearch.lucene.search.uhighlight.Snippet;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;

public class HighlightOperatorTests extends OperatorTestCase {

    private static final String DEFAULT_PRE_TAG = "<em>";
    private static final String DEFAULT_POST_TAG = "</em>";
    private static final String DEFAULT_ENCODER = "default";

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<BytesRef> input = IntStream.range(0, size).mapToObj(i -> new BytesRef("the fox number " + i)).toList();
        return new BytesRefBlockSourceOperator(blockFactory, input);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new HighlightOperator.Factory(config("fox", 5, 0, 0), List.of(dc -> identityEvaluator()));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo(
            "HighlightOperator[query=fox, pre_tag=<em>, post_tag=</em>, encoder=default, number_of_fragments=5, fragment_size=0, "
                + "no_match_size=0, word_boundary=false, locale=, order_by_score=false, max_analyzed_offset=-1, fields=1]"
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "HighlightOperator[query=content:fox, query=fox, pre_tag=<em>, post_tag=</em>, encoder=default, number_of_fragments=5, "
                + "fragment_size=0, no_match_size=0, word_boundary=false, locale=, order_by_score=false, "
                + "max_analyzed_offset=-1, fields=[identity]]"
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        BytesRef scratch = new BytesRef();
        int row = 0;
        for (Page page : results) {
            BytesRefBlock highlighted = page.getBlock(page.getBlockCount() - 1);
            for (int i = 0; i < page.getPositionCount(); i++) {
                String value = highlighted.getBytesRef(highlighted.getFirstValueIndex(i), scratch).utf8ToString();
                assertThat(value, equalTo("the <em>fox</em> number " + row));
                row++;
            }
        }
    }

    public void testNoMatchYieldsNull() {
        BytesRefBlock result = highlightSingle(config("nonexistent", 5, 0, 0), "a plain sentence");
        try {
            assertThat(result.isNull(0), equalTo(true));
        } finally {
            result.close();
        }
    }

    public void testNoMatchSizeReturnsLeadingText() {
        BytesRefBlock result = highlightSingle(config("nonexistent", 5, 0, 200), "Gardens and flowers bloom in spring.");
        try {
            assertThat(value(result, 0), equalTo("Gardens and flowers bloom in spring."));
        } finally {
            result.close();
        }
    }

    public void testEmptyQueryHasNoTermsAndDoesNotMatch() {
        // An empty query analyzes to no terms; the operator turns that into a MatchNoDocsQuery, so nothing is highlighted.
        BytesRefBlock result = highlightSingle(config("", 5, 0, 0), "any text here");
        try {
            assertThat(result.isNull(0), equalTo(true));
        } finally {
            result.close();
        }
    }

    public void testMultiValuedFieldHighlightsEachValueInOrder() {
        BytesRefBlock input = bytesRefs(List.of(List.of("Senior Team Lead", "Lead Architect")));
        BytesRefBlock result = highlight(config("lead", 5, 0, 0), input);
        try {
            assertThat(result.getValueCount(0), equalTo(2));
            int first = result.getFirstValueIndex(0);
            BytesRef scratch = new BytesRef();
            assertThat(result.getBytesRef(first, scratch).utf8ToString(), equalTo("Senior Team <em>Lead</em>"));
            assertThat(result.getBytesRef(first + 1, scratch).utf8ToString(), equalTo("<em>Lead</em> Architect"));
        } finally {
            result.close();
        }
    }

    public void testNumberOfFragmentsCapsInDocumentOrder() {
        String text = "Elasticsearch is fast. Elasticsearch is scalable. Elasticsearch is open.";
        BytesRefBlock result = highlightSingle(config("elasticsearch", 2, 0, 0), text);
        try {
            assertThat(result.getValueCount(0), equalTo(2));
            int first = result.getFirstValueIndex(0);
            BytesRef scratch = new BytesRef();
            assertThat(result.getBytesRef(first, scratch).utf8ToString(), equalTo("<em>Elasticsearch</em> is fast."));
            assertThat(result.getBytesRef(first + 1, scratch).utf8ToString(), equalTo("<em>Elasticsearch</em> is scalable."));
        } finally {
            result.close();
        }
    }

    public void testFragmentSizeBoundsLongSentence() {
        String text = "Elasticsearch powers fast search across very many documents and shards in a single cluster.";
        BytesRefBlock result = highlightSingle(config("elasticsearch", 5, 20, 0), text);
        try {
            // With a 20-char bound the matched fragment is shorter than the full sentence.
            assertThat(value(result, 0).length(), lessThan(text.length() + "<em></em>".length()));
            assertThat(value(result, 0).contains("<em>Elasticsearch</em>"), equalTo(true));
        } finally {
            result.close();
        }
    }

    public void testHtmlEncoderEscapesMarkup() {
        String text = "Use <b>bold</b> tags & special chars with the Ring.";
        HighlightConfig config = new HighlightConfig(
            "ring",
            DEFAULT_PRE_TAG,
            DEFAULT_POST_TAG,
            HighlightConfig.HTML_ENCODER,
            5,
            0,
            0,
            false,
            Locale.ROOT,
            false,
            -1
        );
        BytesRefBlock result = highlightSingle(config, text);
        try {
            assertThat(value(result, 0), equalTo("Use &lt;b&gt;bold&lt;&#x2F;b&gt; tags &amp; special chars with the <em>Ring</em>."));
        } finally {
            result.close();
        }
    }

    public void testWordBoundaryFragments() {
        String text = "Elasticsearch powers fast search across very many documents and shards in a single cluster.";
        BytesRefBlock result = highlight(config("elasticsearch", 5, 20, 0, true, false), bytesRefs(List.of(List.of(text))));
        try {
            // The word scanner ignores fragment_size and breaks on word boundaries, so the fragment is short.
            assertThat(value(result, 0).contains("<em>Elasticsearch</em>"), equalTo(true));
            assertThat(value(result, 0).length(), lessThan(text.length() + "<em></em>".length()));
        } finally {
            result.close();
        }
    }

    public void testOrderByScoreReturnsBestFragmentFirst() {
        // The second sentence has two matches, so it scores higher and must come first when ordering by score.
        String text = "Search is fast. Fast search powers fast results. Indexing is simple.";
        BytesRefBlock result = highlight(config("fast", 5, 0, 0, false, true), bytesRefs(List.of(List.of(text))));
        try {
            int first = result.getFirstValueIndex(0);
            BytesRef scratch = new BytesRef();
            assertThat(result.getBytesRef(first, scratch).utf8ToString(), startsWith("<em>Fast</em> search powers <em>fast</em> results."));
        } finally {
            result.close();
        }
    }

    public void testOrderByScoreWithSingleFragmentReturnsOnlyBest() {
        String text = "Search is fast. Indexing is fast. Fast search powers fast results. Queries are fast.";
        BytesRefBlock result = highlight(config("fast", 1, 0, 0, false, true), bytesRefs(List.of(List.of(text))));
        try {
            assertThat(result.getValueCount(0), equalTo(1));
            assertThat(value(result, 0), equalTo("<em>Fast</em> search powers <em>fast</em> results."));
        } finally {
            result.close();
        }
    }

    // The no-match fallback passage carries a NaN score, which must sort last rather than first under order=score.
    public void testScoreDescendingTreatsNaNAsLowest() {
        Snippet best = new Snippet("best", 5.0f, true);
        Snippet worst = new Snippet("worst", 1.0f, true);
        Snippet noMatch = new Snippet("no-match-fallback", Float.NaN, false);
        Snippet[] snippets = { noMatch, worst, best };
        Arrays.sort(snippets, HighlightOperator.SCORE_DESCENDING);
        assertThat(Arrays.stream(snippets).map(Snippet::getText).toList(), contains("best", "worst", "no-match-fallback"));
    }

    // Equal scores keep document order because Arrays.sort is stable and the comparator returns 0 on ties.
    public void testScoreDescendingKeepsDocumentOrderOnTies() {
        Snippet first = new Snippet("first", 2.0f, true);
        Snippet second = new Snippet("second", 2.0f, true);
        Snippet third = new Snippet("third", 2.0f, true);
        Snippet[] snippets = { first, second, third };
        Arrays.sort(snippets, HighlightOperator.SCORE_DESCENDING);
        assertThat(Arrays.stream(snippets).map(Snippet::getText).toList(), contains("first", "second", "third"));
    }

    public void testNonBytesRefFieldThrows() {
        try (
            HighlightOperator operator = new HighlightOperator(
                blockFactory(),
                config("fox", 5, 0, 0),
                new ExpressionEvaluator[] { identityEvaluator() }
            )
        ) {
            IntBlock intBlock = blockFactory().newConstantIntBlockWith(1, 1);
            try {
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> operator.process(new Page(intBlock)));
                assertThat(e.getMessage(), startsWith("HIGHLIGHT ON fields must evaluate to keyword/text values"));
            } finally {
                intBlock.close();
            }
        }
    }

    private BytesRefBlock highlightSingle(HighlightConfig config, String text) {
        return highlight(config, bytesRefs(List.of(List.of(text))));
    }

    private BytesRefBlock highlight(HighlightConfig config, BytesRefBlock input) {
        try (
            HighlightOperator operator = new HighlightOperator(blockFactory(), config, new ExpressionEvaluator[] { identityEvaluator() })
        ) {
            Page result = operator.process(new Page(input));
            BytesRefBlock highlighted = result.getBlock(result.getBlockCount() - 1);
            highlighted.incRef();
            result.releaseBlocks();
            return highlighted;
        }
    }

    private static String value(BytesRefBlock block, int position) {
        return block.getBytesRef(block.getFirstValueIndex(position), new BytesRef()).utf8ToString();
    }

    private BytesRefBlock bytesRefs(List<List<String>> rows) {
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(rows.size())) {
            for (List<String> row : rows) {
                if (row.size() == 1) {
                    builder.appendBytesRef(new BytesRef(row.get(0)));
                } else {
                    builder.beginPositionEntry();
                    for (String value : row) {
                        builder.appendBytesRef(new BytesRef(value));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private static HighlightConfig config(String queryText, int fragments, int fragmentSize, int noMatchSize) {
        return new HighlightConfig(
            queryText,
            DEFAULT_PRE_TAG,
            DEFAULT_POST_TAG,
            DEFAULT_ENCODER,
            fragments,
            fragmentSize,
            noMatchSize,
            false,
            Locale.ROOT,
            false,
            -1
        );
    }

    private static HighlightConfig config(
        String queryText,
        int fragments,
        int fragmentSize,
        int noMatchSize,
        boolean wordBoundary,
        boolean orderByScore
    ) {
        return new HighlightConfig(
            queryText,
            DEFAULT_PRE_TAG,
            DEFAULT_POST_TAG,
            DEFAULT_ENCODER,
            fragments,
            fragmentSize,
            noMatchSize,
            wordBoundary,
            Locale.ROOT,
            orderByScore,
            -1
        );
    }

    // Returns the input block unchanged, so the operator highlights channel 0 directly.
    private static ExpressionEvaluator identityEvaluator() {
        return new ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                Block block = page.getBlock(0);
                block.incRef();
                return block;
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}

            @Override
            public String toString() {
                return "identity";
            }
        };
    }
}
