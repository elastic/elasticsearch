/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.highlight;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.BytesRefBlockSourceOperator;
import org.elasticsearch.lucene.search.uhighlight.CustomPassageFormatter;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.plan.logical.HighlightOptions;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;

public class HighlightOperatorTests extends OperatorTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<BytesRef> input = IntStream.range(0, size).mapToObj(i -> new BytesRef("the fox number " + i)).toList();
        return new BytesRefBlockSourceOperator(blockFactory, input);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return factory("fox", 5, 0, 0);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("HighlightOperator[query=fox, fields=1, number_of_fragments=5, fragment_size=0, no_match_size=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("HighlightOperator[query=content:fox, number_of_fragments=5, fragment_size=0, no_match_size=0, fields=[identity]]");
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
        BytesRefBlock result = highlightSingle(
            query("nonexistent"),
            formatter("<em>", "</em>", new DefaultEncoder()),
            5,
            0,
            0,
            "a plain sentence"
        );
        try {
            assertThat(result.isNull(0), equalTo(true));
        } finally {
            result.close();
        }
    }

    public void testNoMatchSizeReturnsLeadingText() {
        BytesRefBlock result = highlightSingle(
            query("nonexistent"),
            formatter("<em>", "</em>", new DefaultEncoder()),
            5,
            0,
            200,
            "Gardens and flowers bloom in spring."
        );
        try {
            assertThat(value(result, 0), equalTo("Gardens and flowers bloom in spring."));
        } finally {
            result.close();
        }
    }

    public void testEmptyQueryHasNoTermsAndDoesNotMatch() {
        // An empty query analyzes to no terms; the planner turns that into a MatchNoDocsQuery, so nothing is highlighted.
        BytesRefBlock result = highlightSingle(
            Queries.NO_DOCS_INSTANCE,
            formatter("<em>", "</em>", new DefaultEncoder()),
            5,
            0,
            0,
            "any text here"
        );
        try {
            assertThat(result.isNull(0), equalTo(true));
        } finally {
            result.close();
        }
    }

    public void testMultiValuedFieldHighlightsEachValueInOrder() {
        BytesRefBlock input = bytesRefs(List.of(List.of("Senior Team Lead", "Lead Architect")));
        BytesRefBlock result = highlight(query("lead"), formatter("<em>", "</em>", new DefaultEncoder()), 5, 0, 0, input);
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
        BytesRefBlock result = highlightSingle(query("elasticsearch"), formatter("<em>", "</em>", new DefaultEncoder()), 2, 0, 0, text);
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
        BytesRefBlock result = highlightSingle(query("elasticsearch"), formatter("<em>", "</em>", new DefaultEncoder()), 5, 20, 0, text);
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
        BytesRefBlock result = highlightSingle(query("ring"), formatter("<em>", "</em>", new SimpleHTMLEncoder()), 5, 0, 0, text);
        try {
            assertThat(value(result, 0), equalTo("Use &lt;b&gt;bold&lt;&#x2F;b&gt; tags &amp; special chars with the <em>Ring</em>."));
        } finally {
            result.close();
        }
    }

    public void testNonBytesRefFieldThrows() {
        try (
            HighlightOperator operator = new HighlightOperator(
                blockFactory(),
                query("fox"),
                new StandardAnalyzer(),
                formatter("<em>", "</em>", new DefaultEncoder()),
                5,
                0,
                0,
                new ExpressionEvaluator[] { identityEvaluator() }
            )
        ) {
            IntBlock intBlock = blockFactory().newConstantIntBlockWith(1, 1);
            try {
                EsqlIllegalArgumentException e = expectThrows(
                    EsqlIllegalArgumentException.class,
                    () -> operator.process(new Page(intBlock))
                );
                assertThat(e.getMessage(), startsWith("HIGHLIGHT ON fields must evaluate to keyword/text values"));
            } finally {
                intBlock.close();
            }
        }
    }

    private BytesRefBlock highlightSingle(
        Query query,
        PassageFormatter formatter,
        int fragments,
        int fragmentSize,
        int noMatchSize,
        String text
    ) {
        return highlight(query, formatter, fragments, fragmentSize, noMatchSize, bytesRefs(List.of(List.of(text))));
    }

    private BytesRefBlock highlight(
        Query query,
        PassageFormatter formatter,
        int fragments,
        int fragmentSize,
        int noMatchSize,
        BytesRefBlock input
    ) {
        try (
            HighlightOperator operator = new HighlightOperator(
                blockFactory(),
                query,
                new StandardAnalyzer(),
                formatter,
                fragments,
                fragmentSize,
                noMatchSize,
                new ExpressionEvaluator[] { identityEvaluator() }
            )
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

    private static Query query(String text) {
        Query query = new QueryBuilder(new StandardAnalyzer()).createBooleanQuery(
            HighlightOperator.CONTENT_FIELD,
            text,
            BooleanClause.Occur.SHOULD
        );
        return query == null ? Queries.NO_DOCS_INSTANCE : query;
    }

    private static PassageFormatter formatter(String preTag, String postTag, Encoder encoder) {
        return new CustomPassageFormatter(preTag, postTag, encoder, 5);
    }

    private static HighlightOperator.Factory factory(String queryText, int fragments, int fragmentSize, int noMatchSize) {
        HighlightOptions options = new HighlightOptions(
            HighlightOptions.DEFAULT_PRE_TAG,
            HighlightOptions.DEFAULT_POST_TAG,
            HighlightOptions.DEFAULT_ENCODER,
            fragments,
            fragmentSize,
            noMatchSize
        );
        return new HighlightOperator.Factory(queryText, options, List.of(dc -> identityEvaluator()));
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
