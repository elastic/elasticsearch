/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.LoadFromPageEvaluator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.BytesRefBlockSourceOperator;
import org.elasticsearch.lucene.search.uhighlight.Snippet;
import org.hamcrest.Matcher;

import java.io.IOException;
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

    private static final String CONTENT_FIELD = "content";
    private static final List<String> CONTENT = List.of(CONTENT_FIELD);
    private static final List<String> TITLE_BODY = List.of("title", "body");

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<BytesRef> input = IntStream.range(0, size).mapToObj(i -> new BytesRef("the fox number " + i)).toList();
        return new BytesRefBlockSourceOperator(blockFactory, input);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        Analyzer analyzer = new StandardAnalyzer();
        HighlightConfig config = config("fox", 5, 0, 0).withExecutionContext(analyzer, contentTerm("fox"), CONTENT);
        return new HighlightOperator.Factory(config, List.of(new LoadFromPageEvaluator.Factory(0)));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo(
            "HighlightOperator[query=fox, pre_tag=<em>, post_tag=</em>, encoder=default, number_of_fragments=5, fragment_size=0, "
                + "no_match_size=0, word_boundary=false, locale=, order_by_score=false, analyzer=null, max_analyzed_offset=-1, fields=1]"
        );
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "HighlightOperator[query=content:fox, query=fox, pre_tag=<em>, post_tag=</em>, encoder=default, number_of_fragments=5, "
                + "fragment_size=0, no_match_size=0, word_boundary=false, locale=, order_by_score=false, analyzer=null, "
                + "max_analyzed_offset=-1, fields=[Attribute[channel=0]]]"
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
        BytesRefBlock result = highlightSingle(config("", 5, 0, 0), new MatchNoDocsQuery("HIGHLIGHT query is empty"), "any text here");
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

    public void testNumberOfFragmentsSelectsBestScoringInDocumentOrder() {
        String text = "One fox. Two fox fox. Three fox fox fox.";
        BytesRefBlock result = highlightSingle(config("fox", 2, 0, 0), text);
        try {
            assertThat(result.getValueCount(0), equalTo(2));
            int first = result.getFirstValueIndex(0);
            BytesRef scratch = new BytesRef();
            assertThat(result.getBytesRef(first, scratch).utf8ToString(), equalTo("Two <em>fox</em> <em>fox</em>."));
            assertThat(result.getBytesRef(first + 1, scratch).utf8ToString(), equalTo("Three <em>fox</em> <em>fox</em> <em>fox</em>."));
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
            null,
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
        Analyzer analyzer = new StandardAnalyzer();
        try (
            HighlightOperator operator = new HighlightOperator(
                blockFactory(),
                config("fox", 5, 0, 0).withExecutionContext(analyzer, contentTerm("fox"), CONTENT),
                new ExpressionEvaluator[] { new LoadFromPageEvaluator(0) }
            )
        ) {
            IntBlock intBlock = blockFactory().newConstantIntBlockWith(1, 1);
            try {
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> operator.process(new Page(intBlock)));
                assertThat(e.getMessage(), startsWith("HIGHLIGHT ON fields must be [text] or [keyword]"));
            } finally {
                intBlock.close();
            }
        }
    }

    public void testPhraseHighlightsAsSingleSpan() {
        BytesRefBlock result = highlightSingle(
            config("\"quick brown fox\"", 5, 0, 0),
            new PhraseQuery(CONTENT_FIELD, "quick", "brown", "fox"),
            "The quick brown fox jumps over the lazy dog."
        );
        try {
            assertThat(value(result, 0), equalTo("The <em>quick brown fox</em> jumps over the lazy dog."));
        } finally {
            result.close();
        }
    }

    public void testPerFieldTargetingHighlightsOnlyTheTargetedColumn() {
        Query query = termQuery("title", "fox");
        BytesRefBlock title = bytesRefs(List.of(List.of("the quick fox")));
        BytesRefBlock body = bytesRefs(List.of(List.of("a fox in the henhouse")));
        Page result = highlightFields(config("title:fox", 5, 0, 0), query, TITLE_BODY, title, body);
        try {
            BytesRefBlock highlightTitle = result.getBlock(2);
            BytesRefBlock highlightBody = result.getBlock(3);
            assertThat(value(highlightTitle, 0), equalTo("the quick <em>fox</em>"));
            assertThat(highlightBody.isNull(0), equalTo(true));
        } finally {
            result.releaseBlocks();
        }
    }

    public void testCrossFieldConjunctionHighlightsWholeRowOrNothing() {
        Query query = new BooleanQuery.Builder().add(termQuery("title", "fox"), BooleanClause.Occur.MUST)
            .add(termQuery("body", "dog"), BooleanClause.Occur.MUST)
            .build();
        BytesRefBlock title = bytesRefs(List.of(List.of("the fox"), List.of("the fox")));
        BytesRefBlock body = bytesRefs(List.of(List.of("a dog"), List.of("a cat")));
        Page result = highlightFields(config("+title:fox +body:dog", 5, 0, 0), query, TITLE_BODY, title, body);
        try {
            BytesRefBlock highlightTitle = result.getBlock(2);
            BytesRefBlock highlightBody = result.getBlock(3);
            assertThat(value(highlightTitle, 0), equalTo("the <em>fox</em>"));
            assertThat(value(highlightBody, 0), equalTo("a <em>dog</em>"));
            assertThat(highlightTitle.isNull(1), equalTo(true));
            assertThat(highlightBody.isNull(1), equalTo(true));
        } finally {
            result.releaseBlocks();
        }
    }

    public void testRowWithAllNullFieldsYieldsNullEverywhere() {
        Query query = new BooleanQuery.Builder().add(termQuery("title", "fox"), BooleanClause.Occur.SHOULD)
            .add(termQuery("body", "fox"), BooleanClause.Occur.SHOULD)
            .build();
        BytesRefBlock title = (BytesRefBlock) blockFactory().newConstantNullBlock(1);
        BytesRefBlock body = (BytesRefBlock) blockFactory().newConstantNullBlock(1);
        Page result = highlightFields(config("fox", 5, 0, 0), query, TITLE_BODY, title, body);
        try {
            assertThat(result.<BytesRefBlock>getBlock(2).isNull(0), equalTo(true));
            assertThat(result.<BytesRefBlock>getBlock(3).isNull(0), equalTo(true));
        } finally {
            result.releaseBlocks();
        }
    }

    // The memory index, its reader, and the per-field highlighters are built once in the constructor and reused
    // across rows and pages (see HighlightOperator's field comment). Each test below targets a specific stale-state
    // bug that reuse could introduce if reset() or highlighter caching were wrong.

    public void testReusedIndexDoesNotLeakTermsBetweenRows() {
        // Row 2's vocabulary is completely disjoint from "fox"; if the reused index kept row 1's terms around after
        // reset(), row 2 could falsely match.
        BytesRefBlock result = highlight(
            config("fox", 5, 0, 0),
            bytesRefs(List.of(List.of("the quick fox"), List.of("lorem ipsum dolor sit amet")))
        );
        try {
            assertThat(value(result, 0), equalTo("the quick <em>fox</em>"));
            assertThat(result.isNull(1), equalTo(true));
        } finally {
            result.close();
        }
    }

    public void testReusedIndexDoesNotLeakTermsBetweenRowsMissFirst() {
        // Same as above with the miss row first, to catch leakage in either direction across reset().
        BytesRefBlock result = highlight(
            config("fox", 5, 0, 0),
            bytesRefs(List.of(List.of("lorem ipsum dolor sit amet"), List.of("the quick fox")))
        );
        try {
            assertThat(result.isNull(0), equalTo(true));
            assertThat(value(result, 1), equalTo("the quick <em>fox</em>"));
        } finally {
            result.close();
        }
    }

    public void testReusedIndexAcrossPages() {
        // addInput/getOutput (here, process()) is called repeatedly on the same operator instance in production;
        // a second page must not see any state left over from the first page's rows.
        HighlightConfig config = config("fox", 5, 0, 0);
        try (
            HighlightOperator operator = new HighlightOperator(
                blockFactory(),
                config.withExecutionContext(new StandardAnalyzer(), contentTerm("fox"), CONTENT),
                new ExpressionEvaluator[] { new LoadFromPageEvaluator(0) }
            )
        ) {
            BytesRefBlock page1Input = bytesRefs(List.of(List.of("the quick fox"), List.of("the quick fox again")));
            Page page1Result = operator.process(new Page(page1Input));
            try {
                BytesRefBlock highlighted = page1Result.getBlock(page1Result.getBlockCount() - 1);
                assertThat(value(highlighted, 0), equalTo("the quick <em>fox</em>"));
                assertThat(value(highlighted, 1), equalTo("the quick <em>fox</em> again"));
            } finally {
                page1Result.releaseBlocks();
            }

            BytesRefBlock page2Input = bytesRefs(List.of(List.of("lorem ipsum dolor"), List.of("sit amet consectetur")));
            Page page2Result = operator.process(new Page(page2Input));
            try {
                BytesRefBlock highlighted = page2Result.getBlock(page2Result.getBlockCount() - 1);
                assertThat(highlighted.isNull(0), equalTo(true));
                assertThat(highlighted.isNull(1), equalTo(true));
            } finally {
                page2Result.releaseBlocks();
            }
        }
    }

    public void testShrinkingTextAcrossRows() {
        // Row 1 is long (several hundred tokens); row 2 is short. If the reused index/reader retained stale offsets
        // sized for row 1, highlighting row 2 would either produce garbage text or throw.
        StringBuilder longText = new StringBuilder();
        for (int i = 0; i < 300; i++) {
            longText.append("filler word ");
        }
        longText.append("fox jumps at the end.");
        BytesRefBlock result = highlight(config("fox", 5, 0, 0), bytesRefs(List.of(List.of(longText.toString()), List.of("a fox."))));
        try {
            assertThat(value(result, 0).contains("<em>fox</em>"), equalTo(true));
            assertThat(value(result, 1), equalTo("a <em>fox</em>."));
        } finally {
            result.close();
        }
    }

    public void testMultiFieldAlternatingNulls() {
        // Two ON fields; each row sets a different one of the two. Reusing one highlighter per field must not
        // confuse which field's postings a given row's snippet comes from.
        Query query = new BooleanQuery.Builder().add(termQuery("title", "fox"), BooleanClause.Occur.SHOULD)
            .add(termQuery("body", "fox"), BooleanClause.Occur.SHOULD)
            .build();
        BytesRefBlock title = bytesRefsOrNull(Arrays.asList("the fox", null));
        BytesRefBlock body = bytesRefsOrNull(Arrays.asList(null, "a fox in the barn"));
        Page result = highlightFields(config("fox", 5, 0, 0), query, TITLE_BODY, title, body);
        try {
            BytesRefBlock highlightTitle = result.getBlock(2);
            BytesRefBlock highlightBody = result.getBlock(3);
            assertThat(value(highlightTitle, 0), equalTo("the <em>fox</em>"));
            assertThat(highlightBody.isNull(0), equalTo(true));
            assertThat(highlightTitle.isNull(1), equalTo(true));
            assertThat(value(highlightBody, 1), equalTo("a <em>fox</em> in the barn"));
        } finally {
            result.releaseBlocks();
        }
    }

    // Only tokens the query can match are indexed (see HighlightOperator#buildKeepWordMatcher and #fillRowIndex).
    // Each test below targets a specific correctness risk that dropping non-query tokens could introduce.

    public void testFilteredIndexPreservesPositionsForPhrases() {
        // Row 1: "fox" and "jumps" are adjacent (positions 0,1) -> the slop-0 phrase matches.
        // Row 2: "quickly" sits between them; after filtering it's dropped but its position increment is kept, so
        // the filtered index has fox=0, jumps=2 -> the phrase must NOT match. A broken position-increment carry
        // (fox=0, jumps=1) would make this row falsely match.
        Query phraseQuery = new PhraseQuery(CONTENT_FIELD, "fox", "jumps");
        BytesRefBlock result = highlight(
            config("\"fox jumps\"", 5, 0, 0),
            phraseQuery,
            bytesRefs(List.of(List.of("fox jumps high"), List.of("fox quickly jumps")))
        );
        try {
            assertThat(value(result, 0), equalTo("<em>fox jumps</em> high"));
            assertThat(result.isNull(1), equalTo(true));
        } finally {
            result.close();
        }
    }

    public void testNoMatchSizeStillReturnsLeadingTextWithFiltering() {
        // A miss row (no "fox") sits between two hit rows; no_match_size > 0 means the miss row must still run the
        // highlighter and return its leading text rather than being short-circuited to null.
        BytesRefBlock result = highlight(
            config("fox", 5, 0, 200),
            bytesRefs(
                List.of(List.of("The quick fox jumps."), List.of("Gardens and flowers bloom in spring."), List.of("A fox in the barn."))
            )
        );
        try {
            assertThat(value(result, 0), equalTo("The quick <em>fox</em> jumps."));
            assertThat(value(result, 1), equalTo("Gardens and flowers bloom in spring."));
            assertThat(value(result, 2), equalTo("A <em>fox</em> in the barn."));
        } finally {
            result.close();
        }
    }

    public void testShortCircuitMissRowsYieldNull() {
        // no_match_size == 0, so a row where filtering keeps nothing hits the fillRowIndex short-circuit directly.
        BytesRefBlock result = highlight(
            config("fox", 5, 0, 0),
            bytesRefs(
                List.of(List.of("The quick fox jumps."), List.of("Gardens and flowers bloom in spring."), List.of("A fox in the barn."))
            )
        );
        try {
            assertThat(value(result, 0), equalTo("The quick <em>fox</em> jumps."));
            assertThat(result.isNull(1), equalTo(true));
            assertThat(value(result, 2), equalTo("A <em>fox</em> in the barn."));
        } finally {
            result.close();
        }
    }

    public void testMultiTermQueryDisablesFiltering() {
        // PrefixQuery triggers consumeTermsMatching, which cannot be enumerated into a keep-word matcher, so
        // filtering must be silently disabled (every token indexed) and highlighting must still produce the same
        // output it would without filtering, including a plain miss row yielding null.
        Query prefixQuery = new PrefixQuery(new Term(CONTENT_FIELD, "fo"));
        BytesRefBlock result = highlight(
            config("fo*", 5, 0, 0),
            prefixQuery,
            bytesRefs(List.of(List.of("the quick fox"), List.of("a plain sentence")))
        );
        try {
            assertThat(value(result, 0), equalTo("the quick <em>fox</em>"));
            assertThat(result.isNull(1), equalTo(true));
        } finally {
            result.close();
        }
    }

    public void testMustNotTermsAreNotHighlighted() {
        // MUST_NOT terms are kept in the filtered index (see buildKeepWordMatcher's Javadoc): unlike Lucene's
        // MemoryIndexOffsetStrategy, this operator's memory index is the only thing that decides whether a row
        // matches, so dropping "dog" would erase the evidence needed to correctly exclude row 1. Row 2 has no
        // prohibited term, matches, and highlights only "fox". "dog" never gets wrapped in tags even when kept,
        // because the highlighter never produces spans for MUST_NOT clauses.
        Query query = new BooleanQuery.Builder().add(termQuery(CONTENT_FIELD, "fox"), BooleanClause.Occur.MUST)
            .add(termQuery(CONTENT_FIELD, "dog"), BooleanClause.Occur.MUST_NOT)
            .build();
        BytesRefBlock result = highlight(
            config("+fox -dog", 5, 0, 0),
            query,
            bytesRefs(List.of(List.of("the fox and the dog"), List.of("the fox in the barn")))
        );
        try {
            assertThat(result.isNull(0), equalTo(true));
            assertThat(value(result, 1), equalTo("the <em>fox</em> in the barn"));
        } finally {
            result.close();
        }
    }

    public void testCustomAnalyzerMatchesThroughSynonyms() throws IOException {
        // The keep-word matcher compares post-analysis tokens against post-analysis query terms, so filtering must
        // compose with whatever a custom analyzer emits, including tokens that never appear in the original text.
        // Here the analyzer injects "automobile" as a synonym of "car": the query term can only match through the
        // injected token, so if filtering compared raw text (or dropped injected tokens) row 1 would wrongly yield
        // null. The snippet must wrap the original "car", whose offsets the injected token carries. Row 2 keeps no
        // tokens and exercises the short-circuit under a custom analyzer.
        SynonymMap.Builder synonyms = new SynonymMap.Builder(true);
        synonyms.add(new CharsRef("car"), new CharsRef("automobile"), true);
        SynonymMap synonymMap = synonyms.build();
        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new StandardTokenizer();
                TokenStream stream = new LowerCaseFilter(tokenizer);
                stream = new SynonymGraphFilter(stream, synonymMap, true);
                return new TokenStreamComponents(tokenizer, stream);
            }
        };
        BytesRefBlock input = bytesRefs(List.of(List.of("the red car drives"), List.of("a plain sentence")));
        try (
            HighlightOperator operator = new HighlightOperator(
                blockFactory(),
                config("automobile", 5, 0, 0).withExecutionContext(analyzer, contentTerm("automobile"), CONTENT),
                new ExpressionEvaluator[] { new LoadFromPageEvaluator(0) }
            )
        ) {
            Page result = operator.process(new Page(input));
            BytesRefBlock highlighted = result.getBlock(result.getBlockCount() - 1);
            try {
                assertThat(value(highlighted, 0), equalTo("the red <em>car</em> drives"));
                assertThat(highlighted.isNull(1), equalTo(true));
            } finally {
                result.releaseBlocks();
            }
        }
    }

    private static Query contentTerm(String term) {
        return termQuery(CONTENT_FIELD, term);
    }

    private static Query termQuery(String field, String term) {
        return new TermQuery(new Term(field, term));
    }

    private BytesRefBlock highlightSingle(HighlightConfig config, String text) {
        return highlightSingle(config, contentTerm(config.queryText()), text);
    }

    private BytesRefBlock highlightSingle(HighlightConfig config, Query query, String text) {
        return highlight(config, query, bytesRefs(List.of(List.of(text))));
    }

    private BytesRefBlock highlight(HighlightConfig config, BytesRefBlock input) {
        return highlight(config, contentTerm(config.queryText()), input);
    }

    private BytesRefBlock highlight(HighlightConfig config, Query query, BytesRefBlock input) {
        try (
            HighlightOperator operator = new HighlightOperator(
                blockFactory(),
                config.withExecutionContext(new StandardAnalyzer(), query, CONTENT),
                new ExpressionEvaluator[] { new LoadFromPageEvaluator(0) }
            )
        ) {
            Page result = operator.process(new Page(input));
            BytesRefBlock highlighted = result.getBlock(result.getBlockCount() - 1);
            highlighted.incRef();
            result.releaseBlocks();
            return highlighted;
        }
    }

    // Runs the operator with one input block per ON field.
    private Page highlightFields(HighlightConfig config, Query query, List<String> fieldNames, BytesRefBlock... fields) {
        ExpressionEvaluator[] evaluators = IntStream.range(0, fields.length)
            .mapToObj(LoadFromPageEvaluator::new)
            .toArray(ExpressionEvaluator[]::new);
        try (
            HighlightOperator operator = new HighlightOperator(
                blockFactory(),
                config.withExecutionContext(new StandardAnalyzer(), query, fieldNames),
                evaluators
            )
        ) {
            return operator.process(new Page(fields));
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

    // Like bytesRefs, but a null element appends a null row instead of a value; used to build per-field blocks
    // where different rows populate different ON fields.
    private BytesRefBlock bytesRefsOrNull(List<String> values) {
        try (BytesRefBlock.Builder builder = blockFactory().newBytesRefBlockBuilder(values.size())) {
            for (String value : values) {
                if (value == null) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(new BytesRef(value));
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
            null,
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
            null,
            -1
        );
    }

}
