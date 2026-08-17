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
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
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
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
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

    public void testMultiValuedKeywordAnalyzerAnalyzesValuesSeparately() {
        BytesRefBlock result = highlight(
            config("foo", 5, 0, 0),
            contentTerm("foo"),
            new KeywordAnalyzer(),
            bytesRefs(List.of(List.of("foo", "bar")))
        );
        try {
            assertThat(value(result, 0), equalTo("<em>foo</em>"));
        } finally {
            result.close();
        }
    }

    public void testMultiValuedAnalyzerDoesNotCreateCrossValueTokens() {
        BytesRefBlock result = highlight(
            config("foo bar", 5, 0, 0),
            contentTerm("foo bar"),
            shingleAnalyzer(),
            bytesRefs(List.of(List.of("foo", "bar")))
        );
        try {
            assertThat(result.isNull(0), equalTo(true));
        } finally {
            result.close();
        }
    }

    public void testMaxAnalyzedOffsetAppliesAcrossValues() {
        BytesRefBlock result = highlight(
            configWithMaxAnalyzedOffset("foo", 4),
            contentTerm("foo"),
            new KeywordAnalyzer(),
            bytesRefs(List.of(List.of("long", "foo")))
        );
        try {
            assertThat(result.isNull(0), equalTo(true));
        } finally {
            result.close();
        }
    }

    public void testMultiValuedEmptyValuesKeepOffsets() {
        BytesRefBlock result = highlight(
            config("foo", 5, 0, 0),
            contentTerm("foo"),
            new KeywordAnalyzer(),
            bytesRefs(List.of(List.of("", "foo", "")))
        );
        try {
            assertThat(value(result, 0), equalTo("<em>foo</em>"));
        } finally {
            result.close();
        }
    }

    // Without the analyzer's position gap, the phrase matches across the value boundary.
    public void testMultiValuePositionGapPreventsCrossValuePhraseMatch() {
        Query phrase = new PhraseQuery(CONTENT_FIELD, "lead", "lead");
        List<List<String>> rows = List.of(List.of("Senior Team Lead", "Lead Architect"));

        BytesRefBlock withoutGap = highlight(config("\"lead lead\"", 5, 0, 0), phrase, standardAnalyzerWithGap(0), bytesRefs(rows));
        try {
            assertThat(withoutGap.isNull(0), equalTo(false));
        } finally {
            withoutGap.close();
        }

        BytesRefBlock withGap = highlight(config("\"lead lead\"", 5, 0, 0), phrase, standardAnalyzerWithGap(100), bytesRefs(rows));
        try {
            assertThat(withGap.isNull(0), equalTo(true));
        } finally {
            withGap.close();
        }
    }

    // The gap must not affect a phrase contained in one value.
    public void testMultiValuePositionGapKeepsWithinValuePhraseMatch() {
        Query phrase = new PhraseQuery(CONTENT_FIELD, "team", "lead");
        BytesRefBlock result = highlight(
            config("\"team lead\"", 5, 0, 0),
            phrase,
            standardAnalyzerWithGap(100),
            bytesRefs(List.of(List.of("Senior Team Lead", "Lead Architect")))
        );
        try {
            assertThat(result.getValueCount(0), equalTo(1));
            assertThat(value(result, 0), equalTo("Senior <em>Team Lead</em>"));
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

    public void testReusedIndexDoesNotLeakTermsBetweenRows() {
        BytesRefBlock result = highlight(
            config("fox", 5, 0, 0),
            bytesRefs(List.of(List.of("the quick fox"), List.of("lorem ipsum dolor sit amet"), List.of("a fox in the barn")))
        );
        try {
            assertThat(value(result, 0), equalTo("the quick <em>fox</em>"));
            assertThat(result.isNull(1), equalTo(true));
            assertThat(value(result, 2), equalTo("a <em>fox</em> in the barn"));
        } finally {
            result.close();
        }
    }

    public void testNullValueHighlightsOnlyThePresentField() {
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

    public void testFilteredIndexPreservesPositionsForPhrases() {
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

    // FieldExistsQuery (QSTR "_exists_:content", KQL "content:*") matches through FieldInfos, so the
    // per-row reader must expose fields indexed for that row.
    public void testFieldExistsQueryHighlightsMatchingRow() {
        Query query = new BooleanQuery.Builder().add(contentTerm("fox"), BooleanClause.Occur.MUST)
            .add(new FieldExistsQuery(CONTENT_FIELD), BooleanClause.Occur.MUST)
            .build();
        BytesRefBlock result = highlightSingle(config("fox AND _exists_:content", 5, 0, 0), query, "the quick fox");
        try {
            assertThat(value(result, 0), equalTo("the quick <em>fox</em>"));
        } finally {
            result.close();
        }
    }

    public void testMultiTermQueryFiltersViaAutomaton() {
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

    public void testLeadingWildcardQueryFiltersViaAutomaton() {
        Query wildcardQuery = new WildcardQuery(new Term(CONTENT_FIELD, "*ox"));
        BytesRefBlock result = highlight(
            config("*ox", 5, 0, 0),
            wildcardQuery,
            bytesRefs(List.of(List.of("the quick fox jumps"), List.of("a plain sentence")))
        );
        try {
            assertThat(value(result, 0), equalTo("the quick <em>fox</em> jumps"));
            assertThat(result.isNull(1), equalTo(true));
        } finally {
            result.close();
        }
    }

    public void testRegexpQueryFiltersViaAutomaton() {
        Query regexpQuery = new RegexpQuery(new Term(CONTENT_FIELD, "f.x"));
        BytesRefBlock result = highlight(
            config("/f.x/", 5, 0, 0),
            regexpQuery,
            bytesRefs(List.of(List.of("the quick fox"), List.of("a plain sentence")))
        );
        try {
            assertThat(value(result, 0), equalTo("the quick <em>fox</em>"));
            assertThat(result.isNull(1), equalTo(true));
        } finally {
            result.close();
        }
    }

    public void testMustNotTermExcludesRow() {
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
        SynonymMap.Builder synonyms = new SynonymMap.Builder(true);
        synonyms.add(new CharsRef("car"), new CharsRef("automobile"), true);
        BytesRefBlock result = highlight(
            config("automobile", 5, 0, 0),
            contentTerm("automobile"),
            synonymAnalyzer(synonyms.build()),
            bytesRefs(List.of(List.of("the red car drives"), List.of("a plain sentence")))
        );
        try {
            assertThat(value(result, 0), equalTo("the red <em>car</em> drives"));
            assertThat(result.isNull(1), equalTo(true));
        } finally {
            result.close();
        }
    }

    private static Query contentTerm(String term) {
        return termQuery(CONTENT_FIELD, term);
    }

    private static Query termQuery(String field, String term) {
        return new TermQuery(new Term(field, term));
    }

    private static Analyzer standardAnalyzerWithGap(int gap) {
        return new NamedAnalyzer("standard", AnalyzerScope.GLOBAL, new StandardAnalyzer(), gap);
    }

    private static Analyzer shingleAnalyzer() {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new StandardTokenizer();
                ShingleFilter shingles = new ShingleFilter(new LowerCaseFilter(tokenizer), 2, 2);
                shingles.setOutputUnigrams(false);
                return new TokenStreamComponents(tokenizer, shingles);
            }
        };
    }

    private static Analyzer synonymAnalyzer(SynonymMap synonyms) {
        return new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new StandardTokenizer();
                TokenStream lowerCased = new LowerCaseFilter(tokenizer);
                return new TokenStreamComponents(tokenizer, new SynonymGraphFilter(lowerCased, synonyms, true));
            }
        };
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
        return highlight(config, query, new StandardAnalyzer(), input);
    }

    private BytesRefBlock highlight(HighlightConfig config, Query query, Analyzer analyzer, BytesRefBlock input) {
        try (
            HighlightOperator operator = new HighlightOperator(
                blockFactory(),
                config.withExecutionContext(analyzer, query, CONTENT),
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

    private static HighlightConfig configWithMaxAnalyzedOffset(String queryText, int maxAnalyzedOffset) {
        return new HighlightConfig(
            queryText,
            DEFAULT_PRE_TAG,
            DEFAULT_POST_TAG,
            DEFAULT_ENCODER,
            5,
            0,
            0,
            false,
            Locale.ROOT,
            false,
            null,
            maxAnalyzedOffset
        );
    }

}
