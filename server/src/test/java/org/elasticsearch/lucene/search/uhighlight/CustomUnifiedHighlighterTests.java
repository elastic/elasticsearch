/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.uhighlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.miscellaneous.LimitTokenOffsetFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizerFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymFilterFactory;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.ResourceLoader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.search.fetch.subphase.highlight.LimitTokenOffsetAnalyzer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.text.BreakIterator;
import java.text.ParseException;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.emptyArray;

public class CustomUnifiedHighlighterTests extends ESTestCase {

    private void assertHighlightOneDoc(
        String fieldName,
        String[] inputs,
        Analyzer analyzer,
        Query query,
        Locale locale,
        BreakIterator breakIterator,
        int noMatchSize,
        String[] expectedPassages
    ) throws Exception {

        assertHighlightOneDoc(
            fieldName,
            inputs,
            analyzer,
            query,
            locale,
            breakIterator,
            noMatchSize,
            expectedPassages,
            Integer.MAX_VALUE,
            null
        );
    }

    private void assertHighlightOneDoc(
        String fieldName,
        String[] inputs,
        Analyzer analyzer,
        Query query,
        Locale locale,
        BreakIterator breakIterator,
        int noMatchSize,
        String[] expectedPassages,
        int maxAnalyzedOffset,
        Integer queryMaxAnalyzedOffset
    ) throws Exception {
        assertHighlightOneDoc(
            fieldName,
            inputs,
            analyzer,
            query,
            locale,
            breakIterator,
            noMatchSize,
            expectedPassages,
            maxAnalyzedOffset,
            queryMaxAnalyzedOffset,
            UnifiedHighlighter.OffsetSource.ANALYSIS
        );
    }

    private void assertHighlightOneDoc(
        String fieldName,
        String[] inputs,
        Analyzer analyzer,
        Query query,
        Locale locale,
        BreakIterator breakIterator,
        int noMatchSize,
        String[] expectedPassages,
        int maxAnalyzedOffset,
        Integer queryMaxAnalyzedOffset,
        UnifiedHighlighter.OffsetSource offsetSource
    ) throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
            iwc.setMergePolicy(newTieredMergePolicy(random()));
            RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
            FieldType ft = new FieldType(TextField.TYPE_STORED);
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
            ft.freeze();
            Document doc = new Document();
            for (String input : inputs) {
                Field field = new Field(fieldName, "", ft);
                field.setStringValue(input);
                doc.add(field);
            }
            iw.addDocument(doc);
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                iw.close();
                TopDocs topDocs = searcher.search(Queries.ALL_DOCS_INSTANCE, 1, Sort.INDEXORDER);
                assertThat(topDocs.totalHits.value(), equalTo(1L));
                String rawValue = Strings.arrayToDelimitedString(inputs, String.valueOf(MULTIVAL_SEP_CHAR));
                UnifiedHighlighter.Builder builder = UnifiedHighlighter.builder(searcher, analyzer);
                builder.withBreakIterator(() -> breakIterator);
                builder.withFieldMatcher(name -> "text".equals(name));
                builder.withFormatter(new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder(), 3));
                CustomUnifiedHighlighter highlighter = new CustomUnifiedHighlighter(
                    builder,
                    offsetSource,
                    false,
                    locale,
                    "index",
                    "text",
                    query,
                    noMatchSize,
                    expectedPassages.length,
                    maxAnalyzedOffset,
                    QueryMaxAnalyzedOffset.create(queryMaxAnalyzedOffset, maxAnalyzedOffset),
                    true,
                    true
                );
                final Snippet[] snippets = highlighter.highlightField(getOnlyLeafReader(reader), topDocs.scoreDocs[0].doc, () -> rawValue);
                assertEquals(expectedPassages.length, snippets.length);
                for (int i = 0; i < snippets.length; i++) {
                    assertEquals(expectedPassages[i], snippets[i].getText());
                }
            }
        }
    }

    public void testSimple() throws Exception {
        final String[] inputs = {
            "This is a test. Just a test1 highlighting from unified highlighter.",
            "This is the second highlighting value to perform highlighting on a longer text that gets scored lower.",
            "This is highlighting the third short highlighting value.",
            "Just a test4 highlighting from unified highlighter." };

        String[] expectedPassages = {
            "Just a test1 <b>highlighting</b> from unified highlighter.",
            "This is the second <b>highlighting</b> value to perform <b>highlighting</b> on a" + " longer text that gets scored lower.",
            "This is <b>highlighting</b> the third short <b>highlighting</b> value.",
            "Just a test4 <b>highlighting</b> from unified highlighter." };
        Query query = new TermQuery(new Term("text", "highlighting"));
        assertHighlightOneDoc(
            "text",
            inputs,
            new StandardAnalyzer(),
            query,
            Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT),
            0,
            expectedPassages
        );
    }

    public void testNoMatchSize() throws Exception {
        final String[] inputs = { "This is a test. Just a test highlighting from unified. Feel free to ignore." };
        Query query = new TermQuery(new Term("body", "highlighting"));
        assertHighlightOneDoc(
            "text",
            inputs,
            new StandardAnalyzer(),
            query,
            Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT),
            100,
            inputs
        );
    }

    public void testMultiPhrasePrefixQuerySingleTerm() throws Exception {
        final String[] inputs = { "The quick brown fox." };
        final String[] outputs = { "The quick <b>brown</b> fox." };
        MultiPhrasePrefixQuery query = new MultiPhrasePrefixQuery("text");
        query.add(new Term("text", "bro"));
        assertHighlightOneDoc(
            "text",
            inputs,
            new StandardAnalyzer(),
            query,
            Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT),
            0,
            outputs
        );
    }

    public void testMultiPhrasePrefixQuery() throws Exception {
        final String[] inputs = { "The quick brown fox." };
        final String[] outputs = { "The <b>quick brown fox</b>." };
        MultiPhrasePrefixQuery query = new MultiPhrasePrefixQuery("text");
        query.add(new Term("text", "quick"));
        query.add(new Term("text", "brown"));
        query.add(new Term("text", "fo"));
        assertHighlightOneDoc(
            "text",
            inputs,
            new StandardAnalyzer(),
            query,
            Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT),
            0,
            outputs
        );
    }

    /** Single-document readers retain postings offsets for multi-term queries. */
    public void testMultiTermQueryKeepsPostingsOffsetSourceForSingleDocumentReader() throws Exception {
        final String content = "The quick brown fox.";
        final String expectedSnippet = "The <b>quick</b> brown fox.";
        for (Query query : new Query[] {
            new PrefixQuery(new Term("text", "qui")),
            new WildcardQuery(new Term("text", "qu*ck")),
            new FuzzyQuery(new Term("text", "quack")) }) {
            assertOffsetSourceAndSnippet(content, query, false, UnifiedHighlighter.OffsetSource.ANALYSIS, expectedSnippet);
            assertOffsetSourceAndSnippet(content, query, true, UnifiedHighlighter.OffsetSource.POSTINGS, expectedSnippet);
        }
    }

    public void testRewrittenAndUnrecognizedQueriesKeepPostingsOffsetSourceForSingleDocumentReader() throws Exception {
        final String content = "The quick brown fox.";

        // FieldExistsQuery cannot be converted to an automaton.
        final String termSnippet = "The <b>quick</b> brown fox.";
        Query unrecognized = new BooleanQuery.Builder().add(new TermQuery(new Term("text", "quick")), BooleanClause.Occur.MUST)
            .add(new FieldExistsQuery("text"), BooleanClause.Occur.MUST)
            .build();
        assertOffsetSourceAndSnippet(content, unrecognized, false, UnifiedHighlighter.OffsetSource.ANALYSIS, termSnippet);
        assertOffsetSourceAndSnippet(content, unrecognized, true, UnifiedHighlighter.OffsetSource.POSTINGS, termSnippet);

        // Disable weight matches so Lucene rewrites the phrase before choosing the offset source.
        final String rewrittenSnippet = "The <b>quick</b> <b>brown</b> <b>fox</b>.";
        MultiPhrasePrefixQuery phrasePrefix = new MultiPhrasePrefixQuery("text");
        phrasePrefix.add(new Term("text", "quick"));
        phrasePrefix.add(new Term("text", "brown"));
        phrasePrefix.add(new Term("text", "fo"));
        assertOffsetSourceAndSnippet(content, phrasePrefix, false, UnifiedHighlighter.OffsetSource.ANALYSIS, rewrittenSnippet, false);
        assertOffsetSourceAndSnippet(content, phrasePrefix, true, UnifiedHighlighter.OffsetSource.POSTINGS, rewrittenSnippet, false);
    }

    public void testQueryMaxAnalyzedOffsetBoundsMultiTermMatchingLikeQueryDsl() throws Exception {
        final String content = "quick brown padding padding fox"; // "fox" starts at offset 28, past the cutoff
        final int cutoff = 20;
        Query pastCutoff = new BooleanQuery.Builder().add(new TermQuery(new Term("text", "quick")), BooleanClause.Occur.MUST)
            .add(new PrefixQuery(new Term("text", "fo")), BooleanClause.Occur.MUST)
            .build();
        assertThat(highlightWithTruncatedAnalyzer(content, pastCutoff, UnifiedHighlighter.OffsetSource.POSTINGS, cutoff), emptyArray());
        assertThat(highlightWithTruncatedAnalyzer(content, pastCutoff, UnifiedHighlighter.OffsetSource.ANALYSIS, cutoff), emptyArray());
        assertThat(highlightWithTruncatedIndex(content, pastCutoff, cutoff), emptyArray());

        // Matches before the cutoff should produce the same snippet.
        Query insideCutoff = new BooleanQuery.Builder().add(new TermQuery(new Term("text", "quick")), BooleanClause.Occur.MUST)
            .add(new PrefixQuery(new Term("text", "bro")), BooleanClause.Occur.MUST)
            .build();
        final String expected = "<b>quick</b> <b>brown</b> padding padding fox";
        for (Snippet[] snippets : new Snippet[][] {
            highlightWithTruncatedAnalyzer(content, insideCutoff, UnifiedHighlighter.OffsetSource.POSTINGS, cutoff),
            highlightWithTruncatedAnalyzer(content, insideCutoff, UnifiedHighlighter.OffsetSource.ANALYSIS, cutoff),
            highlightWithTruncatedIndex(content, insideCutoff, cutoff) }) {
            assertThat(snippets.length, equalTo(1));
            assertThat(snippets[0].getText(), equalTo(expected));
        }
    }

    private Snippet[] highlightWithTruncatedAnalyzer(String content, Query query, UnifiedHighlighter.OffsetSource offsetSource, int cutoff)
        throws IOException {
        Analyzer analyzer = new StandardAnalyzer();
        MemoryIndex memoryIndex = new MemoryIndex(true);
        memoryIndex.addField("text", content, analyzer);
        return highlightWithCutoff(
            memoryIndex.createSearcher(),
            new LimitTokenOffsetAnalyzer(analyzer, cutoff),
            query,
            offsetSource,
            false,
            cutoff,
            content
        );
    }

    private Snippet[] highlightWithTruncatedIndex(String content, Query query, int cutoff) throws IOException {
        Analyzer analyzer = new StandardAnalyzer();
        MemoryIndex memoryIndex = new MemoryIndex(true);
        memoryIndex.addField("text", new LimitTokenOffsetFilter(analyzer.tokenStream("text", content), cutoff, false));
        return highlightWithCutoff(
            memoryIndex.createSearcher(),
            analyzer,
            query,
            UnifiedHighlighter.OffsetSource.POSTINGS,
            true,
            cutoff,
            content
        );
    }

    private Snippet[] highlightWithCutoff(
        IndexSearcher searcher,
        Analyzer analyzer,
        Query query,
        UnifiedHighlighter.OffsetSource offsetSource,
        boolean singleDocumentReader,
        int cutoff,
        String content
    ) throws IOException {
        UnifiedHighlighter.Builder builder = UnifiedHighlighter.builder(searcher, analyzer);
        builder.withBreakIterator(() -> BreakIterator.getSentenceInstance(Locale.ROOT));
        builder.withFormatter(new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder(), 3));
        CustomUnifiedHighlighter highlighter = new CustomUnifiedHighlighter(
            builder,
            offsetSource,
            singleDocumentReader,
            Locale.ROOT,
            "index",
            "text",
            query,
            0,
            1,
            Integer.MAX_VALUE,
            QueryMaxAnalyzedOffset.create(cutoff, Integer.MAX_VALUE),
            true,
            true
        );
        return highlighter.highlightField((LeafReader) searcher.getIndexReader(), 0, () -> content);
    }

    private void assertOffsetSourceAndSnippet(
        String content,
        Query query,
        boolean singleDocumentReader,
        UnifiedHighlighter.OffsetSource expectedOffsetSource,
        String expectedSnippet
    ) throws IOException {
        assertOffsetSourceAndSnippet(content, query, singleDocumentReader, expectedOffsetSource, expectedSnippet, true);
    }

    private void assertOffsetSourceAndSnippet(
        String content,
        Query query,
        boolean singleDocumentReader,
        UnifiedHighlighter.OffsetSource expectedOffsetSource,
        String expectedSnippet,
        boolean weightMatchesEnabled
    ) throws IOException {
        Analyzer analyzer = new StandardAnalyzer();
        MemoryIndex memoryIndex = new MemoryIndex(true);
        memoryIndex.addField("text", content, analyzer);
        IndexSearcher searcher = memoryIndex.createSearcher();
        UnifiedHighlighter.Builder builder = UnifiedHighlighter.builder(searcher, analyzer);
        builder.withBreakIterator(() -> BreakIterator.getSentenceInstance(Locale.ROOT));
        builder.withFormatter(new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder(), 3));
        CustomUnifiedHighlighter highlighter = new CustomUnifiedHighlighter(
            builder,
            UnifiedHighlighter.OffsetSource.POSTINGS,
            singleDocumentReader,
            Locale.ROOT,
            "index",
            "text",
            query,
            0,
            1,
            Integer.MAX_VALUE,
            QueryMaxAnalyzedOffset.create(null, Integer.MAX_VALUE),
            true,
            weightMatchesEnabled
        );
        assertThat(highlighter.resolvedOffsetSource(), equalTo(expectedOffsetSource));
        LeafReader reader = (LeafReader) searcher.getIndexReader();
        Snippet[] snippets = highlighter.highlightField(reader, 0, () -> content);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0].getText(), equalTo(expectedSnippet));
    }

    public void testSentenceBoundedBreakIterator() throws Exception {
        final String[] inputs = {
            "The quick brown fox in a long sentence with another quick brown fox. " + "Another sentence with brown fox." };
        final String[] outputs = {
            "The <b>quick</b> <b>brown</b>",
            "<b>fox</b> in a long",
            "another <b>quick</b>",
            "<b>brown</b> <b>fox</b>.",
            "sentence with <b>brown</b>",
            "<b>fox</b>.", };
        BooleanQuery query = new BooleanQuery.Builder().add(new TermQuery(new Term("text", "quick")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "brown")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "fox")), BooleanClause.Occur.SHOULD)
            .build();
        assertHighlightOneDoc(
            "text",
            inputs,
            new StandardAnalyzer(),
            query,
            Locale.ROOT,
            BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 10),
            0,
            outputs
        );
    }

    public void testSmallSentenceBoundedBreakIterator() throws Exception {
        final String[] inputs = { "A short sentence. Followed by a bigger sentence that should be truncated. And a last short sentence." };
        final String[] outputs = { "A short <b>sentence</b>.", "Followed by a bigger <b>sentence</b>", "And a last short <b>sentence</b>" };
        TermQuery query = new TermQuery(new Term("text", "sentence"));
        assertHighlightOneDoc(
            "text",
            inputs,
            new StandardAnalyzer(),
            query,
            Locale.ROOT,
            BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 20),
            0,
            outputs
        );
    }

    public void testRepeatTerm() throws Exception {
        final String[] inputs = { "Fun  fun fun  fun  fun  fun  fun  fun  fun  fun" };
        final String[] outputs = {
            "<b>Fun</b>  <b>fun</b> <b>fun</b>",
            "<b>fun</b>  <b>fun</b>",
            "<b>fun</b>  <b>fun</b>  <b>fun</b>",
            "<b>fun</b>  <b>fun</b>" };
        Query query = new TermQuery(new Term("text", "fun"));
        assertHighlightOneDoc(
            "text",
            inputs,
            new StandardAnalyzer(),
            query,
            Locale.ROOT,
            BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 10),
            0,
            outputs
        );
    }

    public void testRepeatPhrase() throws Exception {
        final String[] inputs = { "Fun  fun fun  fun  fun  fun  fun  fun  fun  fun" };
        final String[] outputs = { "<b>Fun  fun fun</b>", "<b>fun  fun  </b>", "<b>fun  fun  fun</b>", "<b>fun  fun</b>" };
        Query query = new PhraseQuery.Builder().add(new Term("text", "fun")).add(new Term("text", "fun")).build();
        assertHighlightOneDoc(
            "text",
            inputs,
            new StandardAnalyzer(),
            query,
            Locale.ROOT,
            BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 10),
            0,
            outputs
        );
    }

    public void testGroupSentences() throws Exception {
        final String[] inputs = { "Two words. Followed by many words in a big sentence. One. Two. Three. And more words." };
        final String[] outputs = {
            "<b>Two</b> <b>words</b>.",
            "Followed by many <b>words</b>",
            "<b>One</b>. <b>Two</b>. <b>Three</b>.",
            "And more <b>words</b>.", };
        BooleanQuery query = new BooleanQuery.Builder().add(new TermQuery(new Term("text", "one")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "two")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "three")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "words")), BooleanClause.Occur.SHOULD)
            .build();
        assertHighlightOneDoc(
            "text",
            inputs,
            new StandardAnalyzer(),
            query,
            Locale.ROOT,
            BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 20),
            0,
            outputs
        );
    }

    public void testOverlappingTerms() throws Exception {
        final String[] inputs = { "bro", "brown", "brownie", "browser" };
        final String[] outputs = { "<b>bro</b>", "<b>brown</b>", "<b>browni</b>e", "<b>browser</b>" };
        BooleanQuery query = new BooleanQuery.Builder().add(new FuzzyQuery(new Term("text", "brow")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "b")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "br")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "bro")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "brown")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "browni")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "browser")), BooleanClause.Occur.SHOULD)
            .build();
        Analyzer analyzer = CustomAnalyzer.builder()
            .withTokenizer(EdgeNGramTokenizerFactory.class, "minGramSize", "1", "maxGramSize", "7")
            .build();
        assertHighlightOneDoc("text", inputs, analyzer, query, Locale.ROOT, BreakIterator.getSentenceInstance(Locale.ROOT), 0, outputs);
    }

    public static class NYCFilterFactory extends SynonymFilterFactory {
        public NYCFilterFactory(Map<String, String> args) {
            super(args);
        }

        @Override
        protected SynonymMap loadSynonyms(ResourceLoader loader, String cname, boolean dedup, Analyzer analyzer) throws IOException,
            ParseException {
            SynonymMap.Parser parser = new SolrSynonymParser(false, false, analyzer);
            parser.parse(new StringReader("new york city => nyc, new york city"));
            return parser.build();
        }
    }

    public void testOverlappingPositions() throws Exception {
        final String[] inputs = { "new york city" };
        final String[] outputs = { "<b>new york city</b>" };
        BooleanQuery query = new BooleanQuery.Builder().add(
            new BooleanQuery.Builder().add(new TermQuery(new Term("text", "nyc")), BooleanClause.Occur.SHOULD)
                .add(
                    new BooleanQuery.Builder().add(new TermQuery(new Term("text", "new")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("text", "york")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("text", "city")), BooleanClause.Occur.MUST)
                        .build(),
                    BooleanClause.Occur.SHOULD
                )
                .build(),
            BooleanClause.Occur.MUST
        ).build();
        Analyzer analyzer = CustomAnalyzer.builder()
            .withTokenizer(StandardTokenizerFactory.class)
            .addTokenFilter(NYCFilterFactory.class, "synonyms", "N/A")
            .build();
        assertHighlightOneDoc("text", inputs, analyzer, query, Locale.ROOT, BreakIterator.getSentenceInstance(Locale.ROOT), 0, outputs);
    }

    public void testPhraseSpanningMultipleValues() throws Exception {
        final String[] inputs = { "If you say things to a person that turn out not to be true", "the person is not going to believe" };
        final String[] outputs = { "If you say things to a person that turn out not <b>to be true the person</b>" };
        Query query = new PhraseQuery.Builder().add(new Term("text", "to"))
            .add(new Term("text", "be"))
            .add(new Term("text", "true"))
            .add(new Term("text", "the"))
            .add(new Term("text", "person"))
            .build();
        assertHighlightOneDoc(
            "text",
            inputs,
            new StandardAnalyzer(),
            query,
            Locale.ROOT,
            BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 256),
            0,
            outputs
        );
    }

    public void testExceedMaxAnalyzedOffset() throws Exception {
        TermQuery query = new TermQuery(new Term("text", "max"));
        Analyzer analyzer = CustomAnalyzer.builder()
            .withTokenizer(EdgeNGramTokenizerFactory.class, "minGramSize", "1", "maxGramSize", "10")
            .build();

        assertHighlightOneDoc(
            "text",
            new String[] { "short text" },
            analyzer,
            query,
            Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT),
            0,
            new String[] {},
            10,
            null
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            assertHighlightOneDoc(
                "text",
                new String[] { "exceeds max analyzed offset" },
                analyzer,
                query,
                Locale.ROOT,
                BreakIterator.getSentenceInstance(Locale.ROOT),
                0,
                new String[] {},
                10,
                null
            );
        });
        assertEquals(
            "The length [27] of field [text] in doc[0]/index[index] exceeds the [index.highlight.max_analyzed_offset] limit [10]. "
                + "To avoid this error, set the query parameter [max_analyzed_offset] to a value less than index setting [10] and this "
                + "will tolerate long field values by truncating them.",
            e.getMessage()
        );

        final Integer queryMaxAnalyzedOffset = randomIntBetween(11, 1000);
        e = expectThrows(IllegalArgumentException.class, () -> {
            assertHighlightOneDoc(
                "text",
                new String[] { "exceeds max analyzed offset" },
                analyzer,
                query,
                Locale.ROOT,
                BreakIterator.getSentenceInstance(Locale.ROOT),
                0,
                new String[] {},
                10,
                queryMaxAnalyzedOffset
            );
        });
        assertEquals(
            "The length [27] of field [text] in doc[0]/index[index] exceeds the [index.highlight.max_analyzed_offset] limit [10]. "
                + "To avoid this error, set the query parameter [max_analyzed_offset] to a value less than index setting [10] and this "
                + "will tolerate long field values by truncating them.",
            e.getMessage()
        );

        assertHighlightOneDoc(
            "text",
            new String[] { "exceeds max analyzed offset" },
            analyzer,
            query,
            Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT),
            1,
            new String[] { "exceeds" },
            10,
            10
        );
    }

    public void testExceedMaxAnalyzedOffsetWithRepeatedWords() throws Exception {

        TermQuery query = new TermQuery(new Term("text", "Fun"));
        Analyzer analyzer = new WhitespaceAnalyzer();
        assertHighlightOneDoc(
            "text",
            new String[] { "Testing Fun Testing Fun" },
            analyzer,
            query,
            Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT),
            0,
            new String[] { "Testing <b>Fun</b> Testing Fun" },
            29,
            10,
            UnifiedHighlighter.OffsetSource.ANALYSIS
        );
        assertHighlightOneDoc(
            "text",
            new String[] { "Testing Fun Testing Fun" },
            analyzer,
            query,
            Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT),
            0,
            new String[] { "Testing <b>Fun</b> Testing Fun" },
            29,
            10,
            UnifiedHighlighter.OffsetSource.POSTINGS
        );
    }

    public void testExceedMaxAnalyzedOffsetRandomOffset() throws Exception {
        TermQuery query = new TermQuery(new Term("text", "fun"));
        Analyzer analyzer = new WhitespaceAnalyzer();
        UnifiedHighlighter.OffsetSource offsetSource = randomBoolean()
            ? UnifiedHighlighter.OffsetSource.ANALYSIS
            : UnifiedHighlighter.OffsetSource.POSTINGS;
        final String[] inputs = { "Fun fun fun fun fun" };
        TreeMap<Integer, String> outputs = new TreeMap<>(
            Map.of(
                7,
                "Fun <b>fun</b> fun fun fun",
                11,
                "Fun <b>fun</b> <b>fun</b> fun fun",
                15,
                "Fun <b>fun</b> <b>fun</b> <b>fun</b> fun",
                19,
                "Fun <b>fun</b> <b>fun</b> <b>fun</b> <b>fun</b>"
            )
        );
        Integer randomOffset = between(7, 19);
        String output = outputs.ceilingEntry(randomOffset).getValue();
        assertHighlightOneDoc(
            "text",
            inputs,
            analyzer,
            query,
            Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT),
            0,
            new String[] { output },
            47,
            randomOffset,
            offsetSource
        );
    }
}
