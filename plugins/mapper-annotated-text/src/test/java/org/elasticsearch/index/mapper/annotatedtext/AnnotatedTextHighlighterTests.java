/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.annotatedtext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.uhighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.search.uhighlight.SplittingBreakIterator;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedHighlighterAnalyzer;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedText;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotationAnalyzerWrapper;
import org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.elasticsearch.lucene.search.uhighlight.Snippet;
import org.elasticsearch.search.fetch.subphase.highlight.LimitTokenOffsetAnalyzer;
import org.elasticsearch.test.ESTestCase;

import java.net.URLEncoder;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Locale;

import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;
import static org.hamcrest.CoreMatchers.equalTo;

public class AnnotatedTextHighlighterTests extends ESTestCase {

    private void assertHighlightOneDoc(
        String fieldName,
        String[] markedUpInputs,
        Query query,
        Locale locale,
        BreakIterator breakIterator,
        int noMatchSize,
        String[] expectedPassages
    ) throws Exception {

        assertHighlightOneDoc(
            fieldName,
            markedUpInputs,
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
        String[] markedUpInputs,
        Query query,
        Locale locale,
        BreakIterator breakIterator,
        int noMatchSize,
        String[] expectedPassages,
        int maxAnalyzedOffset,
        Integer queryMaxAnalyzedOffset
    ) throws Exception {

        try (Directory dir = newDirectory()) {
            // Annotated fields wrap the usual analyzer with one that injects extra tokens
            Analyzer wrapperAnalyzer = new AnnotationAnalyzerWrapper(new StandardAnalyzer());
            IndexWriterConfig iwc = newIndexWriterConfig(wrapperAnalyzer);
            iwc.setMergePolicy(newTieredMergePolicy(random()));
            RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
            FieldType ft = new FieldType(TextField.TYPE_STORED);
            if (randomBoolean()) {
                ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
            } else {
                ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
            }
            ft.freeze();
            Document doc = new Document();
            for (String input : markedUpInputs) {
                Field field = new Field(fieldName, "", ft);
                field.setStringValue(input);
                doc.add(field);
            }
            iw.addDocument(doc);
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                iw.close();

                AnnotatedText[] annotations = new AnnotatedText[markedUpInputs.length];
                for (int i = 0; i < markedUpInputs.length; i++) {
                    annotations[i] = AnnotatedText.parse(markedUpInputs[i]);
                }
                if (queryMaxAnalyzedOffset != null) {
                    wrapperAnalyzer = new LimitTokenOffsetAnalyzer(wrapperAnalyzer, queryMaxAnalyzedOffset);
                }
                AnnotatedHighlighterAnalyzer hiliteAnalyzer = new AnnotatedHighlighterAnalyzer(wrapperAnalyzer);
                hiliteAnalyzer.setAnnotations(annotations);
                AnnotatedPassageFormatter passageFormatter = new AnnotatedPassageFormatter(new DefaultEncoder());
                passageFormatter.setAnnotations(annotations);

                ArrayList<Object> plainTextForHighlighter = new ArrayList<>(annotations.length);
                for (int i = 0; i < annotations.length; i++) {
                    plainTextForHighlighter.add(annotations[i].textMinusMarkup());
                }

                TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 1, Sort.INDEXORDER);
                assertThat(topDocs.totalHits.value, equalTo(1L));
                String rawValue = Strings.collectionToDelimitedString(plainTextForHighlighter, String.valueOf(MULTIVAL_SEP_CHAR));
                CustomUnifiedHighlighter highlighter = new CustomUnifiedHighlighter(
                    searcher,
                    hiliteAnalyzer,
                    UnifiedHighlighter.OffsetSource.ANALYSIS,
                    passageFormatter,
                    locale,
                    breakIterator,
                    "index",
                    "text",
                    query,
                    noMatchSize,
                    expectedPassages.length,
                    name -> "text".equals(name),
                    maxAnalyzedOffset,
                    queryMaxAnalyzedOffset
                );
                highlighter.setFieldMatcher((name) -> "text".equals(name));
                final Snippet[] snippets = highlighter.highlightField(getOnlyLeafReader(reader), topDocs.scoreDocs[0].doc, () -> rawValue);
                assertEquals(expectedPassages.length, snippets.length);
                for (int i = 0; i < snippets.length; i++) {
                    assertEquals(expectedPassages[i], snippets[i].getText());
                }
            }
        }
    }

    public void testAnnotatedTextStructuredMatch() throws Exception {
        // Check that a structured token eg a URL can be highlighted in a query
        // on marked-up
        // content using an "annotated_text" type field.
        String url = "https://en.wikipedia.org/wiki/Key_Word_in_Context";
        String encodedUrl = URLEncoder.encode(url, "UTF-8");
        String annotatedWord = "[highlighting](" + encodedUrl + ")";
        String highlightedAnnotatedWord = "[highlighting]("
            + AnnotatedPassageFormatter.SEARCH_HIT_TYPE
            + "="
            + encodedUrl
            + "&"
            + encodedUrl
            + ")";
        final String[] markedUpInputs = {
            "This is a test. Just a test1 " + annotatedWord + " from [annotated](bar) highlighter.",
            "This is the second " + annotatedWord + " value to perform highlighting on a longer text that gets scored lower." };

        String[] expectedPassages = {
            "This is a test. Just a test1 " + highlightedAnnotatedWord + " from [annotated](bar) highlighter.",
            "This is the second "
                + highlightedAnnotatedWord
                + " value to perform highlighting on a"
                + " longer text that gets scored lower." };
        Query query = new TermQuery(new Term("text", url));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testAnnotatedTextOverlapsWithUnstructuredSearchTerms() throws Exception {
        final String[] markedUpInputs = {
            "[Donald Trump](Donald+Trump) visited Singapore",
            "Donald duck is a [Disney](Disney+Inc) invention" };

        String[] expectedPassages = {
            "[Donald](_hit_term=donald) Trump visited Singapore",
            "[Donald](_hit_term=donald) duck is a [Disney](Disney+Inc) invention" };
        Query query = new TermQuery(new Term("text", "donald"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testAnnotatedTextMultiFieldWithBreakIterator() throws Exception {
        final String[] markedUpInputs = {
            "[Donald Trump](Donald+Trump) visited Singapore. Kim shook hands with Donald",
            "Donald duck is a [Disney](Disney+Inc) invention" };
        String[] expectedPassages = {
            "[Donald](_hit_term=donald) Trump visited Singapore",
            "Kim shook hands with [Donald](_hit_term=donald)",
            "[Donald](_hit_term=donald) duck is a [Disney](Disney+Inc) invention" };
        Query query = new TermQuery(new Term("text", "donald"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        breakIterator = new SplittingBreakIterator(breakIterator, '.');
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testAnnotatedTextSingleFieldWithBreakIterator() throws Exception {
        final String[] markedUpInputs = { "[Donald Trump](Donald+Trump) visited Singapore. Kim shook hands with Donald" };
        String[] expectedPassages = {
            "[Donald](_hit_term=donald) Trump visited Singapore",
            "Kim shook hands with [Donald](_hit_term=donald)" };
        Query query = new TermQuery(new Term("text", "donald"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        breakIterator = new SplittingBreakIterator(breakIterator, '.');
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testAnnotatedTextSingleFieldWithPhraseQuery() throws Exception {
        final String[] markedUpInputs = { "[Donald Trump](Donald+Trump) visited Singapore", "Donald Jr was with Melania Trump" };
        String[] expectedPassages = { "[Donald](_hit_term=donald) [Trump](_hit_term=trump) visited Singapore" };
        Query query = new PhraseQuery("text", "donald", "trump");
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testBadAnnotation() throws Exception {
        final String[] markedUpInputs = { "Missing bracket for [Donald Trump](Donald+Trump visited Singapore" };
        String[] expectedPassages = { "Missing bracket for [Donald Trump](Donald+Trump visited [Singapore](_hit_term=singapore)" };
        Query query = new TermQuery(new Term("text", "singapore"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testExceedMaxAnalyzedOffset() throws Exception {
        TermQuery query = new TermQuery(new Term("text", "exceeds"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc(
            "text",
            new String[] { "[Short Text](Short+Text)" },
            query,
            Locale.ROOT,
            breakIterator,
            0,
            new String[] {},
            10,
            null
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> assertHighlightOneDoc(
                "text",
                new String[] { "[Long Text exceeds](Long+Text+exceeds) MAX analyzed offset)" },
                query,
                Locale.ROOT,
                breakIterator,
                0,
                new String[] {},
                20,
                null
            )
        );
        assertEquals(
            "The length [38] of field [text] in doc[0]/index[index] exceeds the [index.highlight.max_analyzed_offset] limit [20]. "
                + "To avoid this error, set the query parameter [max_analyzed_offset] to a value less than index setting [20] and this "
                + "will tolerate long field values by truncating them.",
            e.getMessage()
        );

        final Integer queryMaxOffset = randomIntBetween(21, 1000);
        e = expectThrows(
            IllegalArgumentException.class,
            () -> assertHighlightOneDoc(
                "text",
                new String[] { "[Long Text exceeds](Long+Text+exceeds) MAX analyzed offset)" },
                query,
                Locale.ROOT,
                breakIterator,
                0,
                new String[] {},
                20,
                queryMaxOffset
            )
        );
        assertEquals(
            "The length [38] of field [text] in doc[0]/index[index] exceeds the [index.highlight.max_analyzed_offset] limit [20]. "
                + "To avoid this error, set the query parameter [max_analyzed_offset] to a value less than index setting [20] and this "
                + "will tolerate long field values by truncating them.",
            e.getMessage()
        );

        assertHighlightOneDoc(
            "text",
            new String[] { "[Long Text Exceeds](Long+Text+Exceeds) MAX analyzed offset [Long Text Exceeds](Long+Text+Exceeds)" },
            query,
            Locale.ROOT,
            breakIterator,
            0,
            new String[] { "Long Text [Exceeds](_hit_term=exceeds) MAX analyzed offset [Long Text Exceeds](Long+Text+Exceeds)" },
            20,
            15
        );
    }
}
