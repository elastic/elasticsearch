/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
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
import org.apache.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.apache.lucene.search.uhighlight.Snippet;
import org.apache.lucene.search.uhighlight.SplittingBreakIterator;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedHighlighterAnalyzer;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedText;
import org.elasticsearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotationAnalyzerWrapper;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.subphase.highlight.AnnotatedPassageFormatter;
import org.elasticsearch.test.ESTestCase;

import java.net.URLEncoder;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Locale;

import static org.apache.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;
import static org.hamcrest.CoreMatchers.equalTo;

public class AnnotatedTextHighlighterTests extends ESTestCase {
    
    private void assertHighlightOneDoc(String fieldName, String []markedUpInputs,
            Query query, Locale locale, BreakIterator breakIterator,
            int noMatchSize, String[] expectedPassages) throws Exception {
        
        
        // Annotated fields wrap the usual analyzer with one that injects extra tokens
        Analyzer wrapperAnalyzer = new AnnotationAnalyzerWrapper(new StandardAnalyzer());
        HitContext mockHitContext = new HitContext();
        AnnotatedHighlighterAnalyzer hiliteAnalyzer = new AnnotatedHighlighterAnalyzer(wrapperAnalyzer, mockHitContext);
        
        AnnotatedText[] annotations = new AnnotatedText[markedUpInputs.length];
        for (int i = 0; i < markedUpInputs.length; i++) {
            annotations[i] = AnnotatedText.parse(markedUpInputs[i]);
        }
        mockHitContext.cache().put(AnnotatedText.class.getName(), annotations);

        AnnotatedPassageFormatter passageFormatter = new AnnotatedPassageFormatter(annotations,new DefaultEncoder());
        
        ArrayList<Object> plainTextForHighlighter = new ArrayList<>(annotations.length);
        for (int i = 0; i < annotations.length; i++) {
            plainTextForHighlighter.add(annotations[i].textMinusMarkup);
        }        
        
        Directory dir = newDirectory();
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
        DirectoryReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 1, Sort.INDEXORDER);
        assertThat(topDocs.totalHits.value, equalTo(1L));
        String rawValue = Strings.collectionToDelimitedString(plainTextForHighlighter, String.valueOf(MULTIVAL_SEP_CHAR));
        
        CustomUnifiedHighlighter highlighter = new CustomUnifiedHighlighter(searcher, hiliteAnalyzer, null,
                passageFormatter, locale,
                breakIterator, rawValue, noMatchSize);
        highlighter.setFieldMatcher((name) -> "text".equals(name));
        final Snippet[] snippets =
            highlighter.highlightField("text", query, topDocs.scoreDocs[0].doc, expectedPassages.length);
        assertEquals(expectedPassages.length, snippets.length);
        for (int i = 0; i < snippets.length; i++) {
            assertEquals(expectedPassages[i], snippets[i].getText());
        }
        reader.close();
        dir.close();
    }
    

    public void testAnnotatedTextStructuredMatch() throws Exception {
        // Check that a structured token eg a URL can be highlighted in a query
        // on marked-up
        // content using an "annotated_text" type field.
        String url = "https://en.wikipedia.org/wiki/Key_Word_in_Context";
        String encodedUrl = URLEncoder.encode(url, "UTF-8");
        String annotatedWord = "[highlighting](" + encodedUrl + ")";
        String highlightedAnnotatedWord = "[highlighting](" + AnnotatedPassageFormatter.SEARCH_HIT_TYPE + "=" + encodedUrl + "&"
                + encodedUrl + ")";
        final String[] markedUpInputs = { "This is a test. Just a test1 " + annotatedWord + " from [annotated](bar) highlighter.",
                "This is the second " + annotatedWord + " value to perform highlighting on a longer text that gets scored lower." };

        String[] expectedPassages = {
                "This is a test. Just a test1 " + highlightedAnnotatedWord + " from [annotated](bar) highlighter.",
                "This is the second " + highlightedAnnotatedWord + " value to perform highlighting on a"
                        + " longer text that gets scored lower." };
        Query query = new TermQuery(new Term("text", url));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testAnnotatedTextOverlapsWithUnstructuredSearchTerms() throws Exception {
        final String[] markedUpInputs = { "[Donald Trump](Donald+Trump) visited Singapore",
                "Donald duck is a [Disney](Disney+Inc) invention" };

        String[] expectedPassages = { "[Donald](_hit_term=donald) Trump visited Singapore",
                "[Donald](_hit_term=donald) duck is a [Disney](Disney+Inc) invention" };
        Query query = new TermQuery(new Term("text", "donald"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testAnnotatedTextMultiFieldWithBreakIterator() throws Exception {
        final String[] markedUpInputs = { "[Donald Trump](Donald+Trump) visited Singapore. Kim shook hands with Donald",
                "Donald duck is a [Disney](Disney+Inc) invention" };
        String[] expectedPassages = { "[Donald](_hit_term=donald) Trump visited Singapore",
                "Kim shook hands with [Donald](_hit_term=donald)",
                "[Donald](_hit_term=donald) duck is a [Disney](Disney+Inc) invention" };
        Query query = new TermQuery(new Term("text", "donald"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        breakIterator = new SplittingBreakIterator(breakIterator, '.');
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }
    
    public void testAnnotatedTextSingleFieldWithBreakIterator() throws Exception {
        final String[] markedUpInputs = { "[Donald Trump](Donald+Trump) visited Singapore. Kim shook hands with Donald"};
        String[] expectedPassages = { "[Donald](_hit_term=donald) Trump visited Singapore",
                "Kim shook hands with [Donald](_hit_term=donald)"};
        Query query = new TermQuery(new Term("text", "donald"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        breakIterator = new SplittingBreakIterator(breakIterator, '.');
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }  
    
    public void testAnnotatedTextSingleFieldWithPhraseQuery() throws Exception {
        final String[] markedUpInputs = { "[Donald Trump](Donald+Trump) visited Singapore", 
                "Donald Jr was with Melania Trump"};
        String[] expectedPassages = { "[Donald](_hit_term=donald) [Trump](_hit_term=trump) visited Singapore"};
        Query query = new PhraseQuery("text", "donald", "trump");
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }   
    
    public void testBadAnnotation() throws Exception {
        final String[] markedUpInputs = { "Missing bracket for [Donald Trump](Donald+Trump visited Singapore"};
        String[] expectedPassages = { "Missing bracket for [Donald Trump](Donald+Trump visited [Singapore](_hit_term=singapore)"};
        Query query = new TermQuery(new Term("text", "singapore"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }     
   
}
