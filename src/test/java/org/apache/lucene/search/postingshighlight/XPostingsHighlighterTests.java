/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.lucene.search.postingshighlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.BreakIterator;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static org.hamcrest.CoreMatchers.*;

public class XPostingsHighlighterTests extends ElasticsearchTestCase {

    /*
    Tests changes needed to make possible to perform discrete highlighting.
    We want to highlight every field value separately in case of multiple values, at least when needing to return the whole field content
    This is needed to be able to get back a single snippet per value when number_of_fragments=0
     */
    @Test
    public void testDiscreteHighlightingPerValue() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        final String firstValue = "This is a test. Just a test highlighting from postings highlighter.";
        Document doc = new Document();
        doc.add(body);
        body.setStringValue(firstValue);

        final String secondValue = "This is the second value to perform highlighting on.";
        Field body2 = new Field("body", "", offsetsType);
        doc.add(body2);
        body2.setStringValue(secondValue);

        final String thirdValue = "This is the third value to test highlighting with postings.";
        Field body3 = new Field("body", "", offsetsType);
        doc.add(body3);
        body3.setStringValue(thirdValue);

        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter() {
            @Override
            protected BreakIterator getBreakIterator(String field) {
                return new WholeBreakIterator();
            }

            @Override
            protected char getMultiValuedSeparator(String field) {
                //U+2029 PARAGRAPH SEPARATOR (PS): each value holds a discrete passage for highlighting
                return 8233;
            }
        };
        Query query = new TermQuery(new Term("body", "highlighting"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));

        String firstHlValue = "This is a test. Just a test <b>highlighting</b> from postings highlighter.";
        String secondHlValue = "This is the second value to perform <b>highlighting</b> on.";
        String thirdHlValue = "This is the third value to test <b>highlighting</b> with postings.";

        //default behaviour: using the WholeBreakIterator, despite the multi valued paragraph separator we get back a single snippet for multiple values
        assertThat(snippets[0], equalTo(firstHlValue + (char)8233 + secondHlValue + (char)8233 + thirdHlValue));



        highlighter = new XPostingsHighlighter() {
            Iterator<String> valuesIterator = Arrays.asList(firstValue, secondValue, thirdValue).iterator();
            Iterator<Integer> offsetsIterator = Arrays.asList(0, firstValue.length() + 1, firstValue.length() + secondValue.length() + 2).iterator();

            @Override
            protected String[][] loadFieldValues(IndexSearcher searcher, String[] fields, int[] docids, int maxLength) throws IOException {
                return new String[][]{new String[]{valuesIterator.next()}};
            }

            @Override
            protected int getOffsetForCurrentValue(String field, int docId) {
                return offsetsIterator.next();
            }

            @Override
            protected BreakIterator getBreakIterator(String field) {
                return new WholeBreakIterator();
            }
        };

        //first call using the WholeBreakIterator, we get now only the first value properly highlighted as we wish
        snippets = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0], equalTo(firstHlValue));

        //second call using the WholeBreakIterator, we get now only the second value properly highlighted as we wish
        snippets = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0], equalTo(secondHlValue));

        //third call using the WholeBreakIterator, we get now only the third value properly highlighted as we wish
        snippets = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0], equalTo(thirdHlValue));

        ir.close();
        dir.close();
    }

    @Test
    public void testDiscreteHighlightingPerValue_secondValueWithoutMatches() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        final String firstValue = "This is a test. Just a test highlighting from postings highlighter.";
        Document doc = new Document();
        doc.add(body);
        body.setStringValue(firstValue);

        final String secondValue = "This is the second value without matches.";
        Field body2 = new Field("body", "", offsetsType);
        doc.add(body2);
        body2.setStringValue(secondValue);

        final String thirdValue = "This is the third value to test highlighting with postings.";
        Field body3 = new Field("body", "", offsetsType);
        doc.add(body3);
        body3.setStringValue(thirdValue);

        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);

        Query query = new TermQuery(new Term("body", "highlighting"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));

        XPostingsHighlighter highlighter = new XPostingsHighlighter() {
            @Override
            protected BreakIterator getBreakIterator(String field) {
                return new WholeBreakIterator();
            }

            @Override
            protected char getMultiValuedSeparator(String field) {
                //U+2029 PARAGRAPH SEPARATOR (PS): each value holds a discrete passage for highlighting
                return 8233;
            }

            @Override
            protected Passage[] getEmptyHighlight(String fieldName, BreakIterator bi, int maxPassages) {
                return new Passage[0];
            }
        };
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        String firstHlValue = "This is a test. Just a test <b>highlighting</b> from postings highlighter.";
        String thirdHlValue = "This is the third value to test <b>highlighting</b> with postings.";
        //default behaviour: using the WholeBreakIterator, despite the multi valued paragraph separator we get back a single snippet for multiple values
        //but only the first and the third value are returned since there are no matches in the second one.
        assertThat(snippets[0], equalTo(firstHlValue + (char)8233 + secondValue + (char)8233 + thirdHlValue));


        highlighter = new XPostingsHighlighter() {
            Iterator<String> valuesIterator = Arrays.asList(firstValue, secondValue, thirdValue).iterator();
            Iterator<Integer> offsetsIterator = Arrays.asList(0, firstValue.length() + 1, firstValue.length() + secondValue.length() + 2).iterator();

            @Override
            protected String[][] loadFieldValues(IndexSearcher searcher, String[] fields, int[] docids, int maxLength) throws IOException {
                return new String[][]{new String[]{valuesIterator.next()}};
            }

            @Override
            protected int getOffsetForCurrentValue(String field, int docId) {
                return offsetsIterator.next();
            }

            @Override
            protected BreakIterator getBreakIterator(String field) {
                return new WholeBreakIterator();
            }

            @Override
            protected Passage[] getEmptyHighlight(String fieldName, BreakIterator bi, int maxPassages) {
                return new Passage[0];
            }
        };

        //first call using the WholeBreakIterator, we get now only the first value properly highlighted as we wish
        snippets = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0], equalTo(firstHlValue));

        //second call using the WholeBreakIterator, we get now nothing back because there's nothing to highlight in the second value
        snippets = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0], nullValue());

        //third call using the WholeBreakIterator, we get now only the third value properly highlighted as we wish
        snippets = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0], equalTo(thirdHlValue));

        ir.close();
        dir.close();
    }

    @Test
    public void testDiscreteHighlightingPerValue_MultipleMatches() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        final String firstValue = "This is a highlighting test. Just a test highlighting from postings highlighter.";
        Document doc = new Document();
        doc.add(body);
        body.setStringValue(firstValue);

        final String secondValue = "This is the second highlighting value to test highlighting with postings.";
        Field body2 = new Field("body", "", offsetsType);
        doc.add(body2);
        body2.setStringValue(secondValue);

        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);

        Query query = new TermQuery(new Term("body", "highlighting"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));

        String firstHlValue = "This is a <b>highlighting</b> test. Just a test <b>highlighting</b> from postings highlighter.";
        String secondHlValue = "This is the second <b>highlighting</b> value to test <b>highlighting</b> with postings.";

        XPostingsHighlighter highlighter = new XPostingsHighlighter() {
            Iterator<String> valuesIterator = Arrays.asList(firstValue, secondValue).iterator();
            Iterator<Integer> offsetsIterator = Arrays.asList(0, firstValue.length() + 1).iterator();

            @Override
            protected String[][] loadFieldValues(IndexSearcher searcher, String[] fields, int[] docids, int maxLength) throws IOException {
                return new String[][]{new String[]{valuesIterator.next()}};
            }

            @Override
            protected int getOffsetForCurrentValue(String field, int docId) {
                return offsetsIterator.next();
            }

            @Override
            protected BreakIterator getBreakIterator(String field) {
                return new WholeBreakIterator();
            }

            @Override
            protected Passage[] getEmptyHighlight(String fieldName, BreakIterator bi, int maxPassages) {
                return new Passage[0];
            }
        };

        //first call using the WholeBreakIterator, we get now only the first value properly highlighted as we wish
        String[] snippets = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0], equalTo(firstHlValue));

        //second call using the WholeBreakIterator, we get now only the second value properly highlighted as we wish
        snippets = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0], equalTo(secondHlValue));

        ir.close();
        dir.close();
    }

    @Test
    public void testDiscreteHighlightingPerValue_MultipleQueryTerms() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        final String firstValue = "This is the first sentence. This is the second sentence.";
        Document doc = new Document();
        doc.add(body);
        body.setStringValue(firstValue);

        final String secondValue = "This is the third sentence. This is the fourth sentence.";
        Field body2 = new Field("body", "", offsetsType);
        doc.add(body2);
        body2.setStringValue(secondValue);

        final String thirdValue = "This is the fifth sentence";
        Field body3 = new Field("body", "", offsetsType);
        doc.add(body3);
        body3.setStringValue(thirdValue);

        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);

        BooleanQuery query = new BooleanQuery();
        query.add(new BooleanClause(new TermQuery(new Term("body", "third")), BooleanClause.Occur.SHOULD));
        query.add(new BooleanClause(new TermQuery(new Term("body", "seventh")), BooleanClause.Occur.SHOULD));
        query.add(new BooleanClause(new TermQuery(new Term("body", "fifth")), BooleanClause.Occur.SHOULD));
        query.setMinimumNumberShouldMatch(1);

        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));

        String secondHlValue = "This is the <b>third</b> sentence. This is the fourth sentence.";
        String thirdHlValue = "This is the <b>fifth</b> sentence";

        XPostingsHighlighter highlighter = new XPostingsHighlighter() {
            Iterator<String> valuesIterator = Arrays.asList(firstValue, secondValue, thirdValue).iterator();
            Iterator<Integer> offsetsIterator = Arrays.asList(0, firstValue.length() + 1, secondValue.length() + 1).iterator();

            @Override
            protected String[][] loadFieldValues(IndexSearcher searcher, String[] fields, int[] docids, int maxLength) throws IOException {
                return new String[][]{new String[]{valuesIterator.next()}};
            }

            @Override
            protected int getOffsetForCurrentValue(String field, int docId) {
                return offsetsIterator.next();
            }

            @Override
            protected BreakIterator getBreakIterator(String field) {
                return new WholeBreakIterator();
            }

            @Override
            protected Passage[] getEmptyHighlight(String fieldName, BreakIterator bi, int maxPassages) {
                return new Passage[0];
            }
        };

        //first call using the WholeBreakIterator, we get now null as the first value doesn't hold any match
        String[] snippets = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0], nullValue());

        //second call using the WholeBreakIterator, we get now only the second value properly highlighted as we wish
        snippets = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0], equalTo(secondHlValue));

        //second call using the WholeBreakIterator, we get now only the third value properly highlighted as we wish
        snippets = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0], equalTo(thirdHlValue));

        ir.close();
        dir.close();
    }

    /*
    The following are tests that we added to make sure that certain behaviours are possible using the postings highlighter
    They don't require our forked version, but only custom versions of methods that can be overridden and are already exposed to subclasses
     */

    /*
    Tests that it's possible to obtain different fragments per document instead of a big string of concatenated fragments.
    We use our own PassageFormatter for that and override the getFormatter method.
     */
    @Test
    public void testCustomPassageFormatterMultipleFragments() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);
        body.setStringValue("This test is another test. Not a good sentence. Test test test test.");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        IndexSearcher searcher = newSearcher(ir);
        Query query = new TermQuery(new Term("body", "test"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 5);
        assertThat(snippets.length, equalTo(1));
        //default behaviour that we want to change
        assertThat(snippets[0], equalTo("This <b>test</b> is another test. ... <b>Test</b> <b>test</b> <b>test</b> test."));


        final CustomPassageFormatter passageFormatter = new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder());
        highlighter = new XPostingsHighlighter() {
            @Override
            protected PassageFormatter getFormatter(String field) {
                return passageFormatter;
            }
        };

        final ScoreDoc scoreDocs[] = topDocs.scoreDocs;
        int docids[] = new int[scoreDocs.length];
        int maxPassages[] = new int[scoreDocs.length];
        for (int i = 0; i < docids.length; i++) {
            docids[i] = scoreDocs[i].doc;
            maxPassages[i] = 5;
        }
        Map<String, Object[]> highlights = highlighter.highlightFieldsAsObjects(new String[]{"body"}, query, searcher, docids, maxPassages);
        assertThat(highlights, notNullValue());
        assertThat(highlights.size(), equalTo(1));
        Object[] objectSnippets = highlights.get("body");
        assertThat(objectSnippets, notNullValue());
        assertThat(objectSnippets.length, equalTo(1));
        assertThat(objectSnippets[0], instanceOf(Snippet[].class));

        Snippet[] snippetsSnippet = (Snippet[]) objectSnippets[0];
        assertThat(snippetsSnippet.length, equalTo(2));
        //multiple fragments as we wish
        assertThat(snippetsSnippet[0].getText(), equalTo("This <b>test</b> is another test."));
        assertThat(snippetsSnippet[1].getText(), equalTo("<b>Test</b> <b>test</b> <b>test</b> test."));

        ir.close();
        dir.close();
    }

    /*
    Tests that it's possible to return no fragments when there's nothing to highlight
    We do that by overriding the getEmptyHighlight method
     */
    @Test
    public void testHighlightWithNoMatches() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Field none = new Field("none", "", offsetsType);
        Document doc = new Document();
        doc.add(body);
        doc.add(none);

        body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
        none.setStringValue(body.stringValue());
        iw.addDocument(doc);
        body.setStringValue("Highlighting the first term. Hope it works.");
        none.setStringValue(body.stringValue());
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        Query query = new TermQuery(new Term("none", "highlighting"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(2));
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 1);
        //Two null snippets if there are no matches (thanks to our own custom passage formatter)
        assertThat(snippets.length, equalTo(2));
        //default behaviour: returns the first sentence with num passages = 1
        assertThat(snippets[0], equalTo("This is a test. "));
        assertThat(snippets[1], equalTo("Highlighting the first term. "));

        highlighter = new XPostingsHighlighter() {
            @Override
            protected Passage[] getEmptyHighlight(String fieldName, BreakIterator bi, int maxPassages) {
                return new Passage[0];
            }
        };
        snippets = highlighter.highlight("body", query, searcher, topDocs);
        //Two null snippets if there are no matches, as we wish
        assertThat(snippets.length, equalTo(2));
        assertThat(snippets[0], nullValue());
        assertThat(snippets[1], nullValue());

        ir.close();
        dir.close();
    }

    /*
    Tests that it's possible to avoid having fragments that span across different values
    We do that by overriding the getMultiValuedSeparator and using a proper separator between values
     */
    @Test
    public void testCustomMultiValuedSeparator() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);

        body.setStringValue("This is a test. Just a test highlighting from postings");

        Field body2 = new Field("body", "", offsetsType);
        doc.add(body2);
        body2.setStringValue("highlighter.");
        iw.addDocument(doc);


        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        Query query = new TermQuery(new Term("body", "highlighting"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        //default behaviour: getting a fragment that spans across different values
        assertThat(snippets[0], equalTo("Just a test <b>highlighting</b> from postings highlighter."));


        highlighter = new XPostingsHighlighter() {
            @Override
            protected char getMultiValuedSeparator(String field) {
                //U+2029 PARAGRAPH SEPARATOR (PS): each value holds a discrete passage for highlighting
                return 8233;
            }
        };
        snippets = highlighter.highlight("body", query, searcher, topDocs);
        assertThat(snippets.length, equalTo(1));
        //getting a fragment that doesn't span across different values since we used the paragraph separator between the different values
        assertThat(snippets[0], equalTo("Just a test <b>highlighting</b> from postings" + (char)8233));

        ir.close();
        dir.close();
    }




    /*
    The following are all the existing postings highlighter tests, to make sure we don't have regression in our own fork
     */

    @Test
    public void testBasics() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);

        body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
        iw.addDocument(doc);
        body.setStringValue("Highlighting the first term. Hope it works.");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        Query query = new TermQuery(new Term("body", "highlighting"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(2, topDocs.totalHits);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
        assertEquals(2, snippets.length);
        assertEquals("Just a test <b>highlighting</b> from postings. ", snippets[0]);
        assertEquals("<b>Highlighting</b> the first term. ", snippets[1]);

        ir.close();
        dir.close();
    }

    public void testFormatWithMatchExceedingContentLength2() throws Exception {

        String bodyText = "123 TEST 01234 TEST";

        String[] snippets = formatWithMatchExceedingContentLength(bodyText);

        assertEquals(1, snippets.length);
        assertEquals("123 <b>TEST</b> 01234 TE", snippets[0]);
    }

    public void testFormatWithMatchExceedingContentLength3() throws Exception {

        String bodyText = "123 5678 01234 TEST TEST";

        String[] snippets = formatWithMatchExceedingContentLength(bodyText);

        assertEquals(1, snippets.length);
        assertEquals("123 5678 01234 TE", snippets[0]);
    }

    public void testFormatWithMatchExceedingContentLength() throws Exception {

        String bodyText = "123 5678 01234 TEST";

        String[] snippets = formatWithMatchExceedingContentLength(bodyText);

        assertEquals(1, snippets.length);
        // LUCENE-5166: no snippet
        assertEquals("123 5678 01234 TE", snippets[0]);
    }

    private String[] formatWithMatchExceedingContentLength(String bodyText) throws IOException {

        int maxLength = 17;

        final Analyzer analyzer = new MockAnalyzer(random());

        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        final FieldType fieldType = new FieldType(TextField.TYPE_STORED);
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        final Field body = new Field("body", bodyText, fieldType);

        Document doc = new Document();
        doc.add(body);

        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);

        Query query = new TermQuery(new Term("body", "test"));

        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(1, topDocs.totalHits);

        XPostingsHighlighter highlighter = new XPostingsHighlighter(maxLength);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs);


        ir.close();
        dir.close();
        return snippets;
    }

    // simple test highlighting last word.
    public void testHighlightLastWord() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);

        body.setStringValue("This is a test");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        Query query = new TermQuery(new Term("body", "test"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(1, topDocs.totalHits);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
        assertEquals(1, snippets.length);
        assertEquals("This is a <b>test</b>", snippets[0]);

        ir.close();
        dir.close();
    }

    // simple test with one sentence documents.
    @Test
    public void testOneSentence() throws Exception {
        Directory dir = newDirectory();
        // use simpleanalyzer for more natural tokenization (else "test." is a token)
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);

        body.setStringValue("This is a test.");
        iw.addDocument(doc);
        body.setStringValue("Test a one sentence document.");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        Query query = new TermQuery(new Term("body", "test"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(2, topDocs.totalHits);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
        assertEquals(2, snippets.length);
        assertEquals("This is a <b>test</b>.", snippets[0]);
        assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

        ir.close();
        dir.close();
    }

    // simple test with multiple values that make a result longer than maxLength.
    @Test
    public void testMaxLengthWithMultivalue() throws Exception {
        Directory dir = newDirectory();
        // use simpleanalyzer for more natural tokenization (else "test." is a token)
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Document doc = new Document();

        for(int i = 0; i < 3 ; i++) {
            Field body = new Field("body", "", offsetsType);
            body.setStringValue("This is a multivalued field");
            doc.add(body);
        }

        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter(40);
        Query query = new TermQuery(new Term("body", "field"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(1, topDocs.totalHits);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
        assertEquals(1, snippets.length);
        assertTrue("Snippet should have maximum 40 characters plus the pre and post tags",
                snippets[0].length() == (40 + "<b></b>".length()));

        ir.close();
        dir.close();
    }

    @Test
    public void testMultipleFields() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Field title = new Field("title", "", offsetsType);
        Document doc = new Document();
        doc.add(body);
        doc.add(title);

        body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
        title.setStringValue("I am hoping for the best.");
        iw.addDocument(doc);
        body.setStringValue("Highlighting the first term. Hope it works.");
        title.setStringValue("But best may not be good enough.");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(new Term("body", "highlighting")), BooleanClause.Occur.SHOULD);
        query.add(new TermQuery(new Term("title", "best")), BooleanClause.Occur.SHOULD);
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(2, topDocs.totalHits);
        Map<String,String[]> snippets = highlighter.highlightFields(new String [] { "body", "title" }, query, searcher, topDocs);
        assertEquals(2, snippets.size());
        assertEquals("Just a test <b>highlighting</b> from postings. ", snippets.get("body")[0]);
        assertEquals("<b>Highlighting</b> the first term. ", snippets.get("body")[1]);
        assertEquals("I am hoping for the <b>best</b>.", snippets.get("title")[0]);
        assertEquals("But <b>best</b> may not be good enough.", snippets.get("title")[1]);
        ir.close();
        dir.close();
    }

    @Test
    public void testMultipleTerms() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);

        body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
        iw.addDocument(doc);
        body.setStringValue("Highlighting the first term. Hope it works.");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(new Term("body", "highlighting")), BooleanClause.Occur.SHOULD);
        query.add(new TermQuery(new Term("body", "just")), BooleanClause.Occur.SHOULD);
        query.add(new TermQuery(new Term("body", "first")), BooleanClause.Occur.SHOULD);
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(2, topDocs.totalHits);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
        assertEquals(2, snippets.length);
        assertEquals("<b>Just</b> a test <b>highlighting</b> from postings. ", snippets[0]);
        assertEquals("<b>Highlighting</b> the <b>first</b> term. ", snippets[1]);

        ir.close();
        dir.close();
    }

    @Test
    public void testMultiplePassages() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);

        body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
        iw.addDocument(doc);
        body.setStringValue("This test is another test. Not a good sentence. Test test test test.");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        Query query = new TermQuery(new Term("body", "test"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(2, topDocs.totalHits);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
        assertEquals(2, snippets.length);
        assertEquals("This is a <b>test</b>. Just a <b>test</b> highlighting from postings. ", snippets[0]);
        assertEquals("This <b>test</b> is another <b>test</b>. ... <b>Test</b> <b>test</b> <b>test</b> <b>test</b>.", snippets[1]);

        ir.close();
        dir.close();
    }

    @Test
    public void testUserFailedToIndexOffsets() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType positionsType = new FieldType(TextField.TYPE_STORED);
        positionsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        Field body = new Field("body", "", positionsType);
        Field title = new StringField("title", "", Field.Store.YES);
        Document doc = new Document();
        doc.add(body);
        doc.add(title);

        body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
        title.setStringValue("test");
        iw.addDocument(doc);
        body.setStringValue("This test is another test. Not a good sentence. Test test test test.");
        title.setStringValue("test");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        Query query = new TermQuery(new Term("body", "test"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(2, topDocs.totalHits);
        try {
            highlighter.highlight("body", query, searcher, topDocs, 2);
            fail("did not hit expected exception");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        try {
            highlighter.highlight("title", new TermQuery(new Term("title", "test")), searcher, topDocs, 2);
            fail("did not hit expected exception");
        } catch (IllegalArgumentException iae) {
            // expected
        }
        ir.close();
        dir.close();
    }

    @Test
    public void testBuddhism() throws Exception {
        String text = "This eight-volume set brings together seminal papers in Buddhist studies from a vast " +
                "range of academic disciplines published over the last forty years. With a new introduction " +
                "by the editor, this collection is a unique and unrivalled research resource for both " +
                "student and scholar. Coverage includes: - Buddhist origins; early history of Buddhism in " +
                "South and Southeast Asia - early Buddhist Schools and Doctrinal History; Theravada Doctrine " +
                "- the Origins and nature of Mahayana Buddhism; some Mahayana religious topics - Abhidharma " +
                "and Madhyamaka - Yogacara, the Epistemological tradition, and Tathagatagarbha - Tantric " +
                "Buddhism (Including China and Japan); Buddhism in Nepal and Tibet - Buddhism in South and " +
                "Southeast Asia, and - Buddhism in China, East Asia, and Japan.";
        Directory dir = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, analyzer);

        FieldType positionsType = new FieldType(TextField.TYPE_STORED);
        positionsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", text, positionsType);
        Document document = new Document();
        document.add(body);
        iw.addDocument(document);
        IndexReader ir = iw.getReader();
        iw.close();
        IndexSearcher searcher = newSearcher(ir);
        PhraseQuery query = new PhraseQuery();
        query.add(new Term("body", "buddhist"));
        query.add(new Term("body", "origins"));
        TopDocs topDocs = searcher.search(query, 10);
        assertEquals(1, topDocs.totalHits);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
        assertEquals(1, snippets.length);
        assertTrue(snippets[0].contains("<b>Buddhist</b> <b>origins</b>"));
        ir.close();
        dir.close();
    }

    @Test
    public void testCuriousGeorge() throws Exception {
        String text = "It’s the formula for success for preschoolers—Curious George and fire trucks! " +
                "Curious George and the Firefighters is a story based on H. A. and Margret Rey’s " +
                "popular primate and painted in the original watercolor and charcoal style. " +
                "Firefighters are a famously brave lot, but can they withstand a visit from one curious monkey?";
        Directory dir = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, analyzer);
        FieldType positionsType = new FieldType(TextField.TYPE_STORED);
        positionsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", text, positionsType);
        Document document = new Document();
        document.add(body);
        iw.addDocument(document);
        IndexReader ir = iw.getReader();
        iw.close();
        IndexSearcher searcher = newSearcher(ir);
        PhraseQuery query = new PhraseQuery();
        query.add(new Term("body", "curious"));
        query.add(new Term("body", "george"));
        TopDocs topDocs = searcher.search(query, 10);
        assertEquals(1, topDocs.totalHits);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
        assertEquals(1, snippets.length);
        assertFalse(snippets[0].contains("<b>Curious</b>Curious"));
        ir.close();
        dir.close();
    }

    @Test
    public void testCambridgeMA() throws Exception {
        BufferedReader r = new BufferedReader(new InputStreamReader(
                this.getClass().getResourceAsStream("CambridgeMA.utf8"), "UTF-8"));
        String text = r.readLine();
        r.close();
        Directory dir = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, analyzer);
        FieldType positionsType = new FieldType(TextField.TYPE_STORED);
        positionsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", text, positionsType);
        Document document = new Document();
        document.add(body);
        iw.addDocument(document);
        IndexReader ir = iw.getReader();
        iw.close();
        IndexSearcher searcher = newSearcher(ir);
        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(new Term("body", "porter")), BooleanClause.Occur.SHOULD);
        query.add(new TermQuery(new Term("body", "square")), BooleanClause.Occur.SHOULD);
        query.add(new TermQuery(new Term("body", "massachusetts")), BooleanClause.Occur.SHOULD);
        TopDocs topDocs = searcher.search(query, 10);
        assertEquals(1, topDocs.totalHits);
        XPostingsHighlighter highlighter = new XPostingsHighlighter(Integer.MAX_VALUE-1);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
        assertEquals(1, snippets.length);
        assertTrue(snippets[0].contains("<b>Square</b>"));
        assertTrue(snippets[0].contains("<b>Porter</b>"));
        ir.close();
        dir.close();
    }

    @Test
    public void testPassageRanking() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);

        body.setStringValue("This is a test.  Just highlighting from postings. This is also a much sillier test.  Feel free to test test test test test test test.");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        Query query = new TermQuery(new Term("body", "test"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(1, topDocs.totalHits);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
        assertEquals(1, snippets.length);
        assertEquals("This is a <b>test</b>.  ... Feel free to <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b>.", snippets[0]);

        ir.close();
        dir.close();
    }

    @Test
    public void testBooleanMustNot() throws Exception {
        Directory dir = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, analyzer);
        FieldType positionsType = new FieldType(TextField.TYPE_STORED);
        positionsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "This sentence has both terms.  This sentence has only terms.", positionsType);
        Document document = new Document();
        document.add(body);
        iw.addDocument(document);
        IndexReader ir = iw.getReader();
        iw.close();
        IndexSearcher searcher = newSearcher(ir);
        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(new Term("body", "terms")), BooleanClause.Occur.SHOULD);
        BooleanQuery query2 = new BooleanQuery();
        query.add(query2, BooleanClause.Occur.SHOULD);
        query2.add(new TermQuery(new Term("body", "both")), BooleanClause.Occur.MUST_NOT);
        TopDocs topDocs = searcher.search(query, 10);
        assertEquals(1, topDocs.totalHits);
        XPostingsHighlighter highlighter = new XPostingsHighlighter(Integer.MAX_VALUE-1);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
        assertEquals(1, snippets.length);
        assertFalse(snippets[0].contains("<b>both</b>"));
        ir.close();
        dir.close();
    }

    @Test
    public void testHighlightAllText() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);

        body.setStringValue("This is a test.  Just highlighting from postings. This is also a much sillier test.  Feel free to test test test test test test test.");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter(10000) {
            @Override
            protected BreakIterator getBreakIterator(String field) {
                return new WholeBreakIterator();
            }
        };
        Query query = new TermQuery(new Term("body", "test"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(1, topDocs.totalHits);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
        assertEquals(1, snippets.length);
        assertEquals("This is a <b>test</b>.  Just highlighting from postings. This is also a much sillier <b>test</b>.  Feel free to <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b>.", snippets[0]);

        ir.close();
        dir.close();
    }

    @Test
    public void testSpecificDocIDs() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);

        body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
        iw.addDocument(doc);
        body.setStringValue("Highlighting the first term. Hope it works.");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        Query query = new TermQuery(new Term("body", "highlighting"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(2, topDocs.totalHits);
        ScoreDoc[] hits = topDocs.scoreDocs;
        int[] docIDs = new int[2];
        docIDs[0] = hits[0].doc;
        docIDs[1] = hits[1].doc;
        String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, searcher, docIDs, new int[] { 1 }).get("body");
        assertEquals(2, snippets.length);
        assertEquals("Just a test <b>highlighting</b> from postings. ", snippets[0]);
        assertEquals("<b>Highlighting</b> the first term. ", snippets[1]);

        ir.close();
        dir.close();
    }

    @Test
    public void testCustomFieldValueSource() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        Document doc = new Document();

        FieldType offsetsType = new FieldType(TextField.TYPE_NOT_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        final String text = "This is a test.  Just highlighting from postings. This is also a much sillier test.  Feel free to test test test test test test test.";
        Field body = new Field("body", text, offsetsType);
        doc.add(body);
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);

        XPostingsHighlighter highlighter = new XPostingsHighlighter(10000) {
            @Override
            protected String[][] loadFieldValues(IndexSearcher searcher, String[] fields, int[] docids, int maxLength) throws IOException {
                assertThat(fields.length, equalTo(1));
                assertThat(docids.length, equalTo(1));
                String[][] contents = new String[1][1];
                contents[0][0] = text;
                return contents;
            }

            @Override
            protected BreakIterator getBreakIterator(String field) {
                return new WholeBreakIterator();
            }
        };

        Query query = new TermQuery(new Term("body", "test"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(1, topDocs.totalHits);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
        assertEquals(1, snippets.length);
        assertEquals("This is a <b>test</b>.  Just highlighting from postings. This is also a much sillier <b>test</b>.  Feel free to <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b>.", snippets[0]);

        ir.close();
        dir.close();
    }

    /** Make sure highlighter returns first N sentences if
     *  there were no hits. */
    @Test
    public void testEmptyHighlights() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Document doc = new Document();

        Field body = new Field("body", "test this is.  another sentence this test has.  far away is that planet.", offsetsType);
        doc.add(body);
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        Query query = new TermQuery(new Term("body", "highlighting"));
        int[] docIDs = new int[] {0};
        String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, searcher, docIDs, new int[] { 2 }).get("body");
        assertEquals(1, snippets.length);
        assertEquals("test this is.  another sentence this test has.  ", snippets[0]);

        ir.close();
        dir.close();
    }

    /** Make sure highlighter we can customize how emtpy
     *  highlight is returned. */
    @Test
    public void testCustomEmptyHighlights() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Document doc = new Document();

        Field body = new Field("body", "test this is.  another sentence this test has.  far away is that planet.", offsetsType);
        doc.add(body);
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter() {
            @Override
            public Passage[] getEmptyHighlight(String fieldName, BreakIterator bi, int maxPassages) {
                return new Passage[0];
            }
        };
        Query query = new TermQuery(new Term("body", "highlighting"));
        int[] docIDs = new int[] {0};
        String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, searcher, docIDs, new int[] { 2 }).get("body");
        assertEquals(1, snippets.length);
        assertNull(snippets[0]);

        ir.close();
        dir.close();
    }

    /** Make sure highlighter returns whole text when there
     *  are no hits and BreakIterator is null. */
    @Test
    public void testEmptyHighlightsWhole() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Document doc = new Document();

        Field body = new Field("body", "test this is.  another sentence this test has.  far away is that planet.", offsetsType);
        doc.add(body);
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter(10000) {
            @Override
            protected BreakIterator getBreakIterator(String field) {
                return new WholeBreakIterator();
            }
        };
        Query query = new TermQuery(new Term("body", "highlighting"));
        int[] docIDs = new int[] {0};
        String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, searcher, docIDs, new int[] { 2 }).get("body");
        assertEquals(1, snippets.length);
        assertEquals("test this is.  another sentence this test has.  far away is that planet.", snippets[0]);

        ir.close();
        dir.close();
    }

    /** Make sure highlighter is OK with entirely missing
     *  field. */
    @Test
    public void testFieldIsMissing() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Document doc = new Document();

        Field body = new Field("body", "test this is.  another sentence this test has.  far away is that planet.", offsetsType);
        doc.add(body);
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        Query query = new TermQuery(new Term("bogus", "highlighting"));
        int[] docIDs = new int[] {0};
        String snippets[] = highlighter.highlightFields(new String[] {"bogus"}, query, searcher, docIDs, new int[] { 2 }).get("bogus");
        assertEquals(1, snippets.length);
        assertNull(snippets[0]);

        ir.close();
        dir.close();
    }

    @Test
    public void testFieldIsJustSpace() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

        Document doc = new Document();
        doc.add(new Field("body", "   ", offsetsType));
        doc.add(new Field("id", "id", offsetsType));
        iw.addDocument(doc);

        doc = new Document();
        doc.add(new Field("body", "something", offsetsType));
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        int docID = searcher.search(new TermQuery(new Term("id", "id")), 1).scoreDocs[0].doc;

        Query query = new TermQuery(new Term("body", "highlighting"));
        int[] docIDs = new int[1];
        docIDs[0] = docID;
        String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, searcher, docIDs, new int[] { 2 }).get("body");
        assertEquals(1, snippets.length);
        assertEquals("   ", snippets[0]);

        ir.close();
        dir.close();
    }

    @Test
    public void testFieldIsEmptyString() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

        Document doc = new Document();
        doc.add(new Field("body", "", offsetsType));
        doc.add(new Field("id", "id", offsetsType));
        iw.addDocument(doc);

        doc = new Document();
        doc.add(new Field("body", "something", offsetsType));
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        int docID = searcher.search(new TermQuery(new Term("id", "id")), 1).scoreDocs[0].doc;

        Query query = new TermQuery(new Term("body", "highlighting"));
        int[] docIDs = new int[1];
        docIDs[0] = docID;
        String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, searcher, docIDs, new int[] { 2 }).get("body");
        assertEquals(1, snippets.length);
        assertNull(snippets[0]);

        ir.close();
        dir.close();
    }

    @Test
    public void testMultipleDocs() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

        int numDocs = scaledRandomIntBetween(100, 1000);
        for(int i=0;i<numDocs;i++) {
            Document doc = new Document();
            String content = "the answer is " + i;
            if ((i & 1) == 0) {
                content += " some more terms";
            }
            doc.add(new Field("body", content, offsetsType));
            doc.add(newStringField("id", ""+i, Field.Store.YES));
            iw.addDocument(doc);

            if (random().nextInt(10) == 2) {
                iw.commit();
            }
        }

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        Query query = new TermQuery(new Term("body", "answer"));
        TopDocs hits = searcher.search(query, numDocs);
        assertEquals(numDocs, hits.totalHits);

        String snippets[] = highlighter.highlight("body", query, searcher, hits);
        assertEquals(numDocs, snippets.length);
        for(int hit=0;hit<numDocs;hit++) {
            Document doc = searcher.doc(hits.scoreDocs[hit].doc);
            int id = Integer.parseInt(doc.get("id"));
            String expected = "the <b>answer</b> is " + id;
            if ((id  & 1) == 0) {
                expected += " some more terms";
            }
            assertEquals(expected, snippets[hit]);
        }

        ir.close();
        dir.close();
    }

    @Test
    public void testMultipleSnippetSizes() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Field title = new Field("title", "", offsetsType);
        Document doc = new Document();
        doc.add(body);
        doc.add(title);

        body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
        title.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter();
        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(new Term("body", "test")), BooleanClause.Occur.SHOULD);
        query.add(new TermQuery(new Term("title", "test")), BooleanClause.Occur.SHOULD);
        Map<String,String[]> snippets = highlighter.highlightFields(new String[] { "title", "body" }, query, searcher, new int[] { 0 }, new int[] { 1, 2 });
        String titleHighlight = snippets.get("title")[0];
        String bodyHighlight = snippets.get("body")[0];
        assertEquals("This is a <b>test</b>. ", titleHighlight);
        assertEquals("This is a <b>test</b>. Just a <b>test</b> highlighting from postings. ", bodyHighlight);
        ir.close();
        dir.close();
    }

    public void testEncode() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);

        body.setStringValue("This is a test. Just a test highlighting from <i>postings</i>. Feel free to ignore.");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        PostingsHighlighter highlighter = new PostingsHighlighter() {
            @Override
            protected PassageFormatter getFormatter(String field) {
                return new DefaultPassageFormatter("<b>", "</b>", "... ", true);
            }
        };
        Query query = new TermQuery(new Term("body", "highlighting"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(1, topDocs.totalHits);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
        assertEquals(1, snippets.length);
        assertEquals("Just&#32;a&#32;test&#32;<b>highlighting</b>&#32;from&#32;&lt;i&gt;postings&lt;&#x2F;i&gt;&#46;&#32;", snippets[0]);

        ir.close();
        dir.close();
    }

    /** customizing the gap separator to force a sentence break */
    public void testGapSeparator() throws Exception {
        Directory dir = newDirectory();
        // use simpleanalyzer for more natural tokenization (else "test." is a token)
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Document doc = new Document();

        Field body1 = new Field("body", "", offsetsType);
        body1.setStringValue("This is a multivalued field");
        doc.add(body1);

        Field body2 = new Field("body", "", offsetsType);
        body2.setStringValue("This is something different");
        doc.add(body2);

        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        PostingsHighlighter highlighter = new PostingsHighlighter() {
            @Override
            protected char getMultiValuedSeparator(String field) {
                assert field.equals("body");
                return '\u2029';
            }
        };
        Query query = new TermQuery(new Term("body", "field"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(1, topDocs.totalHits);
        String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
        assertEquals(1, snippets.length);
        assertEquals("This is a multivalued <b>field</b>\u2029", snippets[0]);

        ir.close();
        dir.close();
    }

    // LUCENE-4906
    public void testObjectFormatter() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);

        body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        IndexSearcher searcher = newSearcher(ir);
        XPostingsHighlighter highlighter = new XPostingsHighlighter() {
            @Override
            protected PassageFormatter getFormatter(String field) {
                return new PassageFormatter() {
                    PassageFormatter defaultFormatter = new DefaultPassageFormatter();

                    @Override
                    public String[] format(Passage passages[], String content) {
                        // Just turns the String snippet into a length 2
                        // array of String
                        return new String[] {"blah blah", defaultFormatter.format(passages, content).toString()};
                    }
                };
            }
        };

        Query query = new TermQuery(new Term("body", "highlighting"));
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertEquals(1, topDocs.totalHits);
        int[] docIDs = new int[1];
        docIDs[0] = topDocs.scoreDocs[0].doc;
        Map<String,Object[]> snippets = highlighter.highlightFieldsAsObjects(new String[]{"body"}, query, searcher, docIDs, new int[] {1});
        Object[] bodySnippets = snippets.get("body");
        assertEquals(1, bodySnippets.length);
        assertTrue(Arrays.equals(new String[] {"blah blah", "Just a test <b>highlighting</b> from postings. "}, (String[]) bodySnippets[0]));

        ir.close();
        dir.close();
    }
}
