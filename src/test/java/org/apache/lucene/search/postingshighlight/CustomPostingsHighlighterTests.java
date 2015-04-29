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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.search.highlight.HighlightUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

public class CustomPostingsHighlighterTests extends ElasticsearchTestCase {

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

        List<Object> fieldValues = new ArrayList<>();
        fieldValues.add(firstValue);
        fieldValues.add(secondValue);
        fieldValues.add(thirdValue);


        IndexSearcher searcher = newSearcher(ir);

        Query query = new TermQuery(new Term("body", "highlighting"));
        BytesRef[] queryTerms = filterTerms(extractTerms(searcher, query), "body", true);

        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));
        int docId = topDocs.scoreDocs[0].doc;

        //highlighting per value, considering whole values (simulating number_of_fragments=0)
        CustomPostingsHighlighter highlighter = new CustomPostingsHighlighter(new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder()), fieldValues, false, Integer.MAX_VALUE - 1, 0);
        highlighter.setBreakIterator(new WholeBreakIterator());

        Snippet[] snippets = highlighter.highlightDoc("body", queryTerms, searcher, docId, 5);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0].getText(), equalTo("This is a test. Just a test <b>highlighting</b> from postings highlighter."));

        snippets = highlighter.highlightDoc("body", queryTerms, searcher, docId, 5);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0].getText(), equalTo("This is the second value to perform <b>highlighting</b> on."));

        snippets = highlighter.highlightDoc("body", queryTerms, searcher, docId, 5);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0].getText(), equalTo("This is the third value to test <b>highlighting</b> with postings."));


        //let's try without whole break iterator as well, to prove that highlighting works the same when working per value (not optimized though)
        highlighter = new CustomPostingsHighlighter(new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder()), fieldValues, false, Integer.MAX_VALUE - 1, 0);

        snippets = highlighter.highlightDoc("body", queryTerms, searcher, docId, 5);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0].getText(), equalTo("Just a test <b>highlighting</b> from postings highlighter."));

        snippets = highlighter.highlightDoc("body", queryTerms, searcher, docId, 5);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0].getText(), equalTo("This is the second value to perform <b>highlighting</b> on."));

        snippets = highlighter.highlightDoc("body", queryTerms, searcher, docId, 5);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0].getText(), equalTo("This is the third value to test <b>highlighting</b> with postings."));

        ir.close();
        dir.close();
    }

    /*
    Tests that scoring works properly even when using discrete per value highlighting
     */
    @Test
    public void testDiscreteHighlightingScoring() throws Exception {

        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

        //good position but only one match
        final String firstValue = "This is a test. Just a test1 highlighting from postings highlighter.";
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);
        body.setStringValue(firstValue);

        //two matches, not the best snippet due to its length though
        final String secondValue = "This is the second highlighting value to perform highlighting on a longer text that gets scored lower.";
        Field body2 = new Field("body", "", offsetsType);
        doc.add(body2);
        body2.setStringValue(secondValue);

        //two matches and short, will be scored highest
        final String thirdValue = "This is highlighting the third short highlighting value.";
        Field body3 = new Field("body", "", offsetsType);
        doc.add(body3);
        body3.setStringValue(thirdValue);

        //one match, same as first but at the end, will be scored lower due to its position
        final String fourthValue = "Just a test4 highlighting from postings highlighter.";
        Field body4 = new Field("body", "", offsetsType);
        doc.add(body4);
        body4.setStringValue(fourthValue);

        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();


        String firstHlValue = "Just a test1 <b>highlighting</b> from postings highlighter.";
        String secondHlValue = "This is the second <b>highlighting</b> value to perform <b>highlighting</b> on a longer text that gets scored lower.";
        String thirdHlValue = "This is <b>highlighting</b> the third short <b>highlighting</b> value.";
        String fourthHlValue = "Just a test4 <b>highlighting</b> from postings highlighter.";


        IndexSearcher searcher = newSearcher(ir);
        Query query = new TermQuery(new Term("body", "highlighting"));
        BytesRef[] queryTerms = filterTerms(extractTerms(searcher, query), "body", true);

        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));

        int docId = topDocs.scoreDocs[0].doc;

        List<Object> fieldValues = new ArrayList<>();
        fieldValues.add(firstValue);
        fieldValues.add(secondValue);
        fieldValues.add(thirdValue);
        fieldValues.add(fourthValue);

        boolean mergeValues = true;
        CustomPostingsHighlighter highlighter = new CustomPostingsHighlighter(new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder()), fieldValues, mergeValues, Integer.MAX_VALUE-1, 0);
        Snippet[] snippets = highlighter.highlightDoc("body", queryTerms, searcher, docId, 5);

        assertThat(snippets.length, equalTo(4));

        assertThat(snippets[0].getText(), equalTo(firstHlValue));
        assertThat(snippets[1].getText(), equalTo(secondHlValue));
        assertThat(snippets[2].getText(), equalTo(thirdHlValue));
        assertThat(snippets[3].getText(), equalTo(fourthHlValue));


        //Let's highlight each separate value and check how the snippets are scored
        mergeValues = false;
        highlighter = new CustomPostingsHighlighter(new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder()), fieldValues, mergeValues, Integer.MAX_VALUE-1, 0);
        List<Snippet> snippets2 = new ArrayList<>();
        for (int i = 0; i < fieldValues.size(); i++) {
            snippets2.addAll(Arrays.asList(highlighter.highlightDoc("body", queryTerms, searcher, docId, 5)));
        }

        assertThat(snippets2.size(), equalTo(4));
        assertThat(snippets2.get(0).getText(), equalTo(firstHlValue));
        assertThat(snippets2.get(1).getText(), equalTo(secondHlValue));
        assertThat(snippets2.get(2).getText(), equalTo(thirdHlValue));
        assertThat(snippets2.get(3).getText(), equalTo(fourthHlValue));

        Comparator <Snippet> comparator = new Comparator<Snippet>() {
            @Override
            public int compare(Snippet o1, Snippet o2) {
                return (int)Math.signum(o1.getScore() - o2.getScore());
            }
        };

        //sorting both groups of snippets
        Arrays.sort(snippets, comparator);
        Collections.sort(snippets2, comparator);

        //checking that the snippets are in the same order, regardless of whether we used per value discrete highlighting or not
        //we can't compare the scores directly since they are slightly different due to the multiValued separator added when merging values together
        //That causes slightly different lengths and start offsets, thus a slightly different score.
        //Anyways, that's not an issue. What's important is that the score is computed the same way, so that the produced order is always the same.
        for (int i = 0; i < snippets.length; i++) {
            assertThat(snippets[i].getText(), equalTo(snippets2.get(i).getText()));
        }

        ir.close();
        dir.close();
    }

    /*
    Tests that we produce the same snippets and scores when manually merging values in our own custom highlighter rather than using the built-in code
     */
    @Test
    public void testMergeValuesScoring() throws Exception {

        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

        //good position but only one match
        final String firstValue = "This is a test. Just a test1 highlighting from postings highlighter.";
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);
        body.setStringValue(firstValue);

        //two matches, not the best snippet due to its length though
        final String secondValue = "This is the second highlighting value to perform highlighting on a longer text that gets scored lower.";
        Field body2 = new Field("body", "", offsetsType);
        doc.add(body2);
        body2.setStringValue(secondValue);

        //two matches and short, will be scored highest
        final String thirdValue = "This is highlighting the third short highlighting value.";
        Field body3 = new Field("body", "", offsetsType);
        doc.add(body3);
        body3.setStringValue(thirdValue);

        //one match, same as first but at the end, will be scored lower due to its position
        final String fourthValue = "Just a test4 highlighting from postings highlighter.";
        Field body4 = new Field("body", "", offsetsType);
        doc.add(body4);
        body4.setStringValue(fourthValue);

        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();


        String firstHlValue = "Just a test1 <b>highlighting</b> from postings highlighter.";
        String secondHlValue = "This is the second <b>highlighting</b> value to perform <b>highlighting</b> on a longer text that gets scored lower.";
        String thirdHlValue = "This is <b>highlighting</b> the third short <b>highlighting</b> value.";
        String fourthHlValue = "Just a test4 <b>highlighting</b> from postings highlighter.";


        IndexSearcher searcher = newSearcher(ir);
        Query query = new TermQuery(new Term("body", "highlighting"));
        BytesRef[] queryTerms = filterTerms(extractTerms(searcher, query), "body", true);

        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));

        int docId = topDocs.scoreDocs[0].doc;

        List<Object> fieldValues = new ArrayList<>();
        fieldValues.add(firstValue);
        fieldValues.add(secondValue);
        fieldValues.add(thirdValue);
        fieldValues.add(fourthValue);

        boolean mergeValues = true;
        CustomPostingsHighlighter highlighter = new CustomPostingsHighlighter(new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder()), fieldValues, mergeValues, Integer.MAX_VALUE-1, 0);
        Snippet[] snippets = highlighter.highlightDoc("body", queryTerms, searcher, docId, 5);

        assertThat(snippets.length, equalTo(4));

        assertThat(snippets[0].getText(), equalTo(firstHlValue));
        assertThat(snippets[1].getText(), equalTo(secondHlValue));
        assertThat(snippets[2].getText(), equalTo(thirdHlValue));
        assertThat(snippets[3].getText(), equalTo(fourthHlValue));


        //testing now our fork / normal postings highlighter, which merges multiple values together using the paragraph separator
        XPostingsHighlighter highlighter2 = new XPostingsHighlighter(Integer.MAX_VALUE - 1) {
            @Override
            protected char getMultiValuedSeparator(String field) {
                return HighlightUtils.PARAGRAPH_SEPARATOR;
            }

            @Override
            protected PassageFormatter getFormatter(String field) {
                return new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder());
            }
        };

        Map<String, Object[]> highlightMap = highlighter2.highlightFieldsAsObjects(new String[]{"body"}, query, searcher, new int[]{docId}, new int[]{5});
        Object[] objects = highlightMap.get("body");
        assertThat(objects, notNullValue());
        assertThat(objects.length, equalTo(1));
        Snippet[] normalSnippets = (Snippet[])objects[0];

        assertThat(normalSnippets.length, equalTo(4));

        assertThat(normalSnippets[0].getText(), equalTo(firstHlValue));
        assertThat(normalSnippets[1].getText(), equalTo(secondHlValue));
        assertThat(normalSnippets[2].getText(), equalTo(thirdHlValue));
        assertThat(normalSnippets[3].getText(), equalTo(fourthHlValue));


        for (int i = 0; i < normalSnippets.length; i++) {
            Snippet normalSnippet = snippets[0];
            Snippet customSnippet = normalSnippets[0];
            assertThat(customSnippet.getText(), equalTo(normalSnippet.getText()));
            assertThat(customSnippet.getScore(), equalTo(normalSnippet.getScore()));
        }

        ir.close();
        dir.close();
    }

    @Test
    public void testRequireFieldMatch() throws Exception {
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

        String firstValue = "This is a test. Just a test highlighting from postings. Feel free to ignore.";
        body.setStringValue(firstValue);
        none.setStringValue(firstValue);
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        Query query = new TermQuery(new Term("none", "highlighting"));
        IndexSearcher searcher = newSearcher(ir);
        SortedSet<Term> queryTerms = extractTerms(searcher, query);
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));
        int docId = topDocs.scoreDocs[0].doc;

        List<Object> values = new ArrayList<>();
        values.add(firstValue);

        CustomPassageFormatter passageFormatter = new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder());
        CustomPostingsHighlighter highlighter = new CustomPostingsHighlighter(passageFormatter, values, true, Integer.MAX_VALUE - 1, 0);

        //no snippets with simulated require field match (we filter the terms ourselves)
        boolean requireFieldMatch = true;
        BytesRef[] filteredQueryTerms = filterTerms(queryTerms, "body", requireFieldMatch);
        Snippet[] snippets = highlighter.highlightDoc("body", filteredQueryTerms, searcher, docId, 5);
        assertThat(snippets.length, equalTo(0));


        highlighter = new CustomPostingsHighlighter(passageFormatter, values, true, Integer.MAX_VALUE - 1, 0);
        //one snippet without require field match, just passing in the query terms with no filtering on our side
        requireFieldMatch = false;
        filteredQueryTerms = filterTerms(queryTerms, "body", requireFieldMatch);
        snippets = highlighter.highlightDoc("body", filteredQueryTerms, searcher, docId, 5);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0].getText(), equalTo("Just a test <b>highlighting</b> from postings."));

        ir.close();
        dir.close();
    }

    @Test
    public void testNoMatchSize() throws Exception {
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

        String firstValue = "This is a test. Just a test highlighting from postings. Feel free to ignore.";
        body.setStringValue(firstValue);
        none.setStringValue(firstValue);
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        Query query = new TermQuery(new Term("none", "highlighting"));

        IndexSearcher searcher = newSearcher(ir);
        SortedSet<Term> queryTerms = extractTerms(searcher, query);
        TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));
        int docId = topDocs.scoreDocs[0].doc;

        List<Object> values = new ArrayList<>();
        values.add(firstValue);

        BytesRef[] filteredQueryTerms = filterTerms(queryTerms, "body", true);
        CustomPassageFormatter passageFormatter = new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder());

        CustomPostingsHighlighter highlighter = new CustomPostingsHighlighter(passageFormatter, values, true, Integer.MAX_VALUE - 1, 0);
        Snippet[] snippets = highlighter.highlightDoc("body", filteredQueryTerms, searcher, docId, 5);
        assertThat(snippets.length, equalTo(0));

        highlighter = new CustomPostingsHighlighter(passageFormatter, values, true, Integer.MAX_VALUE - 1, scaledRandomIntBetween(1, 10));
        snippets = highlighter.highlightDoc("body", filteredQueryTerms, searcher, docId, 5);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0].getText(), equalTo("This is a test."));

        ir.close();
        dir.close();
    }

    private static SortedSet<Term> extractTerms(IndexSearcher searcher, Query query) throws IOException {
        return extractTerms(searcher.createNormalizedWeight(query, false));
    }

    private static SortedSet<Term> extractTerms(Weight weight) {
        SortedSet<Term> queryTerms = new TreeSet<>();
        weight.extractTerms(queryTerms);
        return queryTerms;
    }

    private static BytesRef[] filterTerms(SortedSet<Term> queryTerms, String field, boolean requireFieldMatch) {
        SortedSet<Term> fieldTerms;
        if (requireFieldMatch) {
            Term floor = new Term(field, "");
            Term ceiling = new Term(field, UnicodeUtil.BIG_TERM);
            fieldTerms = queryTerms.subSet(floor, ceiling);
        } else {
            fieldTerms = queryTerms;
        }

        BytesRef terms[] = new BytesRef[fieldTerms.size()];
        int termUpto = 0;
        for(Term term : fieldTerms) {
            terms[termUpto++] = term.bytes();
        }

        return terms;
    }
}
