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

package org.apache.lucene.search.uhighlight;

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
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.all.AllTermQuery;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.test.ESTestCase;

import java.text.BreakIterator;
import java.util.Locale;

import static org.apache.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;
import static org.hamcrest.CoreMatchers.equalTo;

public class CustomUnifiedHighlighterTests extends ESTestCase {
    private void assertHighlightOneDoc(String fieldName, String[] inputs, Analyzer analyzer, Query query,
                                       Locale locale, BreakIterator breakIterator,
                                       int noMatchSize, String[] expectedPassages) throws Exception {
        Directory dir = newDirectory();
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
        DirectoryReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 1, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1L));
        String rawValue = Strings.arrayToDelimitedString(inputs, String.valueOf(MULTIVAL_SEP_CHAR));
        CustomUnifiedHighlighter highlighter = new CustomUnifiedHighlighter(searcher, analyzer, null,
                new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder()), locale,
                breakIterator, rawValue, noMatchSize);
        highlighter.setFieldMatcher((name) -> "text".equals(name));
        final Snippet[] snippets =
            highlighter.highlightField("text", query, topDocs.scoreDocs[0].doc, expectedPassages.length);
        assertEquals(snippets.length, expectedPassages.length);
        for (int i = 0; i < snippets.length; i++) {
            assertEquals(snippets[i].getText(), expectedPassages[i]);
        }
        reader.close();
        dir.close();
    }

    public void testSimple() throws Exception {
        final String[] inputs = {
            "This is a test. Just a test1 highlighting from unified highlighter.",
            "This is the second highlighting value to perform highlighting on a longer text that gets scored lower.",
            "This is highlighting the third short highlighting value.",
            "Just a test4 highlighting from unified highlighter."
        };

        String[] expectedPassages = {
            "Just a test1 <b>highlighting</b> from unified highlighter.",
            "This is the second <b>highlighting</b> value to perform <b>highlighting</b> on a" +
                " longer text that gets scored lower.",
            "This is <b>highlighting</b> the third short <b>highlighting</b> value.",
            "Just a test4 <b>highlighting</b> from unified highlighter."
        };
        Query query = new TermQuery(new Term("text", "highlighting"));
        assertHighlightOneDoc("text", inputs, new StandardAnalyzer(), query, Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT), 0, expectedPassages);
    }

    public void testNoMatchSize() throws Exception {
        final String[] inputs = {
            "This is a test. Just a test highlighting from unified. Feel free to ignore."
        };
        Query query = new TermQuery(new Term("body", "highlighting"));
        assertHighlightOneDoc("text", inputs, new StandardAnalyzer(), query, Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT), 100, inputs);
    }

    public void testMultiPhrasePrefixQuerySingleTerm() throws Exception {
        final String[] inputs = {
            "The quick brown fox."
        };
        final String[] outputs = {
            "The quick <b>brown</b> fox."
        };
        MultiPhrasePrefixQuery query = new MultiPhrasePrefixQuery();
        query.add(new Term("text", "bro"));
        assertHighlightOneDoc("text", inputs, new StandardAnalyzer(), query, Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT), 0, outputs);
    }

    public void testMultiPhrasePrefixQuery() throws Exception {
        final String[] inputs = {
            "The quick brown fox."
        };
        final String[] outputs = {
            "The <b>quick</b> <b>brown</b> <b>fox</b>."
        };
        MultiPhrasePrefixQuery query = new MultiPhrasePrefixQuery();
        query.add(new Term("text", "quick"));
        query.add(new Term("text", "brown"));
        query.add(new Term("text", "fo"));
        assertHighlightOneDoc("text", inputs, new StandardAnalyzer(), query, Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT), 0, outputs);
    }

    public void testAllTermQuery() throws Exception {
        final String[] inputs = {
            "The quick brown fox."
        };
        final String[] outputs = {
            "The quick brown <b>fox</b>."
        };
        AllTermQuery query = new AllTermQuery(new Term("text", "fox"));
        assertHighlightOneDoc("text", inputs, new StandardAnalyzer(), query, Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT), 0, outputs);
    }

    public void testCommonTermsQuery() throws Exception {
        final String[] inputs = {
            "The quick brown fox."
        };
        final String[] outputs = {
            "The <b>quick</b> <b>brown</b> <b>fox</b>."
        };
        CommonTermsQuery query = new CommonTermsQuery(BooleanClause.Occur.SHOULD, BooleanClause.Occur.SHOULD, 128);
        query.add(new Term("text", "quick"));
        query.add(new Term("text", "brown"));
        query.add(new Term("text", "fox"));
        assertHighlightOneDoc("text", inputs, new StandardAnalyzer(), query, Locale.ROOT,
            BreakIterator.getSentenceInstance(Locale.ROOT), 0, outputs);
    }

    public void testSentenceBoundedBreakIterator() throws Exception {
        final String[] inputs = {
            "The quick brown fox in a long sentence with another quick brown fox. " +
                "Another sentence with brown fox."
        };
        final String[] outputs = {
            "The <b>quick</b> <b>brown</b>",
            "<b>fox</b> in a long",
            "with another <b>quick</b>",
            "<b>brown</b> <b>fox</b>.",
            "sentence with <b>brown</b>",
            "<b>fox</b>.",
        };
        BooleanQuery query = new BooleanQuery.Builder()
            .add(new TermQuery(new Term("text", "quick")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "brown")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("text", "fox")), BooleanClause.Occur.SHOULD)
            .build();
        assertHighlightOneDoc("text", inputs, new StandardAnalyzer(), query, Locale.ROOT,
            BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 10), 0, outputs);
    }

    public void testRepeat() throws Exception {
        final String[] inputs = {
            "Fun  fun fun  fun  fun  fun  fun  fun  fun  fun"
        };
        final String[] outputs = {
            "<b>Fun</b>  <b>fun</b> <b>fun</b>",
            "<b>fun</b>  <b>fun</b>",
            "<b>fun</b>  <b>fun</b>  <b>fun</b>",
            "<b>fun</b>  <b>fun</b>"
        };
        Query query = new TermQuery(new Term("text", "fun"));
        assertHighlightOneDoc("text", inputs, new StandardAnalyzer(), query, Locale.ROOT,
            BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 10), 0, outputs);

        query = new PhraseQuery.Builder()
            .add(new Term("text", "fun"))
            .add(new Term("text", "fun"))
            .build();
        assertHighlightOneDoc("text", inputs, new StandardAnalyzer(), query, Locale.ROOT,
            BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 10), 0, outputs);
    }
}
